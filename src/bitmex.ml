(* DTC to BitMEX simple bridge *)

open Core
open Async
open Cohttp_async

open Bs_devkit
open Bmex

module DTC = Dtc_pb.Dtcprotocol_piqi
module WS = Bmex_ws
module REST = Bmex_rest

let rec loop_log_errors ?log f =
  let rec inner () =
    Monitor.try_with_or_error ~name:"loop_log_errors" f >>= function
    | Ok _ -> assert false
    | Error err ->
      Option.iter log ~f:(fun log -> Log.error log "run: %s" @@ Error.to_string_hum err);
      inner ()
  in inner ()

let conduit_server ~tls ~crt_path ~key_path =
  if tls then
    Sys.file_exists crt_path >>= fun crt_exists ->
    Sys.file_exists key_path >>| fun key_exists ->
    match crt_exists, key_exists with
    | `Yes, `Yes -> `OpenSSL (`Crt_file_path crt_path, `Key_file_path key_path)
    | _ -> failwith "TLS crt/key file not found"
  else
  return `TCP

let price_display_format_of_ticksize tickSize =
  if tickSize >=. 1. then `price_display_format_decimal_0
  else if tickSize =. 1e-1 then `price_display_format_decimal_1
  else if tickSize =. 1e-2 then `price_display_format_decimal_2
  else if tickSize =. 1e-3 then `price_display_format_decimal_3
  else if tickSize =. 1e-4 then `price_display_format_decimal_4
  else if tickSize =. 1e-5 then `price_display_format_decimal_5
  else if tickSize =. 1e-6 then `price_display_format_decimal_6
  else if tickSize =. 1e-7 then `price_display_format_decimal_7
  else if tickSize =. 1e-8 then `price_display_format_decimal_8
  else if tickSize =. 1e-9 then `price_display_format_decimal_9
  else invalid_argf "price_display_format_of_ticksize: %f" tickSize ()

let write_message w (typ : DTC.dtcmessage_type) gen msg =
  let typ =
    Piqirun.(DTC.gen_dtcmessage_type typ |> to_string |> init_from_string |> int_of_varint) in
  let msg = (gen msg |> Piqirun.to_string) in
  let header = Bytes.create 4 in
  Binary_packing.pack_unsigned_16_little_endian ~buf:header ~pos:0 (4 + String.length msg) ;
  Binary_packing.pack_unsigned_16_little_endian ~buf:header ~pos:2 typ ;
  Writer.write w header ;
  Writer.write w msg

module IS = struct
  module T = struct
    type t = Int.t * String.t [@@deriving sexp]
    let compare = compare
    let hash = Hashtbl.hash
  end
  include T
  include Hashable.Make (T)
end

module Instrument = struct
  open RespObj
  let is_index symbol = symbol.[0] = '.'
  let to_secdef ~testnet t =
    let symbol = string_exn t "symbol" in
    let index = is_index symbol in
    let exchange =
      string_exn t "reference"
      ^ (if testnet && not index then "T" else "")
    in
    let tickSize = float_exn t "tickSize" in
    let expiration_date = Option.map (string t "expiry") ~f:(fun time ->
        Time_ns.(of_string time |>
                 to_int_ns_since_epoch |>
                 (fun t -> t / 1_000_000_000) |>
                 Int32.of_int_exn)) in
    let secdef = DTC.default_security_definition_response () in
    secdef.symbol <- Some symbol ;
    secdef.exchange <- Some exchange ;
    secdef.security_type <-
      Some (if index then `security_type_index else `security_type_future) ;
    secdef.min_price_increment <- Some tickSize ;
    secdef.currency_value_per_increment <- Some tickSize ;
    secdef.price_display_format <- Some (price_display_format_of_ticksize tickSize) ;
    secdef.has_market_depth_data <- Some (not index) ;
    secdef.underlying_symbol <- Some (string_exn t "underlyingSymbol") ;
    secdef.updates_bid_ask_only <- Some false ;
    secdef.security_expiration_date <- expiration_date ;
    secdef
end

let bitmex_historical = ref ""

let use_testnet = ref false
let base_uri = ref @@ Uri.of_string "https://www.bitmex.com"
let my_exchange = ref "BMEX"
let my_topic = "bitsouk"

let log_bitmex = Log.create ~level:`Error ~on_error:`Raise ~output:Log.Output.[stderr ()]
let log_dtc = Log.create ~level:`Error ~on_error:`Raise ~output:Log.Output.[stderr ()]
let log_ws = Log.create ~level:`Error ~on_error:`Raise ~output:Log.Output.[stderr ()]

type instr = {
  mutable instrObj: RespObj.t;
  secdef: DTC.Security_definition_response.t ;
  mutable last_trade_price: float;
  mutable last_trade_size: int;
  mutable last_trade_ts: Time_ns.t;
  mutable last_quote_ts: Time_ns.t;
}

let create_instr
    ?(last_trade_price = 0.)
    ?(last_trade_size = 0)
    ?(last_trade_ts = Time_ns.epoch)
    ?(last_quote_ts = Time_ns.epoch)
    ~instrObj ~secdef () = {
  instrObj ; secdef ;
  last_trade_price ; last_trade_size ;
  last_trade_ts ; last_quote_ts
}

let instruments : instr String.Table.t = String.Table.create ()

type client = {
  addr: Socket.Address.Inet.t;
  addr_str: string;
  w: Writer.t;
  ws_r: WS.Response.Update.t Pipe.Reader.t;
  ws_w: WS.Response.Update.t Pipe.Writer.t;
  mutable ws_uuid: Uuid.t;
  key: string;
  secret: Cstruct.t;
  position: RespObj.t IS.Table.t; (* indexed by account, symbol *)
  margin: RespObj.t IS.Table.t; (* indexed by account, currency *)
  order: RespObj.t Uuid.Table.t; (* indexed by orderID *)
  mutable dropped: int;
  subs: int String.Table.t;
  subs_depth: int String.Table.t;
  mutable current_parent: DTC.Submit_new_single_order.t option;
  parents: string String.Table.t;
  stop_exec_inst: [`MarkPrice | `LastPrice];
}

let clients : client InetAddr.Table.t = InetAddr.Table.create ()

let set_parent_order client order =
  client.current_parent <- Some order;
  Option.iter order.client_order_id
    ~f:(fun id -> String.Table.set client.parents id id)

type book_entry = {
  price: float;
  size: int;
}

type books = {
  bids: book_entry Int.Table.t;
  asks: book_entry Int.Table.t;
}

let mapify_ob table =
  let fold_f ~key:_ ~data:{ price; size } map =
    Float.Map.update map price ~f:(function
        | Some size' -> size + size'
        | None -> size
      )
  in
  Int.Table.fold table ~init:Float.Map.empty ~f:fold_f

let orderbooks : books String.Table.t = String.Table.create ()
let quotes : Quote.t String.Table.t = String.Table.create ()

let heartbeat { addr_str; w; dropped } ival =
  let msg = DTC.default_heartbeat () in
  let rec loop () =
    Clock_ns.after @@ Time_ns.Span.of_int_sec ival >>= fun () ->
    Log.debug log_dtc "-> [%s] HB" addr_str;
    msg.num_dropped_messages <- Some (Int32.of_int_exn dropped) ;
    write_message w `heartbeat DTC.gen_heartbeat msg ;
    loop ()
  in
  Monitor.try_with_or_error ~name:"heatbeat" loop >>| function
  | Error _ -> Log.error log_dtc "-/-> %s HB" addr_str
  | Ok _ -> ()


let write_order_update ~nb_msgs ~msg_number w e =
  let seconds_of_ts_string ts =
    Time_ns.(to_int_ns_since_epoch (of_string ts) / 1_000_000_000) |>
    Int64.of_int
  in
  let invalid_arg' execType ordStatus =
    invalid_arg Printf.(sprintf "write_order_update: execType=%s, ordStatus=%s" execType ordStatus)
  in
  let status_reason_of_execType_ordStatus e =
    let execType = RespObj.(string_exn e "execType") in
    let ordStatus = RespObj.(string_exn e "ordStatus") in
    if execType = ordStatus then
      match execType with
      | "New" -> `order_status_open, `new_order_accepted
      | "PartiallyFilled" -> `order_status_open, `order_filled_partially
      | "Filled" -> `order_status_filled, `order_filled
      | "DoneForDay" -> `order_status_open, `general_order_update
      | "Canceled" -> `order_status_canceled, `order_canceled
      | "PendingCancel" -> `order_status_pending_cancel, `general_order_update
      | "Stopped" -> `order_status_open, `general_order_update
      | "Rejected" -> `order_status_rejected, `new_order_rejected
      | "PendingNew" -> `order_status_pending_open, `general_order_update
      | "Expired" -> `order_status_rejected, `new_order_rejected
      | _ -> invalid_arg' execType ordStatus
    else
      match execType, ordStatus with
      | "Restated", _ ->
        (match ordStatus with
         | "New" -> `order_status_open, `general_order_update
         | "PartiallyFilled" -> `order_status_open, `general_order_update
         | "Filled" -> `order_status_filled, `general_order_update
         | "DoneForDay" -> `order_status_open, `general_order_update
         | "Canceled" -> `order_status_canceled, `general_order_update
         | "PendingCancel" -> `order_status_pending_cancel, `general_order_update
         | "Stopped" -> `order_status_open, `general_order_update
         | "Rejected" -> `order_status_rejected, `general_order_update
         | "PendingNew" -> `order_status_pending_open, `general_order_update
         | "Expired" -> `order_status_rejected, `general_order_update
         | _ -> invalid_arg' execType ordStatus
        )
      | "Trade", "Filled" -> `order_status_filled, `order_filled
      | "Trade", "PartiallyFilled" -> `order_status_filled, `order_filled_partially
      | "Replaced", "New" -> `order_status_open, `order_cancel_replace_complete
      | "TriggeredOrActivatedBySystem", "New" -> `order_status_open, `new_order_accepted
      | "Funding", _ -> raise Exit
      | "Settlement", _ -> raise Exit
      | _ -> invalid_arg' execType ordStatus
  in
  match status_reason_of_execType_ordStatus e with
  | exception Exit -> false
  | exception Invalid_argument msg ->
    Log.error log_bitmex "Not sending order update for %s" msg ;
    false
  | status, reason ->
    let u = DTC.default_order_update () in
    let price = RespObj.(float_or_null_exn ~default:Float.max_finite_value e "price") in
    let stopPx = RespObj.(float_or_null_exn ~default:Float.max_finite_value e "stopPx") in
    let side = match (RespObj.string_exn e "side" |> Side.of_string) with
      | None -> `buy_sell_unset
      | Some `Buy -> `buy
      | Some `Sell -> `sell in
    let ord_type =
      (OrderType.of_string RespObj.(string_exn e "ordType")) in
    let tif =
      (TimeInForce.of_string RespObj.(string_exn e "timeInForce")) in
    let ts =
      RespObj.(string e "transactTime" |> Option.map ~f:seconds_of_ts_string) in
    let p1, p2 = p1_p2_of_bitmex ~ord_type ~stopPx ~price in
    u.total_num_messages <- Some nb_msgs ;
    u.message_number <- Some msg_number ;
    u.symbol <- Some RespObj.(string_exn e "symbol") ;
    u.exchange <- Some !my_exchange ;
    u.client_order_id <- Some RespObj.(string_exn e "clOrdID") ;
    u.server_order_id <- Some RespObj.(string_exn e "orderID") ;
    u.exchange_order_id <- Some RespObj.(string_exn e "orderID") ;
    u.order_type <- Some (ord_type :> DTC.order_type_enum) ;
    u.order_status <- Some status ;
    u.order_update_reason <- Some reason ;
    u.buy_sell <- Some side ;
    u.price1 <- p1 ;
    u.price2 <- p2 ;
    u.time_in_force <- Some (tif :> DTC.time_in_force_enum) ;
    u.order_quantity <- Some RespObj.(int64_exn e "orderQty" |> Int64.to_float) ;
    u.filled_quantity <- Some RespObj.(int64_exn e "cumQty" |> Int64.to_float) ;
    u.remaining_quantity <- Some RespObj.(int64_exn e "leavesQty" |> Int64.to_float) ;
    u.average_fill_price <- RespObj.(float e "avgPx") ;
    u.last_fill_price <- RespObj.(float e "lastPx") ;
    u.last_fill_date_time <- ts ;
    u.last_fill_quantity <- RespObj.(int64 e "lastQty" |> Option.map ~f:Int64.to_float) ;
    u.last_fill_execution_id <- Some RespObj.(string_exn e "execID") ;
    u.trade_account <- Some RespObj.(int64_exn e "account" |> Int64.to_string) ;
    u.free_form_text <- Some RespObj.(string_exn e "text") ;
    write_message w `order_update DTC.gen_order_update u ;
    true

let write_position_update ?request_id ~nb_msgs ~msg_number w p =
  let symbol = RespObj.string_exn p "symbol" in
  let trade_account = RespObj.int_exn p "account" in
  let avgEntryPrice = RespObj.float_exn p "avgEntryPrice" in
  let currentQty = RespObj.int_exn p "currentQty" in
  let u = DTC.default_position_update () in
  u.total_number_messages <- Some nb_msgs ;
  u.message_number <- Some msg_number ;
  u.request_id <- request_id ;
  u.symbol <- Some symbol ;
  u.exchange <- Some !my_exchange ;
  u.trade_account <- Some (Int.to_string trade_account) ;
  u.average_price <- Some avgEntryPrice ;
  u.quantity <- Some (Int.to_float currentQty) ;
  write_message w `position_update DTC.gen_position_update u

let write_balance_update ~msg_number ~nb_msgs ~unsolicited w m =
  let u = DTC.default_account_balance_update () in
  u.total_number_messages <- Some nb_msgs ;
  u.message_number <- Some msg_number ;
  u.account_currency <- Some "mXBT" ;
  u.cash_balance <- Some RespObj.(Int64.(to_float @@ int64_exn m "walletBalance") /. 1e5) ;
  u.balance_available_for_new_positions <-
    Some RespObj.(Int64.(to_float @@ int64_exn m "availableMargin") /. 1e5) ;
  u.securities_value <-
    Some RespObj.(Int64.(to_float @@ int64_exn m "marginBalance") /. 1e5) ;
  u.margin_requirement <- RespObj.(Int64.(
      Some (to_float (int64_exn m "initMargin" +
                      int64_exn m "maintMargin" +
                      int64_exn m "sessionMargin") /. 1e5))) ;
  u.trade_account <- Some RespObj.(int64_exn m "account" |> Int64.to_string) ;
  write_message w `account_balance_update DTC.gen_account_balance_update u

type subscribe_msg =
  | Subscribe of client
  | Unsubscribe of Uuid.t * string

let client_ws_r, client_ws_w = Pipe.create ()

let client_ws ({ addr_str; w; ws_r; key; secret; order; margin; position; } as c) =
  let order_partial_done = Ivar.create () in
  let margin_partial_done = Ivar.create () in
  let position_partial_done = Ivar.create () in

  let process_orders action orders =
    let orders = List.map orders ~f:RespObj.of_json in
    List.iter orders ~f:begin fun o ->
      let oid = RespObj.string_exn o "orderID" in
      let oid = Uuid.of_string oid in
      match action with
      | WS.Response.Update.Delete ->
        Uuid.Table.remove order oid;
        Log.debug log_bitmex "<- [%s] order delete" addr_str
      | Insert
      | Partial ->
        Uuid.Table.set order ~key:oid ~data:o;
        Log.debug log_bitmex "<- [%s] order insert/partial" addr_str
      | Update ->
        if Ivar.is_full order_partial_done then begin
          let data = match Uuid.Table.find order oid with
            | None -> o
            | Some old_o -> RespObj.merge old_o o
          in
          Uuid.Table.set order ~key:oid ~data;
          Log.debug log_bitmex "<- [%s] order update" addr_str
        end
    end;
    if action = Partial then Ivar.fill_if_empty order_partial_done ()
  in
  let process_margins action margins =
    let margins = List.map margins ~f:RespObj.of_json in
    List.iteri margins ~f:begin fun i m ->
      let a = RespObj.int_exn m "account" in
      let c = RespObj.string_exn m "currency" in
      match action with
      | WS.Response.Update.Delete ->
        IS.Table.remove margin (a, c);
        Log.debug log_bitmex "<- [%s] margin delete" addr_str
      | Insert
      | Partial ->
        IS.Table.set margin ~key:(a, c) ~data:m;
        Log.debug log_bitmex "<- [%s] margin insert/partial" addr_str;
        write_balance_update ~unsolicited:true ~msg_number:1l ~nb_msgs:1l w m
      | Update ->
        if Ivar.is_full margin_partial_done then begin
          let m = match IS.Table.find margin (a, c) with
            | None -> m
            | Some old_m -> RespObj.merge old_m m
          in
          IS.Table.set margin ~key:(a, c) ~data:m;
          write_balance_update ~unsolicited:true ~msg_number:1l ~nb_msgs:1l w m ;
          Log.debug log_bitmex "<- [%s] margin update" addr_str
        end
    end;
    if action = Partial then Ivar.fill_if_empty margin_partial_done ()
  in
  let process_positions action positions =
    let positions = List.map positions ~f:RespObj.of_json in
    List.iter positions ~f:begin fun p ->
      let a = RespObj.int_exn p "account" in
      let s = RespObj.string_exn p "symbol" in
      match action with
      | WS.Response.Update.Delete ->
        IS.Table.remove position (a, s);
        Log.debug log_bitmex "<- [%s] position delete" addr_str
      | Insert | Partial ->
        IS.Table.set position ~key:(a, s) ~data:p;
        if RespObj.bool_exn p "isOpen" then begin
          write_position_update ~nb_msgs:1l ~msg_number:1l w p ;
          Log.debug log_bitmex "<- [%s] position insert/partial" addr_str
        end
      | Update ->
        if Ivar.is_full position_partial_done then begin
          let old_p, p = match IS.Table.find position (a, s) with
            | None -> None, p
            | Some old_p -> Some old_p, RespObj.merge old_p p
          in
          IS.Table.set position ~key:(a, s) ~data:p;
          match old_p with
          | Some old_p when RespObj.bool_exn old_p "isOpen" ->
            write_position_update ~nb_msgs:1l ~msg_number:1l w p ;
            Log.debug log_dtc "<- [%s] position update %s" addr_str s
          | _ -> ()
        end
    end;
    if action = Partial then Ivar.fill_if_empty position_partial_done ()
  in
  let process_execs action execs =
    let fold_f i e =
      let symbol = RespObj.string_exn e "symbol" in
      match action with
      | WS.Response.Update.Insert ->
        Log.debug log_bitmex "<- [%s] exec %s" addr_str symbol;
        if write_order_update ~nb_msgs:1l ~msg_number:1l w e then succ i else i
      | _ -> i
    in
    let execs = List.map execs ~f:RespObj.of_json in
    let nb_execs = List.fold_left execs ~init:0 ~f:fold_f in
    if nb_execs > 0 then Log.debug log_dtc "-> [%s] OrderUpdate %d" addr_str nb_execs
  in
  let on_update { WS.Response.Update.table; action; data } =
    match table, action, data with
      | "order", action, orders -> process_orders action orders
      | "margin", action, margins -> process_margins action margins
      | "position", action, positions -> process_positions action positions
      | "execution", action, execs -> process_execs action execs
      | table, _, _ -> Log.error log_bitmex "Unknown table %s" table
  in
  Pipe.write client_ws_w @@ Subscribe c >>= fun () ->
  don't_wait_for @@ Monitor.handle_errors
    (fun () -> Pipe.iter_without_pushback ~continue_on_error:true ws_r ~f:on_update)
    (fun exn -> Log.error log_bitmex "%s" @@ Exn.to_string exn);
  Deferred.all_unit (List.map ~f:Ivar.read [order_partial_done;
                                            position_partial_done;
                                            margin_partial_done])

let encoding_request addr w req =
  Log.debug log_dtc "<- [%s] Encoding Request" addr ;
  Dtc_pb.Encoding.(to_string (Response { version = 7 ; encoding = Protobuf })) |>
  Writer.write w ;
  Log.debug log_dtc "-> [%s] Encoding Response" addr

let heartbeat addr w msg =  Log.debug log_dtc "<- [%s] Heartbeat" addr

(* let process addr w msg_cs scratchbuf = *)
(*   let addr_str = Socket.Address.Inet.to_string addr in *)
(*   (\* Erase scratchbuf by security. *\) *)
(*   Bigstring.set_tail_padded_fixed_string *)
(*     scratchbuf ~padding:'\x00' ~pos:0 ~len:(Bigstring.length scratchbuf) ""; *)
(*   let msg = msg_of_enum Cstruct.LE.(get_uint16 msg_cs 2) in *)
(*   let client = match msg with *)
(*     | None -> None *)
(*     | Some EncodingRequest -> None *)
(*     | Some LogonRequest -> None *)
(*     | Some msg -> begin *)
(*         match InetAddr.Table.find clients addr with *)
(*         | None -> *)
(*           Log.error log_dtc "msg type %s and found no client record" (show_msg msg); *)
(*           failwith "internal error: no client record" *)
(*         | Some client -> Some client *)
(*       end *)
(*   in *)
(*   match msg with *)
(*   | Some EncodingRequest -> *)
(*     let open Encoding in *)
(*     Log.debug log_dtc "<- [%s] EncodingReq" addr_str; *)
(*     let response_cs = Cstruct.of_bigarray scratchbuf ~len:Response.sizeof_cs in *)
(*     Response.write response_cs; *)
(*     Writer.write_cstruct w response_cs; *)
(*     Log.debug log_dtc "-> [%s] EncodingResp" addr_str *)

(*   | Some LogonRequest -> *)
(*     let open Logon in *)
(*     let m = Request.read msg_cs in *)
(*     let response_cs = Cstruct.of_bigarray scratchbuf ~len:Response.sizeof_cs in *)
(*     Log.debug log_dtc "<- [%s] %s" addr_str (Request.show m); *)
(*     let create_client ~key ~secret ~stop_exec_inst = *)
(*       let ws_r, ws_w = Pipe.create () in *)
(*       { *)
(*         addr ; addr_str ; w ; ws_r ; ws_w ; key ; *)
(*         secret = Cstruct.of_string secret ; *)
(*         position = IS.Table.create () ; *)
(*         margin = IS.Table.create () ; *)
(*         order = Uuid.Table.create () ; *)
(*         subs = String.Table.create () ; *)
(*         subs_depth = String.Table.create () ; *)
(*         parents = String.Table.create () ; *)
(*         stop_exec_inst ; *)

(*         current_parent = None ; *)
(*         ws_uuid = Uuid.create () ; *)
(*         dropped = 0 ; *)
(*       } *)
(*     in *)
(*     let accept client stop_exec_inst trading = *)
(*       let trading_supported, result_text = *)
(*         match trading with *)
(*         | Ok msg -> true, Printf.sprintf "Trading enabled: %s" msg *)
(*         | Error msg -> false, Printf.sprintf "Trading disabled: %s" msg *)
(*       in *)
(*       let resp = *)
(*         Response.create *)
(*           ~server_name:"BitMEX" *)
(*           ~result:LogonStatus.Success *)
(*           ~result_text *)
(*           ~security_definitions_supported:true *)
(*           ~market_data_supported:true *)
(*           ~historical_price_data_supported:true *)
(*           ~market_depth_supported:true *)
(*           ~market_depth_updates_best_bid_and_ask:true *)
(*           ~trading_supported *)
(*           ~ocr_supported:true *)
(*           ~oco_supported:true *)
(*           ~bracket_orders_supported:true *)
(*           () *)
(*       in *)
(*       don't_wait_for @@ heartbeat client m.Request.heartbeat_interval; *)
(*       Response.to_cstruct response_cs resp; *)
(*       Writer.write_cstruct w response_cs; *)
(*       Log.debug log_dtc "-> [%s] %s" addr_str (Response.show resp); *)
(*       let secdef_resp_cs = Cstruct.of_bigarray scratchbuf ~len:SecurityDefinition.Response.sizeof_cs in *)
(*       let on_instrument { secdef } = *)
(*         SecurityDefinition.Response.to_cstruct secdef_resp_cs { secdef with final = true }; *)
(*         Writer.write_cstruct w secdef_resp_cs *)
(*       in *)
(*       ignore @@ String.Table.iter instruments ~f:on_instrument *)
(*     in *)
(*     let setup_client_ws client = *)
(*       let ws_initialized = client_ws client in *)
(*       let ws_ok = choice ws_initialized (fun () -> *)
(*           Log.info log_dtc "BitMEX accepts API key for %s" m.username; *)
(*           Result.return "API key valid." *)
(*         ) *)
(*       in *)
(*       let timeout = choice (Clock_ns.after @@ Time_ns.Span.of_int_sec 20) (fun () -> *)
(*           Log.info log_dtc "BitMEX rejects API key for %s" m.username; *)
(*           Result.fail "BitMEX rejects your API key." *)
(*         ) *)
(*       in *)
(*       choose [ws_ok; timeout] *)
(*     in *)
(*     let stop_exec_inst = if Int32.bit_and m.integer_1 1l = 1l then `LastPrice else `MarkPrice in begin *)
(*       match m.username, m.password with *)
(*       | "", _ -> *)
(*         let client = create_client "" "" stop_exec_inst in *)
(*         InetAddr.Table.set clients ~key:addr ~data:client; *)
(*         accept client stop_exec_inst @@ Result.fail "No login provided, data only" *)
(*       | key, secret -> *)
(*         let client = create_client key secret stop_exec_inst in *)
(*         InetAddr.Table.set clients ~key:addr ~data:client; *)
(*         don't_wait_for (setup_client_ws client >>| accept client stop_exec_inst) *)
(*     end *)

(*   | Some Heartbeat -> *)
(*     Option.iter client ~f:begin fun { addr_str } -> *)
(*       Log.debug log_dtc "<- [%s] HB" addr_str *)
(*     end *)

(*   | Some SecurityDefinitionForSymbolRequest -> *)
(*     let open SecurityDefinition in *)
(*     let { Request.id; symbol; exchange } = Request.read msg_cs in *)
(*     let { addr_str; _ } = Option.value_exn client in *)
(*     Log.debug log_dtc "<- [%s] SeqDefReq %s-%s" addr_str symbol exchange; *)
(*     let reject_cs = Cstruct.of_bigarray scratchbuf ~len:Reject.sizeof_cs in *)
(*     let reject k = Printf.ksprintf begin fun msg -> *)
(*         Reject.write reject_cs ~request_id:id "%s" msg; *)
(*         Log.debug log_dtc "-> [%s] SeqDefRej" addr_str; *)
(*         Writer.write_cstruct w reject_cs *)
(*       end k *)
(*     in *)
(*     if !my_exchange <> exchange && not Instrument.(is_index symbol) then *)
(*       reject "No such symbol %s-%s" symbol exchange *)
(*     else begin match String.Table.find instruments symbol with *)
(*     | None -> reject "No such symbol %s-%s" symbol exchange *)
(*     | Some { secdef } -> *)
(*       let open Response in *)
(*       let secdef = { secdef with request_id = id; final = true } in *)
(*       Log.debug log_dtc "-> [%s] %s" addr_str (show secdef); *)
(*       let secdef_resp_cs = Cstruct.of_bigarray scratchbuf ~len:sizeof_cs in *)
(*       Response.to_cstruct secdef_resp_cs secdef; *)
(*       Writer.write_cstruct w secdef_resp_cs *)
(*     end *)

(*   | Some MarketDataRequest -> *)
(*     let open MarketData in *)
(*     let { Request.action; symbol_id; symbol; exchange } = Request.read msg_cs in *)
(*     Log.debug log_dtc "<- [%s] MarketDataReq %s %s-%s" addr_str (RequestAction.show action) symbol exchange; *)
(*     let { addr_str; subs; _ } = Option.value_exn client in *)
(*     let r_cs = Cstruct.of_bigarray ~off:0 ~len:Reject.sizeof_cs scratchbuf in *)
(*     let reject k = Printf.ksprintf begin fun reason -> *)
(*         Reject.write r_cs ~symbol_id "%s" reason; *)
(*         Log.debug log_dtc "-> [%s] Market Data Reject: %s" addr_str reason; *)
(*         Writer.write_cstruct w r_cs *)
(*       end k *)
(*     in *)
(*     let snap_of_instr { instrObj; last_trade_price; last_trade_size; last_trade_ts; last_quote_ts } = *)
(*       if action = Unsubscribe then String.Table.remove subs symbol; *)
(*       if action = Subscribe then String.Table.set subs symbol symbol_id; *)
(*       if Instrument.is_index symbol then *)
(*         Snapshot.create *)
(*           ~symbol_id:symbol_id *)
(*           ?session_settlement_price:(RespObj.float instrObj "prevPrice24h") *)
(*           ?last_trade_p:(RespObj.float instrObj "lastPrice") *)
(*           ?last_trade_ts:(RespObj.string instrObj "timestamp" |> Option.map ~f:Time_ns.of_string) *)
(*           () *)
(*       else *)
(*         (\* let open RespObj in *\) *)
(*         let { Quote.bidPrice; bidSize; askPrice; askSize } = String.Table.find_exn quotes symbol in *)
(*         let open RespObj in *)
(*         Snapshot.create *)
(*           ~symbol_id *)
(*           ~session_settlement_price:Option.(value ~default:Float.max_finite_value (float instrObj "indicativeSettlePrice")) *)
(*           ~session_h:Option.(value ~default:Float.max_finite_value @@ float instrObj "highPrice") *)
(*           ~session_l:Option.(value ~default:Float.max_finite_value @@ float instrObj "lowPrice") *)
(*           ~session_v:Option.(value_map (int64 instrObj "volume") ~default:Float.max_finite_value ~f:Int64.to_float) *)
(*           ~open_interest:Option.(value_map (int64 instrObj "openInterest") ~default:0xffffffffl ~f:Int64.to_int32_exn) *)
(*           ?bid:bidPrice *)
(*           ?bid_qty:Option.(map bidSize ~f:Float.of_int) *)
(*           ?ask:askPrice *)
(*           ?ask_qty:Option.(map askSize ~f:Float.of_int) *)
(*           ~last_trade_p:last_trade_price *)
(*           ~last_trade_v:(Int.to_float last_trade_size) *)
(*           ~last_trade_ts *)
(*           ~bid_ask_ts:last_quote_ts *)
(*           () *)
(*     in *)
(*     if !my_exchange <> exchange && not Instrument.(is_index symbol) then *)
(*       reject "No such symbol %s-%s" symbol exchange *)
(*     else begin match String.Table.find instruments symbol with *)
(*       | None -> reject "No such symbol %s-%s" symbol exchange *)
(*       | Some instr -> try *)
(*           let snap = snap_of_instr instr in *)
(*           let snap_cs = Cstruct.of_bigarray scratchbuf ~off:0 ~len:Snapshot.sizeof_cs in *)
(*           Snapshot.to_cstruct snap_cs snap; *)
(*           Log.debug log_dtc "-> [%s] %s" addr_str (Snapshot.show snap); *)
(*           Writer.write_cstruct w snap_cs *)
(*         with *)
(*         | Not_found -> *)
(*           Log.error log_dtc "market data request: no quote found for %s" symbol; *)
(*           reject "No market data for symbol %s-%s" symbol exchange; *)
(*         | exn -> *)
(*           Log.error log_dtc "%s" Exn.(to_string exn); *)
(*           reject "No market data for symbol %s-%s" symbol exchange; *)
(*     end *)

(*   | Some MarketDepthRequest -> *)
(*     let open MarketDepth in *)
(*     let { Request.action; symbol_id; symbol; exchange; nb_levels } = Request.read msg_cs in *)
(*     let { addr_str; subs_depth; _ } = Option.value_exn client in *)
(*     Log.debug log_dtc "<- [%s] MarketDepthReq %s-%s %d" addr_str symbol exchange nb_levels; *)
(*     let r_cs = Cstruct.of_bigarray ~off:0 ~len:Reject.sizeof_cs scratchbuf in *)
(*     let reject k = Printf.ksprintf begin fun msg -> *)
(*         Reject.write r_cs ~symbol_id "%s" msg; *)
(*         Log.debug log_dtc "-> [%s] MarketDepthRej: %s" addr_str msg; *)
(*         Writer.write_cstruct w r_cs *)
(*       end k *)
(*     in *)
(*     let accept { bids; asks } = *)
(*       if action = Unsubscribe then String.Table.remove subs_depth symbol; *)
(*       if action = Subscribe then String.Table.set subs_depth symbol symbol_id; *)
(*       let snap_cs = Cstruct.of_bigarray scratchbuf ~off:0 ~len:Snapshot.sizeof_cs in *)
(*       let bids = mapify_ob bids in *)
(*       let asks = mapify_ob asks in *)
(*       if Float.Map.(is_empty bids && is_empty asks) then begin *)
(*         Snapshot.write snap_cs ~symbol_id ~p:0. ~v:0. ~lvl:0 ~first:true ~last:true; *)
(*         Writer.write_cstruct w snap_cs *)
(*       end *)
(*       else begin *)
(*         Float.Map.fold_right bids ~init:1 ~f:begin fun ~key:price ~data:size lvl -> *)
(*           Snapshot.write snap_cs *)
(*             ~symbol_id *)
(*             ~side:`Buy ~p:price ~v:Float.(of_int size) ~lvl *)
(*             ~first:(lvl = 1) ~last:false; *)
(*           Writer.write_cstruct w snap_cs; *)
(*           succ lvl *)
(*         end |> ignore; *)
(*         Float.Map.fold asks ~init:1 ~f:begin fun ~key:price ~data:size lvl -> *)
(*           Snapshot.write snap_cs *)
(*             ~symbol_id *)
(*             ~side:`Sell ~p:price ~v:Float.(of_int size) ~lvl *)
(*             ~first:(lvl = 1 && Float.Map.is_empty bids) ~last:false; *)
(*           Writer.write_cstruct w snap_cs; *)
(*           succ lvl *)
(*         end |> ignore; *)
(*         Snapshot.write snap_cs ~symbol_id ~p:0. ~v:0. ~lvl:0 ~first:false ~last:true; *)
(*         Writer.write_cstruct w snap_cs *)
(*       end *)
(*     in *)
(*     if Instrument.is_index symbol then *)
(*       reject "%s is an index" symbol *)
(*     else if !my_exchange <> exchange then *)
(*       reject "No such symbol %s-%s" symbol exchange *)
(*     else if action <> Unsubscribe && not @@ String.Table.mem instruments symbol then *)
(*       reject "No such symbol %s-%s" symbol exchange *)
(*     else begin match String.Table.find orderbooks symbol with *)
(*       | None -> *)
(*         Log.error log_dtc "MarketDepthReq: found no orderbooks for %s" symbol; *)
(*         reject "Found no orderbook for symbol %s-%s" symbol exchange *)
(*       | Some obs -> *)
(*         accept obs *)
(*     end *)

(*   | Some HistoricalPriceDataRequest -> *)
(*     let open HistoricalPriceData in *)
(*     let m = Request.read msg_cs in *)
(*     let { addr_str; _ } = Option.value_exn client in *)
(*     Log.debug log_dtc "<- [%s] HistPriceDataReq %s-%s" addr_str m.symbol m.exchange; *)
(*     let r_cs = Cstruct.of_bigarray ~off:0 ~len:Reject.sizeof_cs scratchbuf in *)
(*     let reject k = Printf.ksprintf begin fun reason -> *)
(*         Reject.write r_cs ~request_id:m.request_id "%s" reason; *)
(*         Log.debug log_dtc "-> [%s] HistPriceDataRej" addr_str; *)
(*         Writer.write_cstruct w r_cs; *)
(*       end k *)
(*     in *)
(*     let accept () = *)
(*       match String.Table.find instruments m.Request.symbol with *)
(*        | None -> *)
(*          reject "No such symbol %s-%s" m.symbol m.exchange; *)
(*          Deferred.unit *)
(*        | Some _ -> *)
(*          let f addr te_r te_w = *)
(*            Writer.write_cstruct te_w msg_cs; *)
(*            let r_pipe = Reader.pipe te_r in *)
(*            let w_pipe = Writer.pipe w in *)
(*            Pipe.transfer_id r_pipe w_pipe >>| fun () -> *)
(*            Log.debug log_dtc "-> [%s] <historical data>" addr_str *)
(*          in *)
(*          Monitor.try_with_or_error *)
(*            ~name:"historical_with_connection" *)
(*            (fun () -> Tcp.(with_connection (to_file !bitmex_historical) f)) >>| function *)
(*          | Error err -> reject "Historical data server unavailable" *)
(*          | Ok () -> () *)
(*     in *)
(*     if !my_exchange <> m.exchange && not Instrument.(is_index m.symbol) then *)
(*       reject "No such symbol %s-%s" m.symbol m.exchange *)
(*     else don't_wait_for @@ accept () *)

(*   | Some OpenOrdersRequest -> *)
(*     let open Trading.Order.Open in *)
(*     let m = Request.read msg_cs in *)
(*     let { addr_str; order; _ } = Option.value_exn client in *)
(*     Log.debug log_dtc "<- [%s] %s" addr_str (Request.show m); *)
(*     let nb_open_orders, open_orders = Uuid.Table.fold order *)
(*         ~init:(0, []) *)
(*         ~f:begin fun ~key:_ ~data ((nb_open_orders, os) as acc) -> *)
(*           match RespObj.(string_exn data "ordStatus") with *)
(*           | "New" | "PartiallyFilled" | "PendingCancel" -> *)
(*             (succ nb_open_orders, data :: os) *)
(*           | _ -> acc *)
(*         end *)
(*     in *)
(*     let open Trading.Order.Update in *)
(*     let update_cs = Cstruct.of_bigarray ~off:0 ~len:sizeof_cs scratchbuf in *)
(*     let send_order_update i data = *)
(*       let ord_type = ord_type_of_string RespObj.(string_exn data "ordType") in *)
(*       let price = RespObj.(float_or_null_exn ~default:Float.max_finite_value data "price") in *)
(*       let stopPx = RespObj.(float_or_null_exn ~default:Float.max_finite_value data "stopPx") in *)
(*       let p1, p2 = p1_p2_of_bitmex ~ord_type ~stopPx ~price in *)
(*       write *)
(*         ~nb_msgs:nb_open_orders *)
(*         ~msg_number:(succ i) *)
(*         ~status:`Open (\* FIXME: PartiallyFilled ?? *\) *)
(*         ~reason:UpdateReason.Open_orders_request_response *)
(*         ~request_id:m.Request.id *)
(*         ~symbol:RespObj.(string_exn data "symbol") *)
(*         ~exchange:!my_exchange *)
(*         ~cli_ord_id:RespObj.(string_exn data "clOrdID") *)
(*         ~srv_ord_id:RespObj.(string_exn data "orderID" |> b64_of_uuid) *)
(*         ~xch_ord_id:RespObj.(string_exn data "orderID" |> b64_of_uuid) *)
(*         ~ord_type:(ord_type_of_string RespObj.(string_exn data "ordType")) *)
(*         ?side:(RespObj.string_exn data "side" |> side_of_bmex) *)
(*         ?p1 *)
(*         ?p2 *)
(*         ~order_qty:RespObj.(int64_exn data "orderQty" |> Int64.to_float) *)
(*         ~filled_qty:RespObj.(int64_exn data "cumQty" |> Int64.to_float) *)
(*         ~remaining_qty:RespObj.(int64_exn data "leavesQty" |> Int64.to_float) *)
(*         ~tif:(tif_of_string RespObj.(string_exn data "timeInForce")) *)
(*         ~trade_account:RespObj.(int64_exn data "account" |> Int64.to_string) *)
(*         ~free_form_text:RespObj.(string_exn data "text") *)
(*         update_cs; *)
(*       Writer.write_cstruct w update_cs; *)
(*     in *)
(*     List.iteri open_orders ~f:send_order_update; *)
(*     if nb_open_orders = 0 then begin *)
(*       write ~nb_msgs:1 ~msg_number:1 ~request_id:m.Request.id *)
(*         ~reason:UpdateReason.Open_orders_request_response ~no_orders:true update_cs; *)
(*       Writer.write_cstruct w update_cs *)
(*     end; *)
(*     Log.debug log_dtc "-> [%s] %d orders" addr_str nb_open_orders; *)

(*   | Some CurrentPositionsRequest -> *)
(*     let open Trading.Position in *)
(*     let m = Request.read msg_cs in *)
(*     let update_cs = Cstruct.of_bigarray ~off:0 ~len:Update.sizeof_cs scratchbuf in *)
(*     let { addr_str; position; _ } = Option.value_exn client in *)
(*     Log.debug log_dtc "<- [%s] PosReq (%s)" addr_str m.Request.trade_account; *)
(*     let nb_open_positions, open_positions = IS.Table.fold position *)
(*         ~init:(0, []) *)
(*         ~f:begin fun ~key:_ ~data ((nb_open_ps, open_ps) as acc) -> *)
(*           if RespObj.bool_exn data "isOpen" then *)
(*             succ nb_open_ps, data :: open_ps *)
(*           else *)
(*             acc *)
(*         end *)
(*     in *)
(*     List.iteri open_positions ~f:begin fun i -> *)
(*       write_position_update ~request_id:m.id ~nb_msgs:nb_open_positions ~msg_number:(succ i) w update_cs *)
(*     end; *)
(*     if nb_open_positions = 0 then begin *)
(*       Update.write ~nb_msgs:1 ~msg_number:1 ~request_id:m.Request.id ~no_positions:true update_cs; *)
(*       Writer.write_cstruct w update_cs *)
(*     end; *)
(*     Log.debug log_dtc "-> [%s] %d positions" addr_str nb_open_positions; *)

(*   | Some HistoricalOrderFillsRequest -> *)
(*     let open Trading.Order.Fills in *)
(*     let m = Request.read msg_cs in *)
(*     let response_cs = Cstruct.of_bigarray scratchbuf ~len:Response.sizeof_cs in *)
(*     let { addr_str; key; secret; _ } = Option.value_exn client in *)
(*     Log.debug log_dtc "<- [%s] HistFillsReq" addr_str; *)
(*     let uri = Uri.with_path !base_uri "/api/v1/execution/tradeHistory" in *)
(*     let req = List.filter_opt [ *)
(*         if m.Request.srv_order_id <> "" then Some ("orderID", `String m.Request.srv_order_id) else None; *)
(*         Some ("execType", `String "Trade") *)
(*       ] *)
(*     in *)
(*     let uri = Uri.with_query' uri ["filter", Yojson.Safe.to_string @@ `Assoc req] in *)
(*     let process_body = function *)
(*     | `List orders -> *)
(*       let nb_msgs = List.length orders in *)
(*       List.iteri orders ~f:begin fun i o -> *)
(*         let o = RespObj.of_json o in *)
(*         Response.write *)
(*           ~nb_msgs *)
(*           ~msg_number:(succ i) *)
(*           ~request_id:m.Request.id *)
(*           ~symbol:RespObj.(string_exn o "symbol") *)
(*           ~exchange:!my_exchange *)
(*           ~srv_order_id:RespObj.(string_exn o "orderID" |> b64_of_uuid) *)
(*           ~p:RespObj.(float_exn o "avgPx") *)
(*           ~v:Float.(of_int64 RespObj.(int64_exn o "orderQty")) *)
(*           ?ts:(RespObj.string o "transactTime" |> Option.map ~f:Time_ns.of_string) *)
(*           ?side:(RespObj.string_exn o "side" |> side_of_bmex) *)
(*           ~exec_id:RespObj.(string_exn o "execID" |> b64_of_uuid) *)
(*           response_cs; *)
(*         Writer.write_cstruct w response_cs *)
(*       end; *)
(*       Log.debug log_dtc "-> [%s] HistOrdFillsResp %d" addr_str nb_msgs *)
(*     | #Yojson.Safe.json -> invalid_arg "bitmex historical order fills response" *)
(*     in *)
(*     let get_fills_and_reply () = *)
(*       Rest.call ~name:"execution" ~f:begin fun uri -> *)
(*         Client.get ~headers:(Rest.mk_headers ~key ~secret `GET uri) uri *)
(*       end uri >>| function *)
(*       | Ok body -> process_body body *)
(*       | Error err -> *)
(*         (\* TODO: reject on error *\) *)
(*         Log.error log_bitmex "%s" @@ Error.to_string_hum err *)
(*     in don't_wait_for @@ get_fills_and_reply () (\* TODO: reject on error *\) *)

(*   | Some TradeAccountsRequest -> *)
(*     let open Account.List in *)
(*     let m = Request.read msg_cs in *)
(*     let { addr_str; margin; _ } = Option.value_exn client in *)
(*     Log.debug log_dtc "<- [%s] TradeAccountsReq" addr_str; *)
(*     let response_cs = *)
(*       Cstruct.of_bigarray ~off:0 ~len:Response.sizeof_cs scratchbuf in *)
(*     let account, _ = List.hd_exn @@ IS.Table.keys margin in *)
(*     Response.write ~request_id:m.Request.id ~msg_number:1 ~nb_msgs:1 *)
(*       ~trade_account:Int.(to_string account) response_cs; *)
(*     Writer.write_cstruct w response_cs; *)
(*     Log.debug log_dtc "-> [%s] TradeAccountResp %d" addr_str account *)

(*   | Some AccountBalanceRequest -> *)
(*     let open Account.Balance in *)
(*     let m = Request.read msg_cs in *)
(*     let { addr_str; margin; _ } = Option.value_exn client in *)
(*     let r_cs = Cstruct.of_bigarray ~off:0 ~len:Reject.sizeof_cs scratchbuf in *)
(*     let update_cs = Cstruct.of_bigarray ~off:0 ~len:Update.sizeof_cs scratchbuf in *)
(*     let reject k = Printf.ksprintf begin fun reason -> *)
(*         Reject.write r_cs ~request_id:m.id "%s" reason; *)
(*         Writer.write_cstruct w r_cs; *)
(*         Log.debug log_dtc "-> [%s] AccountBalanceRej" addr_str; *)
(*       end k *)
(*     in *)
(*     let write_no_balances request_id = *)
(*       Account.Balance.Update.write ~request_id ~nb_msgs:1 ~msg_number:1 ~no_account_balance:true update_cs; *)
(*       Writer.write_cstruct w update_cs; *)
(*       Log.debug log_dtc "-> [%s] no account balance" addr_str *)
(*     in *)
(*     let update ~msg_number ~nb_msgs account_id obj = *)
(*       write_balance_update ~unsolicited:false ~msg_number ~nb_msgs update_cs obj; *)
(*       Writer.write_cstruct w update_cs; *)
(*       Log.debug log_dtc "-> [%s] account balance %d" addr_str account_id *)
(*     in *)
(*     Log.debug log_dtc "<- [%s] AccountBalanceReq (%s)" *)
(*       addr_str m.Request.trade_account; *)
(*     let nb_msgs = IS.Table.length margin in *)
(*     if nb_msgs = 0 then *)
(*       write_no_balances m.id *)
(*     else if m.Request.trade_account = "" then *)
(*       ignore @@ IS.Table.fold margin ~init:1 *)
(*         ~f:(fun ~key:(account_id, currency) ~data msg_number -> *)
(*             update ~msg_number ~nb_msgs account_id data; *)
(*             succ msg_number *)
(*           ) *)
(*     else begin *)
(*       match IS.Table.find margin (Int.of_string m.Request.trade_account, "XBt") *)
(*       with *)
(*       | Some obj -> update ~msg_number:1 ~nb_msgs:1 Int.(of_string m.Request.trade_account) obj *)
(*       | None -> write_no_balances m.id *)
(*       | exception _ -> reject "Invalid trade account %s" m.Request.trade_account *)
(*     end *)

(*   | Some SubmitNewSingleOrder -> *)
(*     let module S = Trading.Order.Submit in *)
(*     let module U = Trading.Order.Update in *)
(*     let m = S.read msg_cs in *)
(*     let { addr_str; key; secret; current_parent; stop_exec_inst } as c = Option.value_exn client in *)
(*     let order_update_cs = Cstruct.of_bigarray ~off:0 ~len:U.sizeof_cs scratchbuf in *)
(*     Log.debug log_dtc "<- [%s] %s" addr_str (S.show m); *)
(*     let accept_exn () = *)
(*       let uri = Uri.with_path !base_uri "/api/v1/order" in *)
(*       let qty = match Option.value_exn ~message:"side is undefined" m.S.side with *)
(*         | `Buy -> m.S.qty *)
(*         | `Sell -> Float.neg m.S.qty *)
(*       in *)
(*       let tif = Option.value_exn ~message:"tif is undefined" m.tif in *)
(*       let ordType = Option.value_exn ~message:"ordType is undefined" m.ord_type in *)
(*       let tif = match tif with *)
(*       | `Good_till_date_time -> invalid_arg "good_till_date_time" *)
(*       | #time_in_force as tif -> tif in *)
(*       let body = *)
(*         ["symbol", `String m.S.symbol; *)
(*          "orderQty", `Float qty; *)
(*          "timeInForce", `String (string_of_tif tif); *)
(*          "ordType", `String (string_of_ord_type ordType); *)
(*          "clOrdID", `String m.S.cli_ord_id; *)
(*          "text", `String m.text; *)
(*         ] *)
(*         @ price_fields_of_dtc ordType ~p1:m.S.p1 ~p2:m.S.p2 *)
(*         @ execInst_of_dtc ordType tif stop_exec_inst *)
(*       in *)
(*       let body_str = Yojson.Safe.to_string @@ `Assoc body in *)
(*       let body = Body.of_string body_str in *)
(*       Log.debug log_bitmex "-> %s" body_str; *)
(*       Rest.call ~name:"submit" ~f:begin fun uri -> *)
(*         Client.post ~chunked:false ~body *)
(*           ~headers:(Rest.mk_headers ~key ~secret ~data:body_str `POST uri) uri *)
(*       end uri >>| function *)
(*       | Ok _body -> () *)
(*       | Error err -> *)
(*         let err_str = Error.to_string_hum err in *)
(*         Dtc_util.Trading.Order.Submit.reject order_update_cs m "%s" err_str; *)
(*         Writer.write_cstruct w order_update_cs; *)
(*         Log.error log_bitmex "%s" err_str *)
(*     in *)
(*     if !my_exchange <> m.S.exchange then begin *)
(*       Dtc_util.Trading.Order.Submit.reject order_update_cs m "Unknown exchange"; *)
(*       Writer.write_cstruct w order_update_cs; *)
(*     end *)
(*     else if m.S.tif = Some `Good_till_date_time then begin *)
(*       Dtc_util.Trading.Order.Submit.reject order_update_cs m "BitMEX does not support TIF Good till datetime"; *)
(*       Writer.write_cstruct w order_update_cs; *)
(*     end *)
(*     else if Option.is_some current_parent then begin *)
(*       c.current_parent <- None; *)
(*       Dtc_util.Trading.Order.Submit.reject order_update_cs Option.(value_exn current_parent) "Next received order was not an OCO"; *)
(*       Writer.write_cstruct w order_update_cs; *)
(*       Dtc_util.Trading.Order.Submit.reject order_update_cs m "Previous received order was also a parent and the current order is not an OCO"; *)
(*       Writer.write_cstruct w order_update_cs; *)
(*     end *)
(*     else if m.S.parent then begin *)
(*       set_parent_order c m; *)
(*       Dtc_util.Trading.Order.Submit.update *)
(*         ~status:`Pending_open ~reason:General_order_update *)
(*         order_update_cs m "parent order %s stored" m.S.cli_ord_id; *)
(*       Writer.write_cstruct w order_update_cs; *)
(*       Log.debug log_dtc "Stored parent order %s" m.S.cli_ord_id *)
(*     end *)
(*     else *)
(*       let on_exn _ = *)
(*         Dtc_util.Trading.Order.Submit.update *)
(*           ~status:`Rejected ~reason:New_order_rejected *)
(*           order_update_cs m *)
(*           "exception raised when trying to submit %s" m.S.cli_ord_id; *)
(*         Writer.write_cstruct w order_update_cs; *)
(*         Deferred.unit *)
(*       in *)
(*       don't_wait_for @@ eat_exn ~on_exn accept_exn *)

(*   | Some SubmitNewOCOOrder -> *)
(*     let module S = Trading.Order.SubmitOCO in *)
(*     let module S' = Trading.Order.Submit in *)
(*     let module U = Trading.Order.Update in *)
(*     let m = S.read msg_cs in *)
(*     let { addr_str; key; secret; current_parent; parents; stop_exec_inst } as c = Option.value_exn client in *)
(*     let order_update_cs = Cstruct.of_bigarray ~off:0 ~len:U.sizeof_cs scratchbuf in *)
(*     Log.debug log_dtc "<- [%s] %s" addr_str (S.show m); *)
(*     let reject cs m k = Printf.ksprintf begin fun reason -> *)
(*         U.write *)
(*           ~nb_msgs:1 *)
(*           ~msg_number:1 *)
(*           ~trade_account:m.S.trade_account *)
(*           ~status:`Rejected *)
(*           ~reason:New_order_rejected *)
(*           ~cli_ord_id:m.S.cli_ord_id_1 *)
(*           ~symbol:m.S.symbol *)
(*           ~exchange:m.S.exchange *)
(*           ?ord_type:m.S.ord_type_1 *)
(*           ?side:m.S.side_1 *)
(*           ~p1:m.S.p1_1 *)
(*           ~p2:m.S.p2_1 *)
(*           ~order_qty:m.S.qty_1 *)
(*           ?tif:m.S.tif *)
(*           ~good_till_ts:m.S.good_till_ts *)
(*           ~info_text:reason *)
(*           ~free_form_text:m.text *)
(*           cs; *)
(*         Writer.write_cstruct w cs; *)
(*         Log.debug log_dtc "-> [%s] Reject (%s)" addr_str reason; *)
(*         U.write *)
(*           ~nb_msgs:1 *)
(*           ~msg_number:1 *)
(*           ~trade_account:m.S.trade_account *)
(*           ~status:`Rejected *)
(*           ~reason:New_order_rejected *)
(*           ~cli_ord_id:m.S.cli_ord_id_2 *)
(*           ~symbol:m.S.symbol *)
(*           ~exchange:m.S.exchange *)
(*           ?ord_type:m.S.ord_type_2 *)
(*           ?side:m.S.side_2 *)
(*           ~p1:m.S.p1_2 *)
(*           ~p2:m.S.p2_2 *)
(*           ~order_qty:m.S.qty_2 *)
(*           ?tif:m.S.tif *)
(*           ~good_till_ts:m.S.good_till_ts *)
(*           ~info_text:reason *)
(*           ~free_form_text:m.text *)
(*           cs; *)
(*         Writer.write_cstruct w cs; *)
(*         Log.debug log_dtc "-> [%s] SubmitOCORej: %s" addr_str reason *)
(*       end k *)
(*     in *)
(*     let accept_exn ?parent m = *)
(*       let uri = Uri.with_path !base_uri "/api/v1/order/bulk" in *)
(*       let qty_1 = match m.S.side_1 with *)
(*         | Some `Buy -> m.S.qty_1 *)
(*         | Some `Sell -> Float.neg m.S.qty_1 *)
(*         | None -> invalid_arg "side1 is undefined" *)
(*       in *)
(*       let qty_2 = match m.S.side_2 with *)
(*         | Some `Buy -> m.S.qty_2 *)
(*         | Some `Sell -> Float.neg m.S.qty_2 *)
(*         | None -> invalid_arg "side2 is undefined" *)
(*       in *)
(*       let tif = Option.value_exn ~message:"tif is undefined" m.tif in *)
(*       let tif = match tif with *)
(*       | `Good_till_date_time -> invalid_arg "good_till_date_time" *)
(*       | #time_in_force as tif -> tif in *)
(*       let ordType1 = Option.value_exn ~message:"ordType1 is undefined" m.ord_type_1 in *)
(*       let ordType2 = Option.value_exn ~message:"ordType2 is undefined" m.ord_type_2 in *)
(*       let orders = match parent with *)
(*         | None -> *)
(*           [ *)
(*             ["symbol", `String m.S.symbol; *)
(*              "orderQty", `Float qty_1; *)
(*              "timeInForce", `String (string_of_tif tif); *)
(*              "ordType", `String (string_of_ord_type ordType1); *)
(*              "clOrdID", `String m.S.cli_ord_id_1; *)
(*              "contingencyType", `String "OneUpdatesTheOtherAbsolute"; *)
(*              "clOrdLinkID", `String m.S.cli_ord_id_1; *)
(*              "text", `String m.text; *)
(*             ] *)
(*             @ price_fields_of_dtc ordType1 ~p1:m.S.p1_1 ~p2:m.S.p2_1 *)
(*             @ execInst_of_dtc ordType1 tif stop_exec_inst; *)
(*             ["symbol", `String m.S.symbol; *)
(*              "orderQty", `Float qty_2; *)
(*              "timeInForce", `String (string_of_tif tif); *)
(*              "ordType", `String (string_of_ord_type ordType2); *)
(*              "clOrdID", `String m.S.cli_ord_id_2; *)
(*              "contingencyType", `String "OneUpdatesTheOtherAbsolute"; *)
(*              "clOrdLinkID", `String m.S.cli_ord_id_1; *)
(*              "text", `String m.text; *)
(*             ] *)
(*             @ price_fields_of_dtc ordType2 ~p1:m.S.p1_2 ~p2:m.S.p2_2 *)
(*             @ execInst_of_dtc ordType2 tif stop_exec_inst; *)
(*           ] *)
(*         | Some p -> *)
(*           let p_qty = match p.S'.side with *)
(*             | Some `Buy -> p.S'.qty *)
(*             | Some `Sell -> Float.neg p.S'.qty *)
(*             | None -> invalid_arg "side is undefined" *)
(*           in *)
(*           let pTif = Option.value_exn ~message:"tif is undefined" p.tif in *)
(*           let pTif = match pTif with *)
(*           | `Good_till_date_time -> invalid_arg "good_till_date_time" *)
(*           | #time_in_force as tif -> tif in *)
(*           let pOrdType = Option.value_exn ~message:"ordType1 is undefined" p.ord_type in *)
(*           [ *)
(*             ["symbol", `String p.S'.symbol; *)
(*              "orderQty", `Float p_qty; *)
(*              "timeInForce", `String (string_of_tif pTif); *)
(*              "ordType", `String (string_of_ord_type pOrdType); *)
(*              "clOrdID", `String p.S'.cli_ord_id; *)
(*              "contingencyType", `String "OneTriggersTheOther"; *)
(*              "clOrdLinkID", `String p.S'.cli_ord_id; *)
(*              "text", `String p.text; *)
(*             ] *)
(*             @ price_fields_of_dtc pOrdType ~p1:p.S'.p1 ~p2:p.S'.p2 *)
(*             @ execInst_of_dtc pOrdType pTif stop_exec_inst; *)
(*             ["symbol", `String m.S.symbol; *)
(*              "orderQty", `Float qty_1; *)
(*              "timeInForce", `String (string_of_tif tif); *)
(*              "ordType", `String (string_of_ord_type ordType1); *)
(*              "clOrdID", `String m.S.cli_ord_id_1; *)
(*              "contingencyType", `String "OneUpdatesTheOtherAbsolute"; *)
(*              "clOrdLinkID", `String p.S'.cli_ord_id; *)
(*              "text", `String m.text; *)
(*             ] *)
(*             @ price_fields_of_dtc ordType1 ~p1:m.S.p1_1 ~p2:m.S.p2_1 *)
(*             @ execInst_of_dtc ordType1 tif stop_exec_inst; *)
(*             ["symbol", `String m.S.symbol; *)
(*              "orderQty", `Float qty_2; *)
(*              "timeInForce", `String (string_of_tif tif); *)
(*              "ordType", `String (string_of_ord_type ordType2); *)
(*              "clOrdID", `String m.S.cli_ord_id_2; *)
(*              "contingencyType", `String "OneUpdatesTheOtherAbsolute"; *)
(*              "clOrdLinkID", `String p.S'.cli_ord_id; *)
(*              "text", `String m.text; *)
(*             ] *)
(*             @ price_fields_of_dtc ordType2 ~p1:m.S.p1_2 ~p2:m.S.p2_2 *)
(*             @ execInst_of_dtc ordType2 tif stop_exec_inst *)
(*           ] *)
(*       in *)
(*       let body_str = *)
(*         `Assoc [ "orders", `List List.(map orders ~f:(fun o -> `Assoc o)) ] |> Yojson.Safe.to_string *)
(*       in *)
(*       let body = Body.of_string body_str in *)
(*       Log.debug log_bitmex "-> %s" body_str; *)
(*       Rest.call ~name:"submit" ~f:begin fun uri -> *)
(*         Client.post ~chunked:false ~body *)
(*           ~headers:(Rest.mk_headers ~key ~secret ~data:body_str `POST uri) *)
(*           uri *)
(*       end uri >>| function *)
(*       | Ok _body -> *)
(*         if m.parent <> "" then begin *)
(*           String.Table.set parents m.cli_ord_id_1 m.parent; *)
(*           String.Table.set parents m.cli_ord_id_2 m.parent; *)
(*         end *)
(*         else begin *)
(*           String.Table.set parents m.cli_ord_id_1 m.cli_ord_id_1; *)
(*           String.Table.set parents m.cli_ord_id_2 m.cli_ord_id_1; *)
(*         end *)
(*       | Error err -> *)
(*         let err_str = Error.to_string_hum err in *)
(*         Option.iter parent ~f:(fun p -> *)
(*             Dtc_util.Trading.Order.Submit.reject order_update_cs p "%s" err_str; *)
(*             Writer.write_cstruct w order_update_cs *)
(*           ); *)
(*         reject order_update_cs m "%s" err_str; *)
(*         Log.error log_bitmex "%s" err_str *)
(*     in *)
(*     if !my_exchange <> m.S.exchange then *)
(*       reject order_update_cs m "Unknown exchange" *)
(*     else if m.S.tif = Some `Good_till_date_time then *)
(*       reject order_update_cs m "BitMEX does not support TIF Good till date time" *)
(*     else if Option.is_none current_parent && m.S.parent <> "" then *)
(*       reject order_update_cs m "%s/%s is a child of %s but parent could not be found" *)
(*         m.cli_ord_id_1 m.cli_ord_id_2 m.S.parent *)
(*     else *)
(*       let on_exn ?p _ = *)
(*         Option.iter p ~f:(fun p -> *)
(*             Dtc_util.Trading.Order.Submit.reject order_update_cs p *)
(*               "exception raised when trying to submit cli=%s" p.S'.cli_ord_id; *)
(*             Writer.write_cstruct w order_update_cs *)
(*           ); *)
(*         reject order_update_cs m *)
(*           "exception raised when trying to submit OCO cli=%s,%s" m.cli_ord_id_1 m.cli_ord_id_2; *)
(*         Deferred.unit *)
(*       in *)
(*       begin match current_parent with *)
(*         | None -> *)
(*           don't_wait_for @@ eat_exn ~on_exn (fun () -> accept_exn m) *)
(*         | Some p when p.S'.cli_ord_id = m.S.parent -> *)
(*           c.current_parent <- None; *)
(*           don't_wait_for @@ *)
(*           eat_exn ~on_exn:(on_exn ~p) (fun () -> accept_exn ~parent:p m) *)
(*         | Some p -> *)
(*           c.current_parent <- None; *)
(*           (\* Reject the parent *\) *)
(*           Dtc_util.Trading.Order.Submit.reject order_update_cs p *)
(*             "parent order %s deleted (child %s/%s has parent %s)" *)
(*             p.cli_ord_id m.cli_ord_id_1 m.cli_ord_id_2 m.parent; *)
(*           Writer.write_cstruct w order_update_cs; *)
(*           (\* Reject the child *\) *)
(*           reject order_update_cs m *)
(*             "order %s/%s do not match stored parent %s (expected %s)" *)
(*             m.cli_ord_id_1 m.cli_ord_id_2 p.cli_ord_id m.parent *)
(*       end *)

(*   | Some CancelReplaceOrder -> *)
(*     let open Trading.Order in *)
(*     let reject m k = *)
(*       let order_update_cs = Cstruct.of_bigarray ~off:0 *)
(*           ~len:Update.sizeof_cs scratchbuf *)
(*       in *)
(*       Printf.ksprintf begin fun reason -> *)
(*         Update.write *)
(*           ~nb_msgs:1 *)
(*           ~msg_number:1 *)
(*           ~reason:Cancel_replace_rejected *)
(*           ~cli_ord_id:m.Replace.cli_ord_id *)
(*           ~srv_ord_id:m.srv_ord_id *)
(*           ?ord_type:m.Replace.ord_type *)
(*           ~p1:m.Replace.p1 *)
(*           ~p2:m.Replace.p2 *)
(*           ~order_qty:m.Replace.qty *)
(*           ?tif:m.Replace.tif *)
(*           ~good_till_ts:m.Replace.good_till_ts *)
(*           ~info_text:reason *)
(*           order_update_cs; *)
(*         Writer.write_cstruct w order_update_cs; *)
(*         Log.debug log_dtc "-> [%s] CancelReplaceRej: %s" addr_str reason *)
(*       end k *)
(*     in *)
(*     let m = Replace.read msg_cs in *)
(*     let { addr_str; key; secret; order } = Option.value_exn client in *)
(*     Log.debug log_dtc "<- [%s] %s" addr_str (Replace.show m); *)
(*     let cancel_order_exn order = *)
(*       let orderID = RespObj.string_exn order "orderID" in *)
(*       let ord_type = RespObj.string_exn order "ordType" in *)
(*       let ord_type = ord_type_of_string ord_type in *)
(*       let uri = Uri.with_path !base_uri "/api/v1/order" in *)
(*       let p1 = if m.p1_set then Some m.p1 else None in *)
(*       let p2 = if m.p2_set then Some m.p2 else None in *)
(*       let body = ([Some ("orderID", `String orderID); *)
(*                    if m.Replace.qty <> 0. then Some ("orderQty", `Float m.Replace.qty) else None; *)
(*                   ] |> List.filter_opt) *)
(*                  @ price_fields_of_dtc ord_type ?p1 ?p2 *)
(*       in *)
(*       let body_str = Yojson.Safe.to_string @@ `Assoc body in *)
(*       let body = Body.of_string body_str in *)
(*       Log.debug log_bitmex "-> %s" body_str; *)
(*       Rest.call ~name:"cancel" ~f:begin fun uri -> *)
(*         Client.put *)
(*           ~chunked:false ~body *)
(*           ~headers:(Rest.mk_headers ~key ~secret ~data:body_str `PUT uri) *)
(*             uri *)
(*       end uri >>| function *)
(*       | Ok body -> *)
(*         Log.debug log_bitmex "<- %s" @@ Yojson.Safe.to_string body *)
(*       | Error err -> *)
(*         let err_str = Error.to_string_hum err in *)
(*         reject m "%s" err_str; *)
(*         Log.error log_bitmex "%s" err_str *)
(*     in *)
(*     if Option.is_some m.Replace.ord_type then *)
(*       reject m "Modification of order type is not supported by BitMEX" *)
(*     else if Option.is_some m.Replace.tif then *)
(*       reject m "Modification of time in force is not supported by BitMEX" *)
(*     else *)
(*       let hex_srv_ord_id = uuid_of_b64 m.srv_ord_id in *)
(*       let on_exn _ = *)
(*         reject m "internal error when trying to cancel cli=%s srv=%s" m.cli_ord_id hex_srv_ord_id; *)
(*         Deferred.unit *)
(*       in *)
(*       begin *)
(*         match Uuid.Table.find order @@ Uuid.of_string hex_srv_ord_id with *)
(*         | None -> reject m "internal error: %s not found in db" m.srv_ord_id *)
(*         | Some o -> don't_wait_for @@ eat_exn ~on_exn ~log:log_bitmex (fun () -> cancel_order_exn o) *)
(*       end *)

(*   | Some CancelOrder -> *)
(*     let open Trading.Order in *)
(*     let reject m k = *)
(*       let order_update_cs = Cstruct.of_bigarray ~off:0 *)
(*           ~len:Update.sizeof_cs scratchbuf *)
(*       in *)
(*       Printf.ksprintf begin fun reason -> *)
(*           Update.write *)
(*             ~nb_msgs:1 *)
(*             ~msg_number:1 *)
(*             ~reason:Cancel_rejected *)
(*             ~cli_ord_id:m.Cancel.cli_ord_id *)
(*             ~srv_ord_id:m.srv_ord_id *)
(*             ~info_text:reason *)
(*             order_update_cs; *)
(*           Writer.write_cstruct w order_update_cs; *)
(*           Log.debug log_dtc "-> [%s] CancelOrderRej: %s" addr_str reason *)
(*       end k *)
(*     in *)
(*     let m = Cancel.read msg_cs in *)
(*     let hex_srv_ord_id = uuid_of_b64 m.srv_ord_id in *)
(*     let { addr_str; key; secret; parents; } = Option.value_exn client in *)
(*     Log.debug log_dtc "<- [%s] Cancelorder cli=%s srv=%s" addr_str m.cli_ord_id hex_srv_ord_id; *)
(*     let cancel_order_exn = function *)
(*       | None -> begin *)
(*         let uri = Uri.with_path !base_uri "/api/v1/order" in *)
(*         let body_str = `Assoc ["orderID", `String hex_srv_ord_id] |> Yojson.Safe.to_string in *)
(*         let body = Body.of_string body_str in *)
(*         Client.delete *)
(*           ~chunked:false ~body *)
(*           ~headers:(Rest.mk_headers ~key ~secret ~data:body_str `DELETE uri) *)
(*           uri >>= function (resp, body) -> *)
(*           Body.to_string body >>| fun body_str -> *)
(*           Log.debug log_bitmex "<- %s" body_str *)
(*         end *)
(*       | Some linkId -> begin *)
(*         let uri = Uri.with_path !base_uri "/api/v1/order/all" in *)
(*         let body_str = `Assoc ["filter", `Assoc ["clOrdLinkID", `String linkId]] |> Yojson.Safe.to_string in *)
(*         let body = Body.of_string body_str in *)
(*         Client.delete *)
(*           ~chunked:false ~body *)
(*           ~headers:(Rest.mk_headers ~key ~secret ~data:body_str `DELETE uri) *)
(*           uri >>= function (resp, body) -> *)
(*           Body.to_string body >>| fun body_str -> *)
(*           Log.debug log_bitmex "<- %s" body_str *)
(*         end *)
(*     in *)
(*     let on_exn _ = *)
(*       reject m "exception raised while trying to cancel cli=%s srv=%s" m.cli_ord_id m.srv_ord_id; *)
(*       Deferred.unit *)
(*     in *)
(*     let parent = String.Table.find parents m.cli_ord_id in *)
(*     don't_wait_for @@ eat_exn ~on_exn ~log:log_bitmex *)
(*       (fun () -> cancel_order_exn parent) *)

(*   | Some _ *)
(*   | None -> *)
(*     let buf = Buffer.create 128 in *)
(*     Cstruct.hexdump_to_buffer buf msg_cs; *)
(*     Log.error log_dtc "%s" @@ Buffer.contents buf *)

(* let dtcserver ~server ~port = *)
(*   let server_fun addr r w = *)
(*     (\* So that process does not allocate all the time. *\) *)
(*     let scratchbuf = Bigstring.create 1024 in *)
(*     let rec handle_chunk w consumed buf ~pos ~len = *)
(*       if len < 2 then return @@ `Consumed (consumed, `Need_unknown) *)
(*       else *)
(*         let msglen = Bigstring.unsafe_get_int16_le buf ~pos in *)
(*         if len < msglen then return @@ `Consumed (consumed, `Need msglen) *)
(*         else begin *)
(*           let msg_cs = Cstruct.of_bigarray buf ~off:pos ~len:msglen in *)
(*           process addr w msg_cs scratchbuf; *)
(*           handle_chunk w (consumed + msglen) buf (pos + msglen) (len - msglen) *)
(*         end *)
(*     in *)
(*     let on_client_io_error exn = *)
(*       Log.error log_dtc "on_client_io_error (%s): %s" *)
(*         Socket.Address.(to_string addr) Exn.(to_string exn) *)
(*     in *)
(*     let cleanup r w = *)
(*       Log.info log_dtc "client %s disconnected" Socket.Address.(to_string addr); *)
(*       let { ws_uuid } = InetAddr.Table.find_exn clients addr in *)
(*       let addr_str = InetAddr.sexp_of_t addr |> Sexp.to_string in *)
(*       InetAddr.Table.remove clients addr; *)
(*       Deferred.all_unit [Pipe.write client_ws_w @@ Unsubscribe (ws_uuid, addr_str); Writer.close w; Reader.close r] *)
(*     in *)
(*     Deferred.ignore @@ Monitor.protect *)
(*       ~name:"server_fun" *)
(*       ~finally:(fun () -> cleanup r w) *)
(*       (fun () -> *)
(*          Monitor.detach_and_iter_errors Writer.(monitor w) ~f:on_client_io_error; *)
(*          Reader.(read_one_chunk_at_a_time r ~handle_chunk:(handle_chunk w 0)) *)
(*       ) *)
(*   in *)
(*   let on_handler_error_f addr exn = *)
(*     Log.error log_dtc "on_handler_error (%s): %s" *)
(*       Socket.Address.(to_string addr) Exn.(to_string exn) *)
(*   in *)
(*   Conduit_async.serve *)
(*     ~on_handler_error:(`Call on_handler_error_f) *)
(*     server (Tcp.on_port port) server_fun *)

let dtcserver ~server ~port =
  let server_fun addr r w =
    let addr = Socket.Address.Inet.to_string addr in
    (* So that process does not allocate all the time. *)
    let rec handle_chunk consumed buf ~pos ~len =
      if len < 2 then return @@ `Consumed (consumed, `Need_unknown)
      else
        let msglen = Bigstring.unsafe_get_int16_le buf ~pos in
        Log.debug log_dtc "handle_chunk: pos=%d len=%d, msglen=%d" pos len msglen;
        if len < msglen then return @@ `Consumed (consumed, `Need msglen)
        else begin
          let msgtype_int = Bigstring.unsafe_get_int16_le buf ~pos:(pos+2) in
          let msgtype : DTC.dtcmessage_type =
            DTC.parse_dtcmessage_type (Piqirun.Varint msgtype_int) in
          let msg_str = Bigstring.To_string.subo buf ~pos:(pos+4) ~len:(msglen-4) in
          let msg = Piqirun.init_from_string msg_str in
          begin match msgtype with
            | `encoding_request ->
              begin match (Dtc_pb.Encoding.read (Bigstring.To_string.subo buf ~pos ~len:16)) with
                | None -> Log.error log_dtc "Invalid encoding request received"
                | Some msg -> encoding_request addr w msg
              end
            | `logon_request -> logon_request addr w msg
            | `heartbeat -> heartbeat addr w msg
            | `security_definition_for_symbol_request -> security_definition_request addr w msg
            | `market_data_request -> market_data_request addr w msg
            | `market_depth_request -> market_depth_request addr w msg
            | `open_orders_request -> open_orders_request addr w msg
            | `current_positions_request -> current_positions_request addr w msg
            | `historical_order_fills_request -> historical_order_fills addr w msg
            | `trade_accounts_request -> trade_account_request addr w msg
            | `account_balance_request -> account_balance_request addr w msg
            | `submit_new_single_order -> submit_new_single_order addr w msg
            | `cancel_order -> cancel_order addr w msg
            | `cancel_replace_order -> cancel_replace_order addr w msg
            | #DTC.dtcmessage_type ->
              Log.error log_dtc "Unknown msg type %d" msgtype_int
          end ;
          handle_chunk (consumed + msglen) buf (pos + msglen) (len - msglen)
        end
    in
    let on_connection_io_error exn =
      String.Table.remove Connection.active addr ;
      Log.error log_dtc "on_connection_io_error (%s): %s" addr Exn.(to_string exn)
    in
    let cleanup () =
      Log.info log_dtc "client %s disconnected" addr ;
      String.Table.remove Connection.active addr ;
      Deferred.all_unit [Writer.close w; Reader.close r]
    in
    Deferred.ignore @@ Monitor.protect ~finally:cleanup begin fun () ->
      Monitor.detach_and_iter_errors Writer.(monitor w) ~f:on_connection_io_error;
      Reader.(read_one_chunk_at_a_time r ~handle_chunk:(handle_chunk 0))
    end
  in
  let on_handler_error_f addr exn =
    Log.error log_dtc "on_handler_error (%s): %s"
      Socket.Address.(to_string addr) Exn.(to_string exn)
  in
  Conduit_async.serve
    ~on_handler_error:(`Call on_handler_error_f)
    server (Tcp.on_port port) server_fun

let send_instr_update_msgs w buf_cs instr symbol_id =
  let open RespObj in
  Option.iter (int64 instr "volume")
    ~f:(fun v ->
        let open MarketData.UpdateSession in
        let buf_cs = Cstruct.of_bigarray buf_cs ~len:sizeof_cs in
        write ~kind:`Volume ~symbol_id ~data:Int64.(to_float v) buf_cs;
        Writer.write_cstruct w buf_cs
      );
  Option.iter (float instr "lowPrice")
    ~f:(fun p ->
        let open MarketData.UpdateSession in
        let buf_cs = Cstruct.of_bigarray buf_cs ~len:sizeof_cs in
        write ~kind:`Low ~symbol_id ~data:p buf_cs;
        Writer.write_cstruct w buf_cs
      );
  Option.iter (float instr "highPrice")
    ~f:(fun p ->
        let open MarketData.UpdateSession in
        let buf_cs = Cstruct.of_bigarray buf_cs ~len:sizeof_cs in
        write ~kind:`High ~symbol_id ~data:p buf_cs;
        Writer.write_cstruct w buf_cs
      );
  Option.iter (int64 instr "openInterest")
    ~f:(fun p ->
        let open MarketData.UpdateOpenInterest in
        let buf_cs = Cstruct.of_bigarray buf_cs ~len:sizeof_cs in
        write ~symbol_id ~open_interest:Int64.(to_int32_exn p) buf_cs;
        Writer.write_cstruct w buf_cs
      );
  Option.iter (float instr "prevClosePrice")
    ~f:(fun p ->
        let open MarketData.UpdateSession in
        let buf_cs = Cstruct.of_bigarray buf_cs ~len:sizeof_cs in
        write ~kind:`Open ~symbol_id ~data:p buf_cs;
        Writer.write_cstruct w buf_cs
      )

let delete_instr instrObj =
  let instrObj = RespObj.of_json instrObj in
  Log.info log_bitmex "deleted instrument %s" RespObj.(string_exn instrObj "symbol")

let insert_instr buf_cs instrObj =
  let buf_cs = Cstruct.of_bigarray buf_cs ~len:SecurityDefinition.Response.sizeof_cs in
  let instrObj = RespObj.of_json instrObj in
  let key = RespObj.string_exn instrObj "symbol" in
  let secdef = Instrument.to_secdef ~testnet:!use_testnet instrObj in
  let instr = create_instr ~instrObj ~secdef () in
  let books = Int.Table.{ bids = create () ; asks = create () } in
  String.Table.set instruments key instr;
  String.Table.set orderbooks ~key ~data:books;
  Log.info log_bitmex "inserted instrument %s" key;
  (* Send secdef response to clients. *)
  let on_client { addr; addr_str; w; subs; _ } =
    SecurityDefinition.Response.to_cstruct buf_cs { secdef with final = true };
    Writer.write_cstruct w buf_cs;
  in
  InetAddr.Table.iter clients ~f:on_client

let update_instr buf_cs instrObj =
  let instrObj = RespObj.of_json instrObj in
  let symbol = RespObj.string_exn instrObj "symbol" in
  match String.Table.find instruments symbol with
  | None ->
    Log.error log_bitmex "update_instr: unable to find %s" symbol;
  | Some instr ->
    instr.instrObj <- RespObj.merge instr.instrObj instrObj;
    Log.debug log_bitmex "updated instrument %s" symbol;
    (* Send messages to subscribed clients according to the type of update. *)
    let on_client { addr; addr_str; w; subs; _ } =
      let on_symbol_id symbol_id =
        send_instr_update_msgs w buf_cs instrObj symbol_id;
        Log.debug log_dtc "-> [%s] instrument %s" addr_str symbol
      in
      Option.iter String.Table.(find subs symbol) ~f:on_symbol_id
    in
    InetAddr.Table.iter clients ~f:on_client

let update_depths update_cs action { OrderBook.L2.symbol; id; side; size; price } =
  (* find_exn cannot raise here *)
  let { bids; asks } =  String.Table.find_exn orderbooks symbol in
  let side = side_of_bmex side in
  let table = match side with
  | Some `Buy -> bids
  | Some `Sell -> asks
  | None -> failwith "update_depth: empty side" in
  let price = match price with
    | Some p -> Some p
    | None -> begin match Int.Table.find table id with
        | Some { price } -> Some price
        | None -> None
      end
  in
  let size = match size with
    | Some s -> Some s
    | None -> begin match Int.Table.find table id with
        | Some { size } -> Some size
        | None -> None
      end
  in
  match price, size with
  | Some price, Some size ->
    begin match action with
      | OB.Partial | Insert | Update -> Int.Table.set table id { size ; price }
      | Delete -> Int.Table.remove table id
    end;
    let on_client { addr; addr_str; w; subs; subs_depth; _} =
      let on_symbol_id symbol_id =
        (* Log.debug log_dtc "-> [%s] depth %s %s %s %f %d" addr_str (OB.sexp_of_action action |> Sexp.to_string) symbol side price size; *)
        MarketDepth.Update.write
          ~op:(match action with Partial | Insert | Update -> `Insert_update | Delete -> `Delete)
          ~p:price
          ~v:(Float.of_int size)
          ~symbol_id
          ?side
          update_cs;
        Writer.write_cstruct w update_cs
      in
      Option.iter String.Table.(find subs_depth symbol) ~f:on_symbol_id
    in
    InetAddr.Table.iter clients ~f:on_client
  | _ ->
    Log.info log_bitmex "update_depth: received update before snapshot, ignoring"

let update_quote update_cs q =
  let old_q = String.Table.find_or_add quotes q.Quote.symbol ~default:(fun () -> q) in
  let merged_q = Quote.merge old_q q in
  let bidPrice = Option.value ~default:Float.max_finite_value merged_q.bidPrice in
  let bidSize = Option.value ~default:0 merged_q.bidSize in
  let askPrice = Option.value ~default:Float.max_finite_value merged_q.askPrice in
  let askSize = Option.value ~default:0 merged_q.askSize in
  String.Table.set quotes ~key:q.symbol ~data:merged_q;
  Log.debug log_bitmex "set quote %s" q.symbol;
  let on_client { addr; addr_str; w; subs; subs_depth; _} =
    let on_symbol_id symbol_id =
      Log.debug log_dtc "-> [%s] bidask %s %f %d %f %d"
        addr_str q.symbol bidPrice bidSize askPrice askSize;
      MarketData.UpdateBidAsk.write
        ~symbol_id
        ~bid:bidPrice
        ~bid_qty:Float.(of_int bidSize)
        ~ask:askPrice
        ~ask_qty:Float.(of_int askSize)
        ~ts:(Time_ns.(of_string merged_q.timestamp))
        update_cs;
      Writer.write_cstruct w update_cs
    in
    match String.Table.(find subs q.symbol, find subs_depth q.symbol) with
    | Some id, None -> on_symbol_id id
    | _ -> ()
  in
  InetAddr.Table.iter clients ~f:on_client

let update_trade cs { Trade.symbol; timestamp; price; size; side; tickDirection; trdMatchID;
                      grossValue; homeNotional; foreignNotional; id } =
  let open Trade in
  Log.debug log_bitmex "trade %s %s %f %d" symbol side price size;
  match side_of_bmex side, String.Table.find instruments symbol with
  | None, _ -> ()
  | _, None ->
    Log.error log_bitmex "update_trade: found no instrument for %s" symbol
  | Some s, Some instr ->
    instr.last_trade_price <- price;
    instr.last_trade_size <- size;
    instr.last_trade_ts <- Time_ns.of_string timestamp;
    (* Send trade updates to subscribers. *)
    let on_client { addr; addr_str; w; subs; _} =
      let on_symbol_id symbol_id =
        MarketData.UpdateTrade.write
          ~symbol_id
          ~side:s
          ~p:price
          ~v:(Int.to_float size)
          ~ts:(Time_ns.of_string timestamp)
          cs;
        Writer.write_cstruct w cs;
        Log.debug log_dtc "-> [%s] trade %s %s %f %d" addr_str symbol side price size
      in
      Option.iter String.Table.(find subs symbol) ~f:on_symbol_id
    in
    InetAddr.Table.iter clients ~f:on_client

let bitmex_ws ~instrs_initialized ~orderbook_initialized ~quotes_initialized =
  let open Ws in
  let buf_cs = Bigstring.create 4096 in
  let trade_update_cs = Cstruct.of_bigarray ~len:MarketData.UpdateTrade.sizeof_cs buf_cs in
  let depth_update_cs = Cstruct.of_bigarray ~len:MarketDepth.Update.sizeof_cs buf_cs in
  let bidask_update_cs = Cstruct.of_bigarray ~len:MarketData.UpdateBidAsk.sizeof_cs buf_cs in
  let on_update update =
    let action = update_action_of_string update.action in
    match action, update.table, update.data with
    | Update, "instrument", instrs ->
      if Ivar.is_full instrs_initialized then List.iter instrs ~f:(update_instr buf_cs)
    | Delete, "instrument", instrs ->
      if Ivar.is_full instrs_initialized then List.iter instrs ~f:delete_instr
    | _, "instrument", instrs ->
      List.iter instrs ~f:(insert_instr buf_cs);
      Ivar.fill_if_empty instrs_initialized ()
    | _, "orderBookL2", depths ->
      let filter_f json = match OrderBook.L2.of_yojson json with
        | Ok u -> Some u
        | Error reason ->
          Log.error log_bitmex "%s: %s (%s)"
            reason Yojson.Safe.(to_string json) update.action;
          None
      in
      let depths = List.filter_map depths ~f:filter_f in
      let depths = List.group depths
          ~break:(fun { symbol } { symbol=symbol' } -> symbol <> symbol')
     in
      don't_wait_for begin
        Ivar.read instrs_initialized >>| fun () ->
        List.iter depths ~f:begin function
          | [] -> ()
          | h::t as ds ->
            Log.debug log_bitmex "depth update %s" h.symbol;
            List.iter ds ~f:(update_depths depth_update_cs action)
        end;
        Ivar.fill_if_empty orderbook_initialized ()
      end
    | _, "trade", trades ->
      let open Trade in
      let iter_f t = match Trade.of_yojson t with
        | Error msg ->
          Log.error log_bitmex "%s" msg
        | Ok t -> update_trade trade_update_cs t
      in
      don't_wait_for begin
        Ivar.read instrs_initialized >>| fun () ->
        List.iter trades ~f:iter_f
      end
    | _, "quote", quotes ->
      let filter_f json =
        match Quote.of_yojson json with
        | Ok q -> Some q
        | Error reason ->
          Log.error log_bitmex "%s: %s (%s)"
            reason Yojson.Safe.(to_string json) update.action;
          None
      in
      let quotes = List.filter_map quotes ~f:filter_f in
      List.iter quotes ~f:(update_quote bidask_update_cs);
      Ivar.fill_if_empty quotes_initialized ()
    | _, table, json ->
      Log.error log_bitmex "Unknown/ignored BitMEX DB table %s or wrong json %s"
        table Yojson.Safe.(to_string @@ `List json)
  in
  let to_ws, to_ws_w = Pipe.create () in
  let bitmex_topics = ["instrument"; "quote"; "orderBookL2"; "trade"] in
  let clients_topics = ["order"; "execution"; "position"; "margin"] in
  let subscribe_topics ~id ~topic ~topics =
    let payload =
      create_request
        ~op:"subscribe"
        ~args:(`List (List.map topics ~f:(fun t -> `String t)))
        ()
    in
    MD.message id topic (request_to_yojson payload)
  in
  let subscribe_client ({ addr; key; secret } as c) =
    let id = Uuid.create () in
    let id_str = Uuid.to_string id in
    let topic = InetAddr.sexp_of_t addr |> Sexp.to_string_mach in
    c.ws_uuid <- id;
    Pipe.write to_ws_w @@ MD.to_yojson @@ MD.subscribe ~id:id_str ~topic
  in
  let my_uuid = Uuid.create () in
  let my_uuid_str = Uuid.to_string my_uuid in
  let connected = Mvar.create () in
  let rec resubscribe () =
    Mvar.take (Mvar.read_only connected) >>= fun () ->
    Pipe.write to_ws_w @@ MD.to_yojson @@ MD.subscribe ~id:my_uuid_str ~topic:my_topic >>= fun () ->
    Deferred.List.iter (InetAddr.Table.to_alist clients) ~how:`Sequential ~f:(fun (_addr, c) -> subscribe_client c) >>=
    resubscribe
  in
  don't_wait_for @@ resubscribe ();
  let ws = open_connection ~connected ~to_ws ~log:log_ws ~testnet:!use_testnet ~md:true ~topics:[] () in
  don't_wait_for begin
    Pipe.iter client_ws_r ~f:begin function
      | Subscribe c -> subscribe_client c
      | Unsubscribe (id, topic) -> Pipe.write to_ws_w @@ MD.to_yojson @@ MD.unsubscribe ~id:(Uuid.to_string id) ~topic
    end
  end;
  let on_ws_msg msg = match MD.of_yojson msg with
    | Error msg -> Log.error log_bitmex "%s" msg
    | Ok { MD.typ; id; topic; payload = Some payload } -> begin
        match Ws.msg_of_yojson payload, topic = my_topic with

        (* Server *)
        | Welcome, true ->
          Pipe.write_without_pushback to_ws_w @@ MD.to_yojson @@ subscribe_topics my_uuid_str my_topic bitmex_topics
        | Error err, true ->
          Log.error log_bitmex "BitMEX: error %s" @@ show_error err
        | Ok { request = { op = "subscribe" }; subscribe; success }, true ->
          Log.info log_bitmex "BitMEX: subscribed to %s: %b" subscribe success
        | Ok { success }, true ->
          Log.error log_bitmex "BitMEX: unexpected response %s" (Yojson.Safe.to_string payload)
        | Update update, true -> on_update update

        (* Clients *)
        | Welcome, false ->
          let addr = Fn.compose InetAddr.Blocking_sexp.t_of_sexp Sexp.of_string topic in
          Option.iter (InetAddr.Table.find clients addr) ~f:begin fun { key; secret } ->
            Pipe.write_without_pushback to_ws_w @@ MD.to_yojson @@ MD.auth ~id ~topic ~key ~secret
          end
        | Error err, false ->
          Log.error log_bitmex "%s: error %s" topic @@ show_error err
        | Ok { request = { op = "authKey"}; success}, false ->
          Pipe.write_without_pushback to_ws_w @@ MD.to_yojson @@ subscribe_topics ~id ~topic ~topics:clients_topics
        | Ok { request = { op = "subscribe"}; subscribe; success}, false ->
          Log.info log_bitmex "%s: subscribed to %s: %b" topic subscribe success
        | Ok { success }, false ->
          Log.error log_bitmex "%s: unexpected response %s" topic (Yojson.Safe.to_string payload)
        | Update update, false ->
          let addr = Sexp.of_string topic |> InetAddr.Blocking_sexp.t_of_sexp in
          match InetAddr.Table.find clients addr with
          | None -> if typ <> Unsubscribe then Pipe.write_without_pushback client_ws_w (Unsubscribe (Uuid.of_string id, topic))
          | Some { ws_w } -> Pipe.write_without_pushback ws_w update
      end
    | _ -> ()
  in
  Monitor.handle_errors
    (fun () -> Pipe.iter_without_pushback ~continue_on_error:true ws ~f:on_ws_msg)
    (fun exn -> Log.error log_bitmex "%s" @@ Exn.to_string exn)

let main tls testnet port daemon sockfile pidfile logfile loglevel ll_ws ll_dtc ll_bitmex crt_path key_path =
  let sockfile = if testnet then add_suffix sockfile "_testnet" else sockfile in
  let pidfile = if testnet then add_suffix pidfile "_testnet" else pidfile in
  let logfile = if testnet then add_suffix logfile "_testnet" else logfile in
  let run server =
    let instrs_initialized = Ivar.create () in
    let orderbook_initialized = Ivar.create () in
    let quotes_initialized = Ivar.create () in
    Log.info log_bitmex "WS feed starting";
    let bitmex_th = bitmex_ws ~instrs_initialized ~orderbook_initialized ~quotes_initialized in
    Deferred.List.iter ~how:`Parallel ~f:Ivar.read
      [instrs_initialized; orderbook_initialized; quotes_initialized] >>= fun () ->
    dtcserver ~server ~port >>= fun dtc_server ->
    Log.info log_dtc "DTC server started";
    Deferred.all_unit [Tcp.Server.close_finished dtc_server; bitmex_th]
  in

  (* start initilization code *)
  if testnet then begin
    use_testnet := testnet;
    base_uri := Uri.of_string "https://testnet.bitmex.com";
    my_exchange := "BMEXT"
  end;
  bitmex_historical := sockfile;

  Log.set_level log_dtc @@ loglevel_of_int @@ max loglevel ll_dtc;
  Log.set_level log_bitmex @@ loglevel_of_int @@ max loglevel ll_bitmex;
  Log.set_level log_ws @@ loglevel_of_int ll_ws;

  if daemon then Daemon.daemonize ~cd:"." ();
  don't_wait_for begin
    Lock_file.create_exn pidfile >>= fun () ->
    Writer.open_file ~append:true logfile >>= fun log_writer ->
    Log.(set_output log_dtc Output.[stderr (); writer `Text log_writer]);
    Log.(set_output log_bitmex Output.[stderr (); writer `Text log_writer]);
    Log.(set_output log_ws Output.[stderr (); writer `Text log_writer]);
    conduit_server ~tls ~crt_path ~key_path >>= fun server ->
    loop_log_errors ~log:log_dtc (fun () -> run server)
  end;
  never_returns @@ Scheduler.go ()

let command =
  let spec =
    let open Command.Spec in
    empty
    +> flag "-tls" no_arg ~doc:" Use TLS"
    +> flag "-testnet" no_arg ~doc:" Use testnet"
    +> flag "-port" (optional_with_default 5567 int) ~doc:"int TCP port to use (5567)"
    +> flag "-daemon" no_arg ~doc:" Run as a daemon"
    +> flag "-sockfile" (optional_with_default "run/bitmex.sock" string) ~doc:"filename UNIX sock to use (run/bitmex.sock)"
    +> flag "-pidfile" (optional_with_default "run/bitmex.pid" string) ~doc:"filename Path of the pid file (run/bitmex.pid)"
    +> flag "-logfile" (optional_with_default "log/bitmex.log" string) ~doc:"filename Path of the log file (log/bitmex.log)"
    +> flag "-loglevel" (optional_with_default 1 int) ~doc:"1-3 global loglevel"
    +> flag "-loglevel-ws" (optional_with_default 1 int) ~doc:"1-3 loglevel for the websocket library"
    +> flag "-loglevel-dtc" (optional_with_default 1 int) ~doc:"1-3 loglevel for DTC"
    +> flag "-loglevel-bitmex" (optional_with_default 1 int) ~doc:"1-3 loglevel for BitMEX"
    +> flag "-crt-file" (optional_with_default "ssl/bitsouk.com.crt" string) ~doc:"filename crt file to use (TLS)"
    +> flag "-key-file" (optional_with_default "ssl/bitsouk.com.key" string) ~doc:"filename key file to use (TLS)"
  in
  Command.basic ~summary:"BitMEX bridge" spec main

let () = Command.run command
