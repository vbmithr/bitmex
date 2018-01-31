(* DTC to BitMEX simple bridge *)

open Core
open Async
open Cohttp_async

open Bs_devkit
open Bmex
open Bitmex_types

module WS = Bmex_ws
module REST = Bmex_rest

open Pbrt
open Dtc_pb.Dtcprotocol_types
open Dtc_pb.Dtcprotocol_pb

let use_testnet = ref false
let base_uri = ref @@ Uri.of_string "https://www.bitmex.com"
let my_exchange = ref "BMEX"
let my_topic = "bitsouk"

let log_bitmex = Log.create ~level:`Error ~on_error:`Raise ~output:Log.Output.[stderr ()]
let log_dtc = Log.create ~level:`Error ~on_error:`Raise ~output:Log.Output.[stderr ()]
let log_ws = Log.create ~level:`Error ~on_error:`Raise ~output:Log.Output.[stderr ()]

let ws_feed_connected : unit Condition.t = Condition.create ()

module Connection = struct
  type t = {
    addr: string;
    w: Writer.t;
    ws_r: WS.Response.Update.t Pipe.Reader.t;
    ws_w: WS.Response.Update.t Pipe.Writer.t;
    mutable ws_uuid: Uuid.t;
    key: string;
    secret: string;
    mutable account: int;
    position: Position.t String.Table.t; (* indexed by symbol *)
    margin: Margin.t String.Table.t; (* indexed by currency *)
    order: Order.t Uuid.Table.t; (* indexed by orderID *)
    subs: int32 String.Table.t;
    rev_subs: string Int32.Table.t;
    subs_depth: int32 String.Table.t;
    rev_subs_depth: string Int32.Table.t;
    mutable current_parent: submit_new_single_order option;
    parents: string String.Table.t;
    stop_exec_inst: ExecInst.t ;
  }

  let create ~addr ~w ~key ~secret ~stop_exec_inst =
    let ws_r, ws_w = Pipe.create () in
    {
      addr ; w ; ws_r ; ws_w ; key ; secret ; account = 0 ;
      position = String.Table.create () ;
      margin = String.Table.create () ;
      order = Uuid.Table.create () ;
      subs = String.Table.create () ;
      rev_subs = Int32.Table.create () ;
      subs_depth = String.Table.create () ;
      rev_subs_depth = Int32.Table.create () ;
      parents = String.Table.create () ;
      stop_exec_inst ;

      current_parent = None ;
      ws_uuid = Uuid.create () ;
    }

  let active : t String.Table.t = String.Table.create ()
  let to_alist () = String.Table.to_alist active

  let find = String.Table.find active
  let find_exn = String.Table.find_exn active
  let set = String.Table.set active
  let remove = String.Table.remove active

  let iter = String.Table.iter active

  let set_parent_order conn order =
    conn.current_parent <- Some order;
    Option.iter order.client_order_id
      ~f:(fun id -> String.Table.set conn.parents id id)
end

module Books = struct
  type entry = {
    price: float;
    size: int;
  }

  let mapify_ob =
    let fold_f ~key:_ ~data:{ price; size } map =
      Float.Map.update map price ~f:begin function
        | Some size' -> size + size'
        | None -> size
      end
    in
    Int.Table.fold ~init:Float.Map.empty ~f:fold_f

  let bids : entry Int.Table.t String.Table.t = String.Table.create ()
  let asks : entry Int.Table.t String.Table.t = String.Table.create ()
  let initialized = Ivar.create ()

  let get_bids symbol =
    Option.value_map (String.Table.find bids symbol)
      ~default:Float.Map.empty ~f:mapify_ob

  let get_asks symbol =
    Option.value_map (String.Table.find asks symbol)
      ~default:Float.Map.empty ~f:mapify_ob

  let update action { OrderBookL2.symbol; id; side; size; price } =
    (* find_exn cannot raise here *)
    let bids = String.Table.find_or_add bids symbol ~default:Int.Table.create in
    let asks = String.Table.find_or_add asks symbol ~default:Int.Table.create in
    let table =
      match Side.of_string side with
      | `buy -> bids
      | `sell -> asks
      | `buy_sell_unset -> failwith "update_depth: empty side" in
    let price =
      match price with
      | Some p -> Some p
      | None -> begin match Int.Table.find table id with
          | Some { price } -> Some price
          | None -> None
        end in
    let size =
      match size with
      | Some s -> Some s
      | None -> begin match Int.Table.find table id with
          | Some { size } -> Some size
          | None -> None
        end in
    match price, size with
    | None, None | Some _, None | None, Some _ ->
        let size = Option.value ~default:0 size in
        let price = Option.value ~default:0. price in
        Log.info log_bitmex
          "update_depth: spurious %s %d %s %d %f" symbol id side size price
    | Some price, Some size ->
      begin match action with
        | Bmex_ws.Response.Update.Partial
        | Insert
        | Update -> Int.Table.set table id { size ; price }
        | Delete -> Int.Table.remove table id
      end;
      let update_type =
        match action with
        | Partial
        | Insert
        | Update -> `market_depth_insert_update_level
        | Delete -> `market_depth_delete_level in
      let side =
        match Side.of_string side with
        | `buy -> Some At_bid
        | `sell -> Some At_ask
        | `buy_sell_unset -> None
      in
      let u = default_market_depth_update_level
          ~side ~price ~quantity:(Float.of_int size) ~update_type () in
      (* u.side <- side ;
       * u.price <- Some price ;
       * u.quantity <- Some (Float.of_int size) ;
       * u.update_type <- Some update_type ; *)
      let on_connection { Connection.addr; w; subs; subs_depth } =
        let on_symbol_id symbol_id =
          u.symbol_id <- Some symbol_id ;
          (* Log.debug log_dtc "-> [%s] depth %s %s %s %f %d" addr_str (OB.sexp_of_action action |> Sexp.to_string) symbol side price size; *)
          write_message w `market_depth_update_level DTC.gen_market_depth_update_level u
        in
        Option.iter String.Table.(find subs_depth symbol) ~f:on_symbol_id
      in
      Connection.iter ~f:on_connection
end

module Instr = struct
  let is_index symbol = symbol.[0] = '.'

  let to_secdef ~testnet (t : Instrument.t) =
    let index = is_index t.symbol in
    let exchange = (Option.value ~default:"" t.reference)
      ^ (if testnet && not index then "T" else "")
    in
    let tickSize = Option.value ~default:0. t.tickSize in
    let expiration_date = Option.map t.expiry ~f:(fun time ->
        Time_ns.(to_int_ns_since_epoch time |>
                 (fun t -> t / 1_000_000_000) |>
                 Int32.of_int_exn)) in
    let secdef = DTC.default_security_definition_response () in
    secdef.symbol <- Some t.symbol ;
    secdef.exchange <- Some exchange ;
    secdef.security_type <-
      Some (if index then `security_type_index else `security_type_future) ;
    secdef.min_price_increment <- Some tickSize ;
    secdef.currency_value_per_increment <- Some tickSize ;
    secdef.price_display_format <- Some (price_display_format_of_ticksize tickSize) ;
    secdef.has_market_depth_data <- Some (not index) ;
    secdef.underlying_symbol <- t.underlyingSymbol ;
    secdef.updates_bid_ask_only <- Some false ;
    secdef.security_expiration_date <- expiration_date ;
    secdef

  type t = {
    mutable instr: Instrument.t;
    secdef: DTC.Security_definition_response.t ;
    mutable last_trade_price: float;
    mutable last_trade_size: int;
    mutable last_trade_ts: Time_ns.t;
    mutable last_quote_ts: Time_ns.t;
  }

  let create
      ?(last_trade_price = 0.)
      ?(last_trade_size = 0)
      ?(last_trade_ts = Time_ns.epoch)
      ?(last_quote_ts = Time_ns.epoch)
      ~instr ~secdef () = {
    instr ; secdef ;
    last_trade_price ; last_trade_size ;
    last_trade_ts ; last_quote_ts
  }

  let active : t String.Table.t = String.Table.create ()
  let initialized = Ivar.create ()

  let mem = String.Table.mem active
  let find = String.Table.find active
  let find_exn = String.Table.find_exn active
  let set = String.Table.set active
  let remove = String.Table.remove active

  let iter = String.Table.iter active

  let delete t =
    remove t.Instrument.symbol ;
    Log.info log_bitmex "deleted instrument %s" t.symbol

  let insert t =
    let secdef = to_secdef ~testnet:!use_testnet t in
    let instr = create ~instr:t ~secdef () in
    set t.symbol instr;
    Log.info log_bitmex "inserted instrument %s" t.symbol;
    (* Send secdef response to connections. *)
    let on_connection { Connection.addr; w } =
      secdef.is_final_message <- Some true ;
      write_message w `security_definition_response
        DTC.gen_security_definition_response secdef
    in
    Connection.iter ~f:on_connection

  let send_instr_update_msgs w t symbol_id =
    Option.iter t.Instrument.volume ~f:begin fun volume ->
      let msg = DTC.default_market_data_update_session_volume () in
      msg.symbol_id <- Some symbol_id ;
      msg.volume <- Some Int.(to_float volume) ;
      write_message w `market_data_update_session_volume
        DTC.gen_market_data_update_session_volume msg
    end ;
    Option.iter t.lowPrice ~f:begin fun low ->
      let msg = DTC.default_market_data_update_session_low () in
      msg.symbol_id <- Some symbol_id ;
      msg.price <- Some low ;
      write_message w `market_data_update_session_low
        DTC.gen_market_data_update_session_low msg
    end ;
    Option.iter t.highPrice ~f:begin fun high ->
      let msg = DTC.default_market_data_update_session_high () in
      msg.symbol_id <- Some symbol_id ;
      msg.price <- Some high ;
      write_message w `market_data_update_session_high
        DTC.gen_market_data_update_session_high msg
    end ;
    Option.iter t.openInterest ~f:begin fun open_interest ->
      let msg = DTC.default_market_data_update_open_interest () in
      msg.symbol_id <- Some symbol_id ;
      msg.open_interest <- Some (Int.to_int32_exn open_interest) ;
      write_message w `market_data_update_open_interest
        DTC.gen_market_data_update_open_interest msg
    end ;
    Option.iter t.prevClosePrice ~f:begin fun prev_close ->
      let msg = DTC.default_market_data_update_session_open () in
      msg.symbol_id <- Some symbol_id ;
      msg.price <- Some prev_close ;
      write_message w `market_data_update_session_open
        DTC.gen_market_data_update_session_open msg
    end

  let update t =
    match find t.Instrument.symbol with
    | None ->
      Log.error log_bitmex "update_instr: unable to find %s" t.symbol;
    | Some instr ->
      instr.instr <- Instrument.merge instr.instr t;
      Log.debug log_bitmex "updated instrument %s" t.symbol;
      (* Send messages to subscribed clients according to the type of update. *)
      let on_connection { Connection.addr; w; subs } =
        let on_symbol_id symbol_id =
          send_instr_update_msgs w t symbol_id;
          Log.debug log_dtc "-> [%s] instrument %s" addr t.symbol
        in
        Option.iter String.Table.(find subs t.symbol) ~f:on_symbol_id
      in
      Connection.iter ~f:on_connection
end

module Quotes = struct
  let quotes : Quote.t String.Table.t = String.Table.create ()
  let initialized = Ivar.create ()

  let find = String.Table.find quotes
  let find_exn = String.Table.find_exn quotes

  let update ({ Quote.timestamp; symbol; bidPrice; bidSize; askPrice; askSize } as q) =
    let old_q = String.Table.find_or_add quotes symbol ~default:(fun () -> q) in
    let merged_q = Quote.merge old_q q in
    let bidPrice = Option.value ~default:Float.max_finite_value merged_q.bidPrice in
    let bidSize = Option.value ~default:0 merged_q.bidSize in
    let askPrice = Option.value ~default:Float.max_finite_value merged_q.askPrice in
    let askSize = Option.value ~default:0 merged_q.askSize in
    String.Table.set quotes ~key:q.symbol ~data:merged_q;
    Log.debug log_bitmex "set quote %s" q.symbol;
    let u = DTC.default_market_data_update_bid_ask () in
    u.bid_price <- Some bidPrice ;
    u.bid_quantity <- Some (Float.of_int bidSize) ;
    u.ask_price <- Some askPrice ;
    u.ask_quantity <- Some (Float.of_int askSize) ;
    u.date_time <- seconds_int32_of_ts merged_q.timestamp ;
    let on_connection { Connection.addr; w; subs; subs_depth } =
      let on_symbol_id symbol_id =
        u.symbol_id <- Some symbol_id ;
        Log.debug log_dtc "-> [%s] bidask %s %f %d %f %d"
          addr q.symbol bidPrice bidSize askPrice askSize ;
        write_message w `market_data_update_bid_ask
          DTC.gen_market_data_update_bid_ask u
      in
      match String.Table.(find subs q.symbol, find subs_depth q.symbol) with
      | Some id, None -> on_symbol_id id
      | _ -> ()
    in
    Connection.iter ~f:on_connection
end

let log_error ?(f=ignore) log err =
  match Error.to_exn err with
  | Failure msg ->
    f msg ;
    Log.error log "%s" msg
  | _ ->
    let err_msg = (Error.to_string_hum err) in
    f err_msg ;
    Log.error log "%s" err_msg

module TradeHistory = struct
  let buf = Bi_outbuf.create 4096
  let table : Execution.t Uuid.Map.t Int.Table.t = Int.Table.create ()

  let get ~account ~key ~secret =
    let filter = `Assoc ["execType", `String "Trade"] in
    REST.Execution.trade_history ~extract_exn:true ~count:500
      ~buf ~log:log_bitmex ~testnet:!use_testnet
      ~filter ~key ~secret () >>| function
    | Error err ->
      log_error log_bitmex err ;
      Error err
    | Ok (resp, execs) ->
      let trades =
        List.fold_left execs ~init:Uuid.Map.empty ~f:begin fun a e ->
          match e.orderID with
          | None -> a
          | Some orderID -> Uuid.Map.add a orderID e
        end in
      Int.Table.set table account trades ;
      Ok trades

  let filter_trades min_ts trades =
    Uuid.Map.filter trades ~f:begin fun t ->
      let ts =
        Option.value t.Execution.transactTime ~default:Time_ns.max_value in
      Time_ns.(ts > min_ts)
    end

  let get ?(min_ts=Time_ns.epoch) ~account ~key ~secret =
    match Int.Table.find table account with
    | Some trades ->
      Deferred.Or_error.return (filter_trades min_ts trades)
    | None ->
      Deferred.Or_error.(get ~account ~key ~secret >>| filter_trades min_ts)

  let get_one ~account ~orderID =
    let open Option in
    Int.Table.find table account >>= fun trades ->
    Uuid.Map.find trades orderID

  let set ~account trade =
    match trade.Execution.orderID with
    | None -> invalid_arg "TradeHistory.set"
    | Some orderID ->
        let data = Option.value_map
            (Int.Table.find table account)
            ~default:(Uuid.Map.singleton orderID trade)
            ~f:(Uuid.Map.add ~key:orderID ~data:trade) in
        Int.Table.set table ~key:account ~data
end

let send_heartbeat { Connection.addr ; w } span =
  let msg = DTC.default_heartbeat () in
  Clock_ns.every
    ~stop:(Writer.close_started w)
    ~continue_on_error:false span
    begin fun () ->
      (* Log.debug log_dtc "-> [%s] HB" addr ; *)
      write_message w `heartbeat DTC.gen_heartbeat msg
    end

let fail_ordStatus_execType ~ordStatus ~execType =
  invalid_argf
    "Wrong ordStatus/execType pair: %s, %s"
    (OrdStatus.show ordStatus)
    (ExecType.show execType)
    ()

let fail_ordStatus ordStatus =
  invalid_argf "Wrong ordStatus: %s" (OrdStatus.show ordStatus) ()

let write_exec_update ?request_id ~nb_msgs ~msg_number ~status ~reason w (e : Execution.t) =
  let cumQty = Option.map e.cumQty ~f:Int.to_float in
  let leavesQty = Option.map e.leavesQty ~f:Int.to_float in
  let orderQty = Option.map2 cumQty leavesQty ~f:Float.add in
  let u = DTC.default_order_update () in
  let price = Option.value ~default:Float.max_finite_value e.price in
  let stopPx = Option.value ~default:Float.max_finite_value e.stopPx in
  let side = Option.map e.side ~f:Side.of_string in
  let ordType = Option.map e.ordType ~f:OrderType.of_string in
  let timeInForce = Option.map e.timeInForce ~f:TimeInForce.of_string in
  let ts = Option.map e.transactTime ~f:seconds_int64_of_ts in
  let p1, p2 = OrderType.to_p1_p2 ~stopPx ~price
      (Option.value ~default:`order_type_unset ordType) in
  u.request_id <- request_id ;
  u.total_num_messages <- Some (Int32.of_int_exn nb_msgs) ;
  u.message_number <- Some (Int32.of_int_exn msg_number) ;
  u.symbol <- e.symbol ;
  u.exchange <- Some !my_exchange ;
  u.client_order_id <- e.clOrdID ;
  u.server_order_id <- Option.map ~f:Uuid.to_string e.orderID ;
  u.exchange_order_id <- Option.map ~f:Uuid.to_string e.orderID ;
  u.order_type <- ordType ;
  u.order_status <- Some status ;
  u.order_update_reason <- Some reason ;
  u.buy_sell <- side ;
  u.price1 <- p1 ;
  u.price2 <- p2 ;
  u.time_in_force <- timeInForce ;
  u.order_quantity <- orderQty ;
  u.filled_quantity <- cumQty ;
  u.remaining_quantity <- leavesQty ;
  u.average_fill_price <- e.avgPx ;
  u.last_fill_price <- e.lastPx ;
  u.last_fill_date_time <- ts ;
  u.last_fill_quantity <- Option.map ~f:Int.to_float e.lastQty ;
  u.last_fill_execution_id <- Some (Uuid.to_string e.execID) ;
  u.trade_account <- Option.map ~f:Int.to_string e.account ;
  u.info_text <- e.ordRejReason ;
  u.free_form_text <- e.text ;
  write_message w `order_update DTC.gen_order_update u

let write_order_update ?request_id ~nb_msgs ~msg_number ~status ~reason w (e : Order.t) =
  let cumQty = Option.map e.cumQty ~f:Int.to_float in
  let leavesQty = Option.map e.leavesQty ~f:Int.to_float in
  let orderQty = Option.map2 cumQty leavesQty ~f:Float.add in
  let u = DTC.default_order_update () in
  let price = Option.value ~default:Float.max_finite_value e.price in
  let stopPx = Option.value ~default:Float.max_finite_value e.stopPx in
  let side = Option.map e.side ~f:Side.of_string in
  let ordType = Option.map e.ordType ~f:OrderType.of_string in
  let timeInForce = Option.map e.timeInForce ~f:TimeInForce.of_string in
  let ts = Option.map e.transactTime ~f:seconds_int64_of_ts in
  let p1, p2 = OrderType.to_p1_p2 ~stopPx ~price
      (Option.value ~default:`order_type_unset ordType) in
  u.request_id <- request_id ;
  u.total_num_messages <- Some (Int32.of_int_exn nb_msgs) ;
  u.message_number <- Some (Int32.of_int_exn msg_number) ;
  u.symbol <- e.symbol ;
  u.exchange <- Some !my_exchange ;
  u.client_order_id <- e.clOrdID ;
  u.server_order_id <- Some (Uuid.to_string e.orderID) ;
  u.exchange_order_id <- Some (Uuid.to_string e.orderID) ;
  u.order_type <- ordType ;
  u.order_status <- Some status ;
  u.order_update_reason <- Some reason ;
  u.buy_sell <- side ;
  u.price1 <- p1 ;
  u.price2 <- p2 ;
  u.time_in_force <- timeInForce ;
  u.order_quantity <- orderQty ;
  u.filled_quantity <- cumQty ;
  u.remaining_quantity <- leavesQty ;
  u.average_fill_price <- e.avgPx ;
  u.last_fill_date_time <- ts ;
  u.trade_account <- Option.map ~f:Int.to_string e.account ;
  u.info_text <- e.ordRejReason ;
  u.free_form_text <- e.text ;
  write_message w `order_update DTC.gen_order_update u

let status_reason_of_execType_ordStatus execType ordStatus =
  match ordStatus, execType with

  | OrdStatus.New, ExecType.New
  | New, TriggeredOrActivatedBySystem -> `order_status_open, `new_order_accepted
  | New, Replaced -> `order_status_open, `order_cancel_replace_complete
  | New, Restated -> `order_status_open, `general_order_update

  | PartiallyFilled, Trade -> `order_status_partially_filled, `order_filled_partially
  | PartiallyFilled, Replaced -> `order_status_partially_filled, `order_cancel_replace_complete
  | PartiallyFilled, Restated -> `order_status_partially_filled, `general_order_update

  | Filled, Trade -> `order_status_filled, `order_filled
  | Canceled, Canceled -> `order_status_canceled, `order_canceled
  | Rejected, Rejected -> `order_status_rejected, `new_order_rejected

  | _, Funding -> raise Exit
  | _, Settlement -> raise Exit
  | _ -> fail_ordStatus_execType ~ordStatus ~execType

let status_reason_of_ordStatus = function
  | OrdStatus.New -> `order_status_open, `new_order_accepted
  | PartiallyFilled -> `order_status_partially_filled, `order_filled_partially
  | Filled -> `order_status_filled, `order_filled
  | Canceled -> `order_status_canceled, `order_canceled
  | Rejected -> `order_status_rejected, `new_order_rejected
  | ordStatus -> fail_ordStatus ordStatus

let write_exec_update ?request_id ?(nb_msgs=1) ?(msg_number=1) ?status_reason w e =
  match status_reason with
  | Some (status, reason) ->
      write_exec_update ?request_id ~nb_msgs ~msg_number ~status ~reason w e
  | None ->
      let ordStatus =
        Option.value_map e.ordStatus ~default:(OrdStatus.Unknown "") ~f:OrdStatus.of_string in
      let execType =
        Option.value_map e.execType ~default:(ExecType.Unknown "") ~f:ExecType.of_string in
      match status_reason_of_execType_ordStatus execType ordStatus with
      | exception Exit -> ()
      | exception Invalid_argument msg ->
          Log.error log_bitmex "Not sending order update for %s" msg ;
          ()
      | status, reason ->
          write_exec_update ?request_id ~nb_msgs ~msg_number ~status ~reason w e

let write_order_update ?request_id ?(nb_msgs=1) ?(msg_number=1) ?status_reason w o =
  match status_reason with
  | Some (status, reason) ->
      write_order_update ?request_id ~nb_msgs ~msg_number ~status ~reason w o
  | None ->
      let ordStatus =
        Option.value_map o.ordStatus ~default:(OrdStatus.Unknown "") ~f:OrdStatus.of_string in
      match status_reason_of_ordStatus ordStatus with
      | exception Exit -> ()
      | exception Invalid_argument msg ->
          Log.error log_bitmex "Not sending order update for %s" msg ;
          ()
      | status, reason ->
          write_order_update ?request_id ~nb_msgs ~msg_number ~status ~reason w o

let write_position_update ?request_id ~nb_msgs ~msg_number w (p : Position.t) =
  let u = DTC.default_position_update () in
  u.total_number_messages <- Some (Int32.of_int_exn nb_msgs) ;
  u.message_number <- Some (Int32.of_int_exn msg_number) ;
  u.request_id <- request_id ;
  u.symbol <- Some p.symbol ;
  u.exchange <- Some !my_exchange ;
  u.trade_account <- Some (Int.to_string p.account) ;
  u.average_price <- p.avgEntryPrice ;
  u.quantity <- Option.map p.currentQty ~f:Int.to_float ;
  write_message w `position_update DTC.gen_position_update u

let write_balance_update ?request_id ~msg_number ~nb_msgs w (m : Margin.t) =
  let margin_req = match m.initMargin, m.maintMargin, m.sessionMargin with
    | Some a, Some b, Some c -> Some ((a+b+c) // 100000)
    | _ -> None in
  let u = DTC.default_account_balance_update () in
  u.request_id <- request_id ;
  u.unsolicited <- Some (Option.is_none request_id) ;
  u.total_number_messages <- Some (Int32.of_int_exn nb_msgs) ;
  u.message_number <- Some (Int32.of_int_exn msg_number) ;
  u.account_currency <- Some "mXBT" ;
  u.cash_balance <-
    Option.map m.walletBalance ~f:(fun b -> Int.to_float b /. 1e5) ;
  u.balance_available_for_new_positions <-
    Option.map m.availableMargin ~f:(fun b -> Int.to_float b /. 1e5) ;
  u.securities_value <-
    Option.map m.marginBalance ~f:(fun b -> Int.to_float b /. 1e5) ;
  u.margin_requirement <- margin_req ;
  u.trade_account <- Some (Int.to_string m.account) ;
  write_message w `account_balance_update DTC.gen_account_balance_update u

type subscribe_msg =
  | Subscribe of Connection.t
  | Unsubscribe of Uuid.t * string

let client_ws_r, client_ws_w = Pipe.create ()

let process_order { Connection.addr ; order } partial_iv action o =
  match action with
  | WS.Response.Update.Delete ->
    Uuid.Table.remove order o.Order.orderID;
    Log.debug log_bitmex "<- [%s] order delete" addr
  | Insert
  | Partial ->
    Uuid.Table.set order ~key:o.orderID ~data:o;
    Log.debug log_bitmex "<- [%s] order insert/partial" addr
  | Update ->
    if Ivar.is_full partial_iv then begin
      let data = match Uuid.Table.find order o.orderID with
        | None -> o
        | Some old_o -> Order.merge old_o o
      in
      Uuid.Table.set order ~key:o.orderID ~data;
      Log.debug log_bitmex "<- [%s] order update" addr
    end

let process_margin ({ Connection.addr ; w ; margin } as c) partial_iv action i m =
  match action with
  | WS.Response.Update.Delete ->
    String.Table.remove margin m.Margin.currency ;
    Log.debug log_bitmex "<- [%s] margin delete" addr
  | Insert
  | Partial ->
    String.Table.set margin ~key:m.currency ~data:m;
    Log.debug log_bitmex "<- [%s] margin insert/partial" addr;
    write_balance_update ~msg_number:1 ~nb_msgs:1 w m ;
    if action = Partial then c.account <- m.account
  | Update ->
    if Ivar.is_full partial_iv then begin
      let m = match String.Table.find margin m.currency with
        | None -> m
        | Some old_m -> Margin.merge old_m m
      in
      String.Table.set margin ~key:m.currency ~data:m;
      write_balance_update ~msg_number:1 ~nb_msgs:1 w m ;
      Log.debug log_bitmex "<- [%s] margin update" addr
    end

let process_position { Connection.addr ; w ; position } partial_iv action p =
  match action with
  | WS.Response.Update.Delete ->
    String.Table.remove position p.Position.symbol ;
    Log.debug log_bitmex "<- [%s] position delete" addr
  | Insert | Partial ->
    String.Table.set position ~key:p.symbol ~data:p;
    if Option.value ~default:false p.isOpen then begin
      write_position_update ~nb_msgs:1 ~msg_number:1 w p ;
      Log.debug log_bitmex "<- [%s] position insert/partial" addr ;
    end
  | Update ->
    if Ivar.is_full partial_iv then begin
      let old_p, p = match String.Table.find position p.symbol with
        | None -> None, p
        | Some old_p -> Some old_p, Position.merge old_p p
      in
      String.Table.set position ~key:p.symbol ~data:p;
      match old_p with
      | Some old_p when Option.value ~default:false old_p.isOpen ->
        write_position_update ~nb_msgs:1 ~msg_number:1 w p ;
        Log.debug log_dtc "<- [%s] position update %s" addr p.symbol
      | _ -> ()
    end

let process_exec { Connection.addr ; w ; key ; account } e =
  let execType = Option.map e.Execution.execType ~f:ExecType.of_string in
  let ordStatus = Option.map e.ordStatus ~f:OrdStatus.of_string in
  Option.iter e.symbol ~f:(fun s -> Log.debug log_bitmex "<- [%s] exec %s" addr s) ;
  begin match execType, ordStatus with
    | Some Trade, Some Filled -> TradeHistory.set ~account e
    | _ -> ()
  end ;
  write_exec_update w e

let client_ws ({ Connection.addr; w; ws_r; key; secret; order; margin; position; } as c) =
  let order_iv = Ivar.create () in
  let margin_iv = Ivar.create () in
  let position_iv = Ivar.create () in

  let on_update { WS.Response.Update.table; action; data } =
    match table, action, data with
    | Order, action, orders ->
      List.iter ~f:(Fn.compose (process_order c order_iv action) Order.of_yojson) orders ;
      if action = Partial then Ivar.fill_if_empty order_iv ()
    | Margin, action, margins ->
      List.iteri ~f:(fun i m -> process_margin c margin_iv action i (Margin.of_yojson m)) margins ;
      if action = Partial then Ivar.fill_if_empty margin_iv ()
    | Position, action, positions ->
      List.iter ~f:(Fn.compose (process_position c position_iv action) Position.of_yojson) positions ;
      if action = Partial then Ivar.fill_if_empty position_iv ()
    | Execution, action, execs ->
      List.iter ~f:(Fn.compose (process_exec c) Execution.of_yojson) execs ;
      Log.debug log_dtc "-> [%s] Sent Order Updates" addr
    | table, _, _ ->
        Log.error log_bitmex "Unknown table %s" (Bmex_ws.Topic.to_string table)
  in
  Pipe.write client_ws_w @@ Subscribe c >>= fun () ->
  don't_wait_for @@ Monitor.handle_errors
    (fun () -> Pipe.iter_without_pushback ~continue_on_error:true ws_r ~f:on_update)
    (fun exn -> Log.error log_bitmex "%s" @@ Exn.to_string exn);
  Deferred.all_unit (List.map ~f:Ivar.read [order_iv; position_iv; margin_iv])

let encoding_request addr w req =
  Log.debug log_dtc "<- [%s] Encoding Request" addr ;
  Dtc_pb.Encoding.(to_string (Response { version = 7 ; encoding = Protobuf })) |>
  Writer.write w ;
  Log.debug log_dtc "-> [%s] Encoding Response" addr

let accept_logon_request addr w req client stop_exec_inst trading =
  let hb_span =
    Option.value_map req.DTC.Logon_request.heartbeat_interval_in_seconds
      ~default:(Time_ns.Span.of_int_sec 10)
      ~f:(fun span -> Time_ns.Span.of_int_sec (Int32.to_int_exn span)) in
  let trading_supported, result_text =
    match trading with
    | Ok msg -> true, Printf.sprintf "Trading enabled: %s" msg
    | Error msg -> false, Printf.sprintf "Trading disabled: %s" msg
  in
  let r = DTC.default_logon_response () in
  r.protocol_version <- Some 7l ;
  r.server_name <- Some "BitMEX" ;
  r.result <- Some `logon_success ;
  r.result_text <- Some result_text ;
  r.symbol_exchange_delimiter <- Some "-" ;
  r.security_definitions_supported <- Some true ;
  r.market_data_supported <- Some true ;
  r.historical_price_data_supported <- Some false ;
  r.market_depth_is_supported <- Some true ;
  r.market_depth_updates_best_bid_and_ask <- Some true ;
  r.trading_is_supported <- Some trading_supported ;
  r.order_cancel_replace_supported <- Some true ;
  r.ocoorders_supported <- Some false ;
  r.bracket_orders_supported <- Some false ;

  send_heartbeat client hb_span ;
  write_message w `logon_response DTC.gen_logon_response r ;

  Log.debug log_dtc "-> [%s] Logon Response" addr ;
  let on_instrument { Instr.secdef } =
    secdef.request_id <- Some 0l ;
    secdef.is_final_message <- Some true ;
    write_message w `security_definition_response
      DTC.gen_security_definition_response secdef
  in
  Instr.iter ~f:on_instrument

let setup_client_ws conn apikey =
  let ws_initialized = client_ws conn in
  let ws_ok = choice ws_initialized (fun () ->
      Log.info log_dtc "BitMEX accepts API key %s" apikey ;
      Result.return "API key valid."
    )
  in
  let timeout = choice (Clock_ns.after @@ Time_ns.Span.of_int_sec 20) (fun () ->
      Log.info log_dtc "BitMEX rejects API key %s" apikey ;
      Result.fail "BitMEX rejects your API key."
    )
  in
  choose [ws_ok; timeout]

let logon_request addr w msg =
  let req = DTC.parse_logon_request msg in
  Log.debug log_dtc "<- [%s] Logon Request" addr ;
  let stop_exec_inst =
    match req.integer_1 with
    | Some 1l -> ExecInst.LastPrice
    | Some 2l -> IndexPrice
    | _ -> MarkPrice in
  begin
    match req.username, req.password with
    | Some apikey, Some secret ->
      let conn = Connection.create addr w apikey secret stop_exec_inst in
      String.Table.set Connection.active ~key:addr ~data:conn ;
      don't_wait_for begin
        setup_client_ws conn apikey >>|
        accept_logon_request addr w req conn stop_exec_inst
      end
    | _ ->
      let conn = Connection.create addr w "" "" stop_exec_inst in
      String.Table.set Connection.active ~key:addr ~data:conn ;
      accept_logon_request addr w req conn stop_exec_inst @@
      Result.fail "No login provided, data only"
  end

let heartbeat addr w msg =
  (* Log.debug log_dtc "<- [%s] Heartbeat" addr *)
  ()

let security_definition_reject addr w request_id k =
  Printf.ksprintf begin fun msg ->
    let resp = DTC.default_security_definition_reject () in
    resp.request_id <- Some request_id ;
    resp.reject_text <- Some msg ;
    Log.debug log_dtc "-> [%s] Security Definition Reject" addr ;
    write_message w `security_definition_reject DTC.gen_security_definition_reject resp
  end k

let security_definition_request addr w msg =
  let req = DTC.parse_security_definition_for_symbol_request msg in
  match req.request_id, req.symbol, req.exchange with
  | Some id, Some symbol, Some exchange ->
    Log.debug log_dtc "<- [%s] Security Definition Request %s %s" addr symbol exchange;
    if !my_exchange <> exchange && not Instr.(is_index symbol) then
      security_definition_reject addr w id "No such symbol %s %s" symbol exchange
    else begin
      match Instr.find symbol with
      | None ->
        security_definition_reject addr w id "No such symbol %s %s" symbol exchange
      | Some { secdef } ->
        secdef.request_id <- Some id ;
        secdef.is_final_message <- Some true ;
        Log.debug log_dtc
          "-> [%s] Security Definition Response %s %s" addr symbol exchange;
        write_message w `security_definition_response
          DTC.gen_security_definition_response secdef
    end
  | _ ->
    Log.error log_dtc "<- [%s] BAD Security Definition Request" addr

let reject_market_data_request ?id addr w k =
  Printf.ksprintf begin fun reason ->
    let resp = DTC.default_market_data_reject () in
    resp.symbol_id <- id ;
    resp.reject_text <- Some reason ;
    Log.debug log_dtc "-> [%s] Market Data Reject" addr ;
    write_message w `market_data_reject DTC.gen_market_data_reject resp
  end k

let write_market_data_snapshot ?id addr w symbol
    { Instr.instr; last_trade_price;
      last_trade_size; last_trade_ts; last_quote_ts } =
  if Instr.is_index symbol then begin
    let snap = DTC.default_market_data_snapshot () in
    snap.symbol_id <- id ;
    snap.session_settlement_price <- instr.prevPrice24h ;
    snap.last_trade_price <- instr.lastPrice ;
    snap.last_trade_date_time <- Option.map instr.timestamp ~f:float_of_ts ;
    write_message w `market_data_snapshot DTC.gen_market_data_snapshot snap
  end
  else begin
    let { Quote.bidPrice; bidSize; askPrice; askSize } = Quotes.find_exn symbol in
    let open Option in
    let snap = DTC.default_market_data_snapshot () in
    snap.session_settlement_price <-
      Some (value ~default:Float.max_finite_value instr.indicativeSettlePrice) ;
    snap.session_high_price <-
      Some (value ~default:Float.max_finite_value instr.highPrice) ;
    snap.session_low_price <-
      Some (value ~default:Float.max_finite_value instr.lowPrice) ;
    snap.session_volume <-
      Some (value_map instr.volume ~default:Float.max_finite_value ~f:Int.to_float) ;
    snap.open_interest <-
      Some (value_map instr.openInterest ~default:0xffffffffl ~f:Int.to_int32_exn) ;
    snap.bid_price <- bidPrice ;
    snap.bid_quantity <- Option.(map bidSize ~f:Float.of_int) ;
    snap.ask_price <- askPrice ;
    snap.ask_quantity <- Option.(map askSize ~f:Float.of_int) ;
    snap.last_trade_price <- Some last_trade_price ;
    snap.last_trade_volume <- Some (Int.to_float last_trade_size) ;
    snap.last_trade_date_time <- Some (float_of_ts last_trade_ts) ;
    snap.bid_ask_date_time <- Some (float_of_ts last_quote_ts) ;
    write_message w `market_data_snapshot DTC.gen_market_data_snapshot snap
  end

let market_data_request addr w msg =
  let req = DTC.parse_market_data_request msg in
  let { Connection.subs ; rev_subs } = Connection.find_exn addr in
  match req.request_action,
        req.symbol_id,
        req.symbol,
        req.exchange
  with
  | _, id, Some symbol, Some exchange
    when exchange <> !my_exchange && Instr.(is_index symbol) ->
    reject_market_data_request ?id addr w "No such exchange %s" exchange
  | _, id, Some symbol, _ when not (Instr.mem symbol) ->
    reject_market_data_request ?id addr w "No such symbol %s" symbol
  | Some `unsubscribe, Some id, _, _ ->
    begin match Int32.Table.find rev_subs id with
    | None -> ()
    | Some symbol -> String.Table.remove subs symbol
    end ;
    Int32.Table.remove rev_subs id
  | Some `snapshot, id, Some symbol, Some exchange ->
    Log.debug log_dtc "<- [%s] Market Data Request (snapshot) %s %s"
      addr symbol exchange ;
    let instr = Instr.find_exn symbol in
    write_market_data_snapshot ?id addr w symbol instr ;
    Log.debug log_dtc "-> [%s] Market Data Snapshot %s %s" addr symbol exchange
  | Some `subscribe, Some id, Some symbol, Some exchange ->
    Log.debug log_dtc "<- [%s] Market Data Request (subscribe) %ld %s %s"
      addr id symbol exchange ;
    begin
      match Int32.Table.find rev_subs id with
      | Some symbol' when symbol <> symbol' ->
        reject_market_data_request addr w ~id
          "Already subscribed to %s %s with a different id (was %ld)"
          symbol exchange id
      | _ ->
        String.Table.set subs symbol id ;
        Int32.Table.set rev_subs id symbol ;
        let instr = Instr.find_exn symbol in
        write_market_data_snapshot ~id addr w symbol instr ;
        Log.debug log_dtc "-> [%s] Market Data Snapshot %s %s" addr symbol exchange
    end
  | _ ->
    reject_market_data_request addr w "Market Data Request: wrong request"

let reject_market_depth_request ?id addr w k =
  Printf.ksprintf begin fun reject_text ->
    let rej = DTC.default_market_depth_reject () in
    rej.symbol_id <- id ;
    rej.reject_text <- Some reject_text ;
    Log.debug log_dtc "-> [%s] Market Depth Reject: %s" addr reject_text;
    write_message w `market_depth_reject
      DTC.gen_market_depth_reject rej
  end k

let write_market_depth_snapshot ?id addr w ~symbol ~num_levels =
  let bids = Books.get_bids symbol in
  let asks = Books.get_asks symbol in
  let snap = DTC.default_market_depth_snapshot_level () in
  snap.symbol_id <- id ;
  snap.side <- Some `at_bid ;
  snap.is_last_message_in_batch <- Some false ;
  Float.Map.fold_right bids ~init:1 ~f:begin fun ~key:price ~data:size lvl ->
    snap.price <- Some price ;
    snap.quantity <- Some (Float.of_int size) ;
    snap.level <- Some (Int32.of_int_exn lvl) ;
    snap.is_first_message_in_batch <- Some (lvl = 1) ;
    write_message w `market_depth_snapshot_level DTC.gen_market_depth_snapshot_level snap ;
    succ lvl
  end |> ignore;
  snap.side <- Some `at_ask ;
  Float.Map.fold asks ~init:1 ~f:begin fun ~key:price ~data:size lvl ->
    snap.price <- Some price ;
    snap.quantity <- Some (Float.of_int size) ;
    snap.level <- Some (Int32.of_int_exn lvl) ;
    snap.is_first_message_in_batch <- Some (lvl = 1 && Float.Map.is_empty bids) ;
    write_message w `market_depth_snapshot_level DTC.gen_market_depth_snapshot_level snap ;
    succ lvl
  end |> ignore;
  snap.price <- None ;
  snap.quantity <- None ;
  snap.level <- None ;
  snap.is_first_message_in_batch <- Some false ;
  snap.is_last_message_in_batch <- Some true ;
  write_message w `market_depth_snapshot_level DTC.gen_market_depth_snapshot_level snap

let market_depth_request addr w msg =
  let req = DTC.parse_market_depth_request msg in
  let num_levels = Option.value_map req.num_levels ~default:50 ~f:Int32.to_int_exn in
  let { Connection.subs_depth ; rev_subs_depth } = Connection.find_exn addr in
  match req.request_action,
        req.symbol_id,
        req.symbol,
        req.exchange
  with
  | _, id, _, Some exchange when exchange <> !my_exchange ->
    reject_market_depth_request ?id addr w "No such exchange %s" exchange
  | _, id, Some symbol, _ when not (Instr.mem symbol) ->
    reject_market_data_request ?id addr w "No such symbol %s" symbol
  | Some `unsubscribe, Some id, _, _ ->
    begin match Int32.Table.find rev_subs_depth id with
    | None -> ()
    | Some symbol -> String.Table.remove subs_depth symbol
    end ;
    Int32.Table.remove rev_subs_depth id
  | Some `snapshot, id, Some symbol, Some exchange ->
    write_market_depth_snapshot ?id addr w ~symbol ~num_levels
  | Some `subscribe, Some id, Some symbol, Some exchange ->
    Log.debug log_dtc "<- [%s] Market Data Request %ld %s %s"
      addr id symbol exchange ;
    begin
      match Int32.Table.find rev_subs_depth id with
      | Some symbol' when symbol <> symbol' ->
        reject_market_data_request addr w ~id
          "Already subscribed to %s %s with a different id (was %ld)"
          symbol exchange id
      | _ ->
        String.Table.set subs_depth symbol id ;
        Int32.Table.set rev_subs_depth id symbol ;
        write_market_depth_snapshot ~id addr w ~symbol ~num_levels
    end
  | _ ->
    reject_market_data_request addr w "Market Data Request: wrong request"

let write_no_open_orders req w =
  let u = DTC.default_order_update () in
  u.total_num_messages <- Some 1l ;
  u.message_number <- Some 1l ;
  u.request_id <- req.DTC.Open_orders_request.request_id ;
  u.trade_account <- req.trade_account ;
  u.no_orders <- Some true ;
  u.order_update_reason <- Some `open_orders_request_response ;
  write_message w `order_update DTC.gen_order_update u

let open_orders_request addr w msg =
  let req = DTC.parse_open_orders_request msg in
  let { Connection.order } = Connection.find_exn addr in
  Log.debug log_dtc "<- [%s] Open Orders Request" addr ;
  let nb_msgs, open_orders =
    Uuid.Table.fold order  ~init:(0, []) ~f:begin fun ~key:_ ~data ((nb_open_orders, os) as acc) ->
      match Option.map ~f:OrdStatus.of_string data.Order.ordStatus with
      | Some New -> (succ nb_open_orders, (`order_status_open, data) :: os)
      | Some PartiallyFilled -> (succ nb_open_orders, (`order_status_partially_filled, data) :: os)
      | Some PendingCancel -> (succ nb_open_orders, (`order_status_pending_cancel, data) :: os)
      | _ -> acc
    end
  in
  List.iteri open_orders ~f:begin fun msg_number (ordStatus, o) ->
    let status_reason = ordStatus, `open_orders_request_response in
    write_order_update ~status_reason ~nb_msgs ~msg_number w o
  end ;
  if nb_msgs = 0 then write_no_open_orders req w ;
  Log.debug log_dtc "-> [%s] Open Orders Response (%d orders)" addr nb_msgs

let write_no_positions req w =
  let u = DTC.default_position_update () in
  u.total_number_messages <- Some 1l ;
  u.message_number <- Some 1l ;
  u.request_id <- req.DTC.Current_positions_request.request_id ;
  u.trade_account <- req.trade_account ;
  u.no_positions <- Some true ;
  u.unsolicited <- Some false ;
  write_message w `position_update DTC.gen_position_update u

let current_positions_request addr w msg =
  let req = DTC.parse_current_positions_request msg in
  let { Connection.position } = Connection.find_exn addr in
  Log.debug log_dtc "<- [%s] Current Positions Request" addr ;
  let nb_msgs, open_positions = String.Table.fold position ~init:(0, [])
      ~f:begin fun ~key:_ ~data ((nb_open_ps, open_ps) as acc) ->
        if Option.value ~default:false data.Position.isOpen then
          succ nb_open_ps, data :: open_ps
        else acc
      end
  in
  List.iteri open_positions ~f:begin fun i ->
    write_position_update
      ?request_id:req.request_id
      ~nb_msgs
      ~msg_number:(succ i) w
  end;
  if nb_msgs = 0 then write_no_positions req w ;
  Log.debug log_dtc "-> [%s] Current Position Response (%d positions)" addr nb_msgs

let send_historical_order_fills_response req addr w execs =
  let resp = DTC.default_historical_order_fill_response () in
  let nb_msgs = Uuid.Map.length execs in
  resp.total_number_messages <- Some (Int32.of_int_exn nb_msgs) ;
  resp.request_id <- req.DTC.Historical_order_fills_request.request_id ;
  resp.trade_account <- req.trade_account ;
  let _ = Uuid.Map.fold execs ~init:1l ~f:begin fun ~key:_ ~data:t message_number ->
      let orderQty =
        Option.map2 t.Execution.cumQty t.leavesQty ~f:(fun a b -> Int.to_float (a + b)) in
      let side = Option.value_map ~default:`buy_sell_unset t.side ~f:Side.of_string in
      resp.message_number <- Some message_number ;
      resp.symbol <- t.symbol ;
      resp.exchange <- Some !my_exchange ;
      resp.server_order_id <- Option.map ~f:Uuid.to_string t.orderID ;
      resp.price <- t.avgPx ;
      resp.quantity <- orderQty ;
      resp.date_time <- Option.map t.transactTime ~f:seconds_int64_of_ts ;
      resp.buy_sell <- Some side ;
      resp.unique_execution_id <- Some (Uuid.to_string t.execID) ;
      write_message w `historical_order_fill_response
        DTC.gen_historical_order_fill_response resp ;
      Int32.succ message_number
    end in
  Log.debug log_dtc "-> [%s] Historical Order Fills Response (%d fills)" addr nb_msgs

let reject_historical_order_fills_request ?request_id w k =
  let rej = DTC.default_historical_order_fills_reject () in
  rej.request_id <- request_id ;
  Printf.ksprintf begin fun reject_text ->
    rej.reject_text <- Some reject_text ;
    write_message w `historical_order_fills_reject
      DTC.gen_historical_order_fills_reject rej
  end k

let write_no_historical_order_fills req w =
  let u = DTC.default_historical_order_fill_response () in
  u.total_number_messages <- Some 1l ;
  u.message_number <- Some 1l ;
  u.request_id <- req.DTC.Historical_order_fills_request.request_id ;
  u.server_order_id <- req.server_order_id ;
  u.trade_account <- req.trade_account ;
  u.no_order_fills <- Some true ;
  write_message w `historical_order_fill_response
    DTC.gen_historical_order_fill_response u

let historical_order_fills_request addr w msg =
  let req = DTC.parse_historical_order_fills_request msg in
  let min_ts = Option.map req.number_of_days ~f:begin fun i ->
      Time_ns.(sub (now ()) (Span.of_day (Int32.to_float i)))
    end in
  let { Connection.key ; secret ; account} = Connection.find_exn addr in
  match Option.value ~default:"" req.server_order_id with
  | "" ->
    Log.debug log_dtc "<- [%s] Historical Order Fills Request" addr ;
    don't_wait_for begin
      TradeHistory.get ?min_ts ~account ~key ~secret >>| function
      | Ok trades when Uuid.Map.is_empty trades ->
        write_no_historical_order_fills req w ;
        Log.debug log_dtc "-> [%s] No Order Fills" addr ;
      | Ok trades ->
        send_historical_order_fills_response req addr w trades ;
      | Error err ->
        log_error log_bitmex err ;
        reject_historical_order_fills_request ?request_id:req.request_id w
          "Error fetching historical order fills from BitMEX"
    end
  | orderID_str -> begin
      let orderID = Uuid.of_string orderID_str in
      Log.debug log_dtc "<- [%s] Historical Order Fills Request for %s" addr orderID_str ;
      match TradeHistory.get_one ~account ~orderID with
      | None ->
        write_no_historical_order_fills req w ;
        Log.debug log_dtc "-> [%s] No Order Fills" addr ;
      | Some trade ->
        send_historical_order_fills_response req addr w
          (Uuid.Map.singleton orderID trade)
    end

let trade_accounts_request addr w msg =
  let req = DTC.parse_trade_accounts_request msg in
  let { Connection.margin } = Connection.find_exn addr in
  Log.debug log_dtc "<- [%s] Trade Accounts Request" addr ;
  let margin = List.hd_exn (String.Table.data margin) in
  let resp = DTC.default_trade_account_response () in
  resp.request_id <- req.request_id ;
  resp.total_number_messages <- Some 1l ;
  resp.message_number <- Some 1l ;
  resp.trade_account <- Some Int.(to_string margin.Margin.account) ;
  write_message w `trade_account_response  DTC.gen_trade_account_response resp ;
  Log.debug log_dtc "-> [%s] Trade Account Response (%d)" addr margin.account

let reject_account_balance_request ?request_id addr w k =
  let rej = DTC.default_account_balance_reject () in
  rej.request_id <- request_id ;
  Printf.ksprintf begin fun reject_text ->
    rej.reject_text <- Some reject_text ;
    write_message w `account_balance_reject  DTC.gen_account_balance_reject rej ;
    Log.debug log_dtc "-> [%s] Account Balance Reject" addr ;
  end k

let write_no_balances req addr w =
  let resp = DTC.default_account_balance_update () in
  resp.request_id <- req.DTC.Account_balance_request.request_id ;
  resp.trade_account <- req.trade_account ;
  resp.total_number_messages <- Some 1l ;
  resp.message_number <- Some 1l ;
  resp.no_account_balances <- Some true ;
  resp.unsolicited <- Some false ;
  write_message w `account_balance_update  DTC.gen_account_balance_update resp ;
  Log.debug log_dtc "-> [%s] No Account Balance" addr

let write_account_balance_update ?request_id ~msg_number ~nb_msgs w obj =
  write_balance_update ?request_id ~msg_number ~nb_msgs w obj

let account_balance_request addr w msg =
  let req = DTC.parse_account_balance_request msg in
  let { Connection.margin } = Connection.find_exn addr in
  let nb_msgs = String.Table.length margin in
  Log.debug log_dtc "<- [%s] Account Balance Request" addr ;
  let msg_sent =
    String.Table.fold margin ~init:1 ~f:begin fun ~key:currency ~data msg_number ->
      write_account_balance_update ~msg_number ~nb_msgs w data ;
      Log.debug log_dtc "-> [%s] Account Balance Response (ALL)" addr ;
      succ msg_number
    end in
  if msg_sent = 1 then write_no_balances req addr w

let reject_order (req : DTC.Submit_new_single_order.t) w k =
  let rej = DTC.default_order_update () in
  rej.total_num_messages <- Some 1l ;
  rej.message_number <- Some 1l ;
  rej.trade_account <- req.trade_account ;
  rej.symbol <- req.symbol ;
  rej.exchange <- req.exchange ;
  rej.order_status <- Some `order_status_rejected ;
  rej.order_update_reason <- Some `new_order_rejected ;
  rej.client_order_id <- req.client_order_id ;
  rej.order_type <- req.order_type ;
  rej.buy_sell <- req.buy_sell ;
  rej.price1 <- req.price1 ;
  rej.price2 <- req.price2 ;
  rej.order_quantity <- req.quantity ;
  rej.time_in_force <- req.time_in_force ;
  rej.good_till_date_time <- req.good_till_date_time ;
  rej.free_form_text <- req.free_form_text ;
  Printf.ksprintf begin fun info_text ->
    rej.info_text <- Some info_text ;
    write_message w `order_update DTC.gen_order_update rej ;
  end k

let update_order (req : DTC.Submit_new_single_order.t) w ~status ~reason k =
  let u = DTC.default_order_update () in
  u.total_num_messages <- Some 1l ;
  u.message_number <- Some 1l ;
  u.trade_account <- req.trade_account ;
  u.symbol <- req.symbol ;
  u.exchange <- req.exchange ;
  u.order_status <- Some status ;
  u.order_update_reason <- Some reason ;
  u.client_order_id <- req.client_order_id ;
  u.order_type <- req.order_type ;
  u.buy_sell <- req.buy_sell ;
  u.open_or_close <- req.open_or_close ;
  u.price1 <- req.price1 ;
  u.price2 <- req.price2 ;
  u.order_quantity <- req.quantity ;
  u.time_in_force <- req.time_in_force ;
  u.good_till_date_time <- req.good_till_date_time ;
  u.free_form_text <- req.free_form_text ;
  Printf.ksprintf begin fun info_text ->
    u.info_text <- Some info_text ;
    write_message w `order_update DTC.gen_order_update u ;
  end k

let submit_order w ~key ~secret (req : DTC.Submit_new_single_order.t) stop_exec_inst =
  let symbol = Option.value_exn ~message:"symbol is undefined" req.symbol in
  let orderQty = Option.value_exn ~message:"qty is undefined" req.quantity in
  let orderQty = Int.of_float @@ match req.buy_sell with
    | Some `sell -> Float.neg orderQty
    | _ -> orderQty in
  let ordType = Option.value ~default:`order_type_unset req.order_type in
  let timeInForce = Option.value ~default:`tif_unset req.time_in_force in
  let price, stopPx =
    OrderType.to_price_stopPx ?p1:req.price1 ?p2:req.price2 ordType in
  let execInst = match ordType with
    | `order_type_market
    | `order_type_limit -> []
    | #OrderType.t -> [stop_exec_inst] in
  let displayQty, execInst = match timeInForce with
    | `tif_all_or_none -> Some 0, ExecInst.AllOrNone :: execInst
    | #DTC.time_in_force_enum -> None, execInst in
  let order =
    REST.Order.create
      ?displayQty
      ~execInst
      ?price
      ?stopPx
      ?clOrdID:req.client_order_id
      ?text:req.free_form_text
      ~symbol ~orderQty ~ordType ~timeInForce ()
  in
  REST.Order.submit_bulk
    ~extract_exn:true
    ~log:log_bitmex
    ~testnet:!use_testnet ~key ~secret [order] >>| function
  | Ok (resp, orders) ->
      List.iter orders ~f:begin fun o ->
        Log.sexp log_bitmex (Order.sexp_of_t o)
      end
  | Error err ->
    log_error log_bitmex err ~f:(fun s -> reject_order req w "%s" s)

let submit_new_single_order addr w msg =
  let req = DTC.parse_submit_new_single_order msg in
  let { Connection.key; secret;
        current_parent; stop_exec_inst } as conn = Connection.find_exn addr in
  Log.debug log_dtc "<- [%s] Submit New Single Order" addr ;
  if req.time_in_force = Some `tif_good_till_date_time then begin
    reject_order req w "BitMEX does not support TIF Good till datetime"
  end
  else if Option.is_some current_parent then begin
    let current_parent = Option.(value_exn current_parent) in
    conn.current_parent <- None ;
    reject_order current_parent w "Next received order was not an OCO" ;
    reject_order req w "Previous received order was also a parent and the current order is not an OCO"
  end
  else if req.is_parent_order = Some true then begin
    Connection.set_parent_order conn req ;
    update_order req w
      ~status:`order_status_pending_open
      ~reason:`general_order_update
      "parent order stored" ;
    Log.debug log_dtc "Stored parent order"
  end
  else don't_wait_for (submit_order w ~key ~secret req stop_exec_inst)

let reject_cancel_replace_order
    ?client_order_id
    ?server_order_id
    ?symbol
    ?(order_status=`order_status_unspecified)
    (req : DTC.Cancel_replace_order.t) addr w k =
  let rej = DTC.default_order_update () in
  rej.symbol <- symbol ;
  rej.exchange <- Some !my_exchange ;
  rej.total_num_messages <- Some 1l ;
  rej.message_number <- Some 1l ;
  rej.order_update_reason <- Some `order_cancel_replace_rejected ;
  rej.client_order_id <- client_order_id ;
  rej.server_order_id <- Option.map server_order_id ~f:Uuid.to_string ;
  rej.order_status <- Some order_status ;
  Printf.ksprintf begin fun info_text ->
    rej.info_text <- Some info_text ;
    write_message w `order_update DTC.gen_order_update rej ;
    Log.debug log_dtc "-> [%s] Cancel Replace Rejected: %s" addr info_text
  end k

let amend_order addr w req key secret (o : Order.t) =
  let price1 = if req.DTC.Cancel_replace_order.price1_is_set = Some true then req.price1 else None in
  let price2 = if req.price2_is_set = Some true then req.price2 else None in
  let price, stopPx = match Option.map ~f:OrderType.of_string o.ordType with
  | None -> None, None
  | Some ordType -> OrderType.to_price_stopPx ?p1:price1 ?p2:price2 ordType in
  let amend = REST.Order.create_amend
    ?orderQty:(Option.map req.quantity ~f:Float.to_int)
    ?price
    ?stopPx
    ~orderID:o.orderID () in
  REST.Order.amend_bulk
    ~extract_exn:true ~log:log_bitmex
    ~testnet:!use_testnet ~key ~secret [amend] >>| function
  | Ok (_resp, body) -> ()
  | Error err ->
    log_error log_bitmex err ~f:begin fun s ->
      reject_cancel_replace_order
        ~server_order_id:o.orderID
        ?order_status:(Option.map o.ordStatus ~f:OrdStatus.(Fn.compose to_dtc of_string))
        req addr w "%s" s
    end

let cancel_replace_order addr w msg =
  let req = DTC.parse_cancel_replace_order msg in
  let { Connection.key; secret; order } = Connection.find_exn addr in
  Log.debug log_dtc "<- [%s] Cancel Replace Order" addr ;
  let order_type = Option.value ~default:`order_type_unset req.order_type in
  let time_in_force = Option.value ~default:`tif_unset req.time_in_force in
  match Option.bind req.server_order_id ~f:begin fun orderID ->
      Uuid.Table.find order (Uuid.of_string orderID)
    end with
  | None ->
    reject_cancel_replace_order
      ?client_order_id:req.client_order_id
      ~order_status:`order_status_rejected req addr w
      "Unknown ServerOrderID"
  | Some o ->
    let order_status =
      Option.value_map ~default:`order_status_unspecified ~f:(Fn.compose OrdStatus.to_dtc OrdStatus.of_string) o.ordStatus in
    let order_status = (order_status :> DTC.order_status_enum) in
    if order_type <> `order_type_unset then
      reject_cancel_replace_order
        ?symbol:o.symbol
        ~server_order_id:o.orderID
        ~order_status req addr w
        "Modification of order type is not supported by BitMEX"
    else if time_in_force <> `tif_unset then
      reject_cancel_replace_order
        ?symbol:o.symbol
        ~server_order_id:o.orderID
        ~order_status req addr w
        "Modification of time in force is not supported by BitMEX"
    else
      don't_wait_for (amend_order addr w req key secret o)

let reject_cancel_order
  ?client_order_id
  ?server_order_id
  ?symbol
  ~order_status
  (req : DTC.Cancel_order.t) addr w k =
  let rej = DTC.default_order_update () in
  rej.symbol <- symbol ;
  rej.exchange <- Some !my_exchange ;
  rej.total_num_messages <- Some 1l ;
  rej.message_number <- Some 1l ;
  rej.order_update_reason <- Some `order_cancel_rejected ;
  rej.client_order_id <- client_order_id ;
  rej.server_order_id <- Option.map server_order_id ~f:Uuid.to_string ;
  rej.order_status <- Some order_status ;
  Printf.ksprintf begin fun info_text ->
    rej.info_text <- Some info_text ;
    write_message w `order_update DTC.gen_order_update rej ;
    Log.debug log_dtc "-> [%s] Cancel Rejected: %s" addr info_text
  end k

let cancel_solo_order req addr w key secret o =
  REST.Order.cancel
    ~extract_exn:true ~log:log_bitmex ~testnet:!use_testnet
    ~key ~secret ~orderIDs:[o.Order.orderID] () >>| function
  | Ok (_resp, body) -> ()
  | Error err ->
    let order_status =
      Option.value_map o.ordStatus
        ~default:`order_status_unspecified
        ~f:(Fn.compose OrdStatus.to_dtc OrdStatus.of_string) in
    let order_status = (order_status :> DTC.order_status_enum) in
    log_error log_bitmex err ~f:begin fun s ->
      reject_cancel_order
        ?symbol:o.symbol
        ~order_status
        ~server_order_id:o.orderID
        req addr w "%s" s
    end

(* let cancel_linked_orders req addr w key secret linkID order_status = *)
(*   let filter = `Assoc ["clOrdLinkID", `String linkID] in *)
(*   REST.Order.cancel_all *)
(*     ~extract_exn:true ~log:log_bitmex ~testnet:!use_testnet ~key ~secret ~filter () >>| function *)
(*   | Ok _resp -> *)
(*     Log.debug log_bitmex "<- Cancel Order OK" *)
(*   | Error err -> *)
(*     log_error log_bitmex err *)
(*       ~f:(fun s -> reject_cancel_order req addr w "%s" s) *)

let cancel_order addr w msg =
  let req = DTC.parse_cancel_order msg in
  let { Connection.key; secret; parents ; order } = Connection.find_exn addr in
  Log.debug log_dtc "<- [%s] Cancel Order" addr ;
  match Option.(req.server_order_id >>| Uuid.of_string >>= Uuid.Table.find order) with
  (* , *)
  (* Option.(req.client_order_id >>= String.Table.find parents) with *)
  | Some o -> begin
      match Option.map ~f:OrdStatus.of_string o.ordStatus with
      | Some Canceled ->
        reject_cancel_order
          ?symbol:o.symbol
          ~server_order_id:o.orderID
          ~order_status:`order_status_canceled
          req addr w "Order has already been canceled"
      | Some PendingCancel ->
        reject_cancel_order
          ?symbol:o.symbol
          ~server_order_id:o.orderID
          ~order_status:`order_status_pending_cancel
          req addr w "Order is pending cancel"
      | _ ->
        don't_wait_for (cancel_solo_order req addr w key secret o)
    end
  (* | Some orderID, Some linkID -> *)
  (*   don't_wait_for (cancel_linked_orders req addr w key secret linkID) *)
  | _ ->
    reject_cancel_order
      ?client_order_id:req.client_order_id
      ~order_status:`order_status_rejected
      req addr w "Unknown ServerOrderID"

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

(*   | Some _ *)
(*   | None -> *)
(*     let buf = Buffer.create 128 in *)
(*     Cstruct.hexdump_to_buffer buf msg_cs; *)
(*     Log.error log_dtc "%s" @@ Buffer.contents buf *)

let dtcserver ~server ~port =
  let server_fun addr r w =
    don't_wait_for begin
      Condition.wait ws_feed_connected >>= fun () ->
      Deferred.all_unit [Writer.close w ; Reader.close r]
    end ;
    let addr = Socket.Address.Inet.to_string addr in
    (* So that process does not allocate all the time. *)
    let rec handle_chunk consumed buf ~pos ~len =
      if len < 2 then return @@ `Consumed (consumed, `Need_unknown)
      else
        let msglen = Bigstring.unsafe_get_int16_le buf ~pos in
        (* Log.debug log_dtc "handle_chunk: pos=%d len=%d, msglen=%d" pos len msglen; *)
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
            | `historical_order_fills_request -> historical_order_fills_request addr w msg
            | `trade_accounts_request -> trade_accounts_request addr w msg
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

let update_trade { Trade.symbol; timestamp; price; size; side } =
  let side = Option.value_map side ~default:`buy_sell_unset ~f:Side.of_string in
  let price = Option.value ~default:0. price in
  let size = Option.value ~default:0 size in
  Log.debug log_bitmex "trade %s %s %f %d" symbol (Side.show side) price size;
  match side, Instr.find symbol with
  | `buy_sell_unset, _ -> ()
  | _, None ->
    Log.error log_bitmex "update_trade: found no instrument for %s" symbol
  | _, Some instr ->
    instr.last_trade_price <- price;
    instr.last_trade_size <- size;
    instr.last_trade_ts <- timestamp;
    (* Send trade updates to subscribers. *)
    let at_bid_or_ask =
      match side with
      | `buy -> `at_ask
      | `sell -> `at_bid
      | `buy_sell_unset -> `bid_ask_unset in
    let u = DTC.default_market_data_update_trade () in
    u.at_bid_or_ask <- Some at_bid_or_ask ;
    u.price <- Some price ;
    u.volume <- Some (Int.to_float size) ;
    u.date_time <- Some (float_of_ts timestamp) ;
    let on_connection { Connection.addr; w; subs } =
      let on_symbol_id symbol_id =
        u.symbol_id <- Some symbol_id ;
        write_message w `market_data_update_trade
          DTC.gen_market_data_update_trade u ;
        Log.debug log_dtc "-> [%s] trade %s %s %f %d"
          addr symbol (Side.show side) price size
      in
      Option.iter String.Table.(find subs symbol) ~f:on_symbol_id
    in
    Connection.iter ~f:on_connection

let on_update { Bmex_ws.Response.Update.table ; action ; data } =
  match action, table, data with
  | Update, Instrument, instrs ->
    if Ivar.is_full Instr.initialized then
      List.iter instrs ~f:(Fn.compose Instr.update Instrument.of_yojson)
  | Delete, Instrument, instrs ->
    if Ivar.is_full Instr.initialized then
      List.iter instrs ~f:(Fn.compose Instr.delete Instrument.of_yojson)
  | _, Instrument, instrs ->
    List.iter instrs ~f:(Fn.compose Instr.insert Instrument.of_yojson) ;
    Ivar.fill_if_empty Instr.initialized ()
  | _, OrderBookL2, depths ->
    let depths = List.map depths ~f:OrderBookL2.of_yojson in
    let depths = List.group depths
        ~break:(fun { symbol } { symbol=symbol' } -> symbol <> symbol')
    in
    don't_wait_for begin
      Ivar.read Instr.initialized >>| fun () ->
      List.iter depths ~f:begin function
        | [] -> ()
        | h::t as ds ->
          Log.debug log_bitmex "depth update %s" h.symbol;
          List.iter ds ~f:(Books.update action)
      end;
      Ivar.fill_if_empty Books.initialized ()
    end
  | _, Trade, trades ->
    let open Trade in
    don't_wait_for begin
      Ivar.read Instr.initialized >>| fun () ->
      List.iter trades ~f:(Fn.compose update_trade Trade.of_yojson)
    end
  | _, Quote, quotes ->
    List.iter quotes ~f:(Fn.compose Quotes.update Quote.of_yojson) ;
    Ivar.fill_if_empty Quotes.initialized ()
  | _, table, json ->
    Log.error log_bitmex "Unknown/ignored BitMEX DB table %s or wrong json %s"
      (Bmex_ws.Topic.to_string table)
      Yojson.Safe.(to_string @@ `List json)

let subscribe_topics ~id ~topic ~topics =
  let open Bmex_ws in
  let payload =
    Request.(subscribe (List.map topics ~f:Sub.create) |> to_yojson) in
  Bmex_ws.MD.message id topic payload

let on_ws_msg to_ws_w my_uuid msg =
  let open Bmex_ws in
  let bitmex_topics = Topic.[Instrument; Quote; OrderBookL2; Trade] in
  let clients_topics = Topic.[Order; Execution; Position; Margin] in
  match MD.of_yojson ~log:log_bitmex msg with
  | Unsubscribe _ -> ()
  | Subscribe _ -> ()
  | Message { stream = { id ; topic } ; payload } ->
    match Response.of_yojson ~log:log_bitmex payload, topic = my_topic with

    (* Server *)
    | Response.Welcome _, true ->
      Pipe.write_without_pushback to_ws_w @@
      MD.to_yojson @@ subscribe_topics my_uuid my_topic bitmex_topics
    | Error err, true ->
      Log.error log_bitmex "BitMEX: error %s" err
    | Response { subscribe = Some { topic; symbol = Some sym }}, true ->
        Log.info log_bitmex "BitMEX: subscribed to %s:%s" (Topic.show topic) sym
    | Response { subscribe = Some { topic; symbol = None }}, true ->
        Log.info log_bitmex "BitMEX: subscribed to %s" (Topic.show topic)
    | Update update, true -> on_update update

    (* Clients *)
    | Welcome _, false ->
      Option.iter (Connection.find topic) ~f:begin fun { key; secret } ->
        Pipe.write_without_pushback to_ws_w @@
        MD.to_yojson @@ MD.auth ~id ~topic ~key ~secret
      end
    | Error err, false ->
      Log.error log_bitmex "%s: error %s" topic err
    | Response { request = AuthKey _ }, false ->
      Pipe.write_without_pushback to_ws_w @@
      MD.to_yojson @@ subscribe_topics ~id ~topic ~topics:clients_topics
    | Response { subscribe = Some { topic = subscription } }, false ->
      Log.info log_bitmex "%s: subscribed to %s" topic (Topic.show subscription)
    | Response _, false ->
      Log.error log_bitmex "%s: unexpected response %s" topic (Yojson.Safe.to_string payload)
    | Update update, false -> begin
      match Connection.find topic with
      | None ->
        Pipe.write_without_pushback client_ws_w (Unsubscribe (Uuid.of_string id, topic))
      | Some { ws_w } -> Pipe.write_without_pushback ws_w update
    end
    | _ -> ()

let subscribe_client to_ws_w ({ Connection.addr; key; secret } as c) =
  let id = Uuid.create () in
  let id_str = Uuid.to_string id in
  c.ws_uuid <- id;
  Pipe.write to_ws_w Bmex_ws.MD.(to_yojson (subscribe ~id:id_str ~topic:addr))

let bitmex_ws () =
  let open Bmex_ws in
  let to_ws, to_ws_w = Pipe.create () in
  let my_uuid = Uuid.(create () |> to_string) in
  let connected = Condition.create () in
  let rec resubscribe () =
    Condition.wait connected >>= fun () ->
    Condition.broadcast ws_feed_connected () ;
    Pipe.write to_ws_w @@ MD.to_yojson @@
    MD.subscribe ~id:my_uuid ~topic:my_topic >>= fun () ->
    Deferred.List.iter (Connection.to_alist ())
      ~how:`Sequential ~f:(fun (_addr, c) -> subscribe_client to_ws_w c) >>=
    resubscribe
  in
  don't_wait_for @@ resubscribe ();
  let ws = open_connection ~connected ~to_ws ~log:log_ws
      ~testnet:!use_testnet ~md:true ~topics:[] () in
  don't_wait_for begin
    Pipe.iter client_ws_r ~f:begin function
      | Subscribe c -> subscribe_client to_ws_w c
      | Unsubscribe (id, topic) ->
        Pipe.write to_ws_w @@ MD.to_yojson @@
        MD.unsubscribe ~id:(Uuid.to_string id) ~topic
    end
  end;
  Monitor.handle_errors
    (fun () -> Pipe.iter_without_pushback
        ~continue_on_error:true ws ~f:(on_ws_msg to_ws_w my_uuid))
    (fun exn -> Log.error log_bitmex "%s" @@ Exn.to_string exn)

let main
    tls testnet port daemon pidfile logfile
    loglevel ll_ws ll_dtc ll_bitmex crt_path key_path () =
  let pidfile = if testnet then add_suffix pidfile "_testnet" else pidfile in
  let logfile = if testnet then add_suffix logfile "_testnet" else logfile in
  let run server =
    Log.info log_bitmex "WS feed starting";
    let bitmex_th = bitmex_ws () in
    Deferred.List.iter ~how:`Parallel ~f:Ivar.read
      [Instr.initialized; Books.initialized; Quotes.initialized] >>= fun () ->
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

  Log.set_level log_dtc @@ loglevel_of_int @@ max loglevel ll_dtc;
  Log.set_level log_bitmex @@ loglevel_of_int @@ max loglevel ll_bitmex;
  Log.set_level log_ws @@ loglevel_of_int ll_ws;

  if daemon then Daemon.daemonize ~cd:"." ();
  stage begin fun `Scheduler_started ->
    Lock_file.create_exn pidfile >>= fun () ->
    Writer.open_file ~append:true logfile >>= fun log_writer ->
    Log.(set_output log_dtc Output.[stderr (); writer `Text log_writer]);
    Log.(set_output log_bitmex Output.[stderr (); writer `Text log_writer]);
    Log.(set_output log_ws Output.[stderr (); writer `Text log_writer]);
    conduit_server ~tls ~crt_path ~key_path >>= fun server ->
    loop_log_errors ~log:log_dtc (fun () -> run server)
  end

let command =
  let spec =
    let open Command.Spec in
    empty
    +> flag "-tls" no_arg ~doc:" Use TLS"
    +> flag "-testnet" no_arg ~doc:" Use testnet"
    +> flag "-port" (optional_with_default 5567 int) ~doc:"int TCP port to use (5567)"
    +> flag "-daemon" no_arg ~doc:" Run as a daemon"
    +> flag "-pidfile" (optional_with_default "run/bitmex.pid" string) ~doc:"filename Path of the pid file (run/bitmex.pid)"
    +> flag "-logfile" (optional_with_default "log/bitmex.log" string) ~doc:"filename Path of the log file (log/bitmex.log)"
    +> flag "-loglevel" (optional_with_default 1 int) ~doc:"1-3 global loglevel"
    +> flag "-loglevel-ws" (optional_with_default 1 int) ~doc:"1-3 loglevel for the websocket library"
    +> flag "-loglevel-dtc" (optional_with_default 1 int) ~doc:"1-3 loglevel for DTC"
    +> flag "-loglevel-bitmex" (optional_with_default 1 int) ~doc:"1-3 loglevel for BitMEX"
    +> flag "-crt-file" (optional_with_default "ssl/bitsouk.com.crt" string) ~doc:"filename crt file to use (TLS)"
    +> flag "-key-file" (optional_with_default "ssl/bitsouk.com.key" string) ~doc:"filename key file to use (TLS)"
  in
  Command.Staged.async ~summary:"BitMEX bridge" spec main

let () = Command.run command
