(* Continuously pump data from BitMEX and serves it with DTC *)

open Core
open Async
open Log.Global
open Cohttp_async

open Bs_devkit
open Bs_api.BMEX
open Dtc.Dtc

module DB = Data_util.Make(LevelDB)

let log_ws = Log.create ~level:`Error ~on_error:`Raise ~output:Log.Output.[stderr ()]

let use_testnet = ref false
let base_uri = ref @@ Uri.of_string "https://www.bitmex.com"
let datadir = ref "data"
let dry_run = ref false

let db_path symbol =
  let (//) = Filename.concat in
  !datadir // Uri.host_with_default !base_uri // symbol

let trades ?start ?count ?startTime ?endTime () =
  let uri = Uri.with_path !base_uri "/api/v1/trade" in
  let uri = Uri.add_query_params uri @@
    List.filter_opt [
      Option.some @@ ("columns", ["price"; "size"; "side"]);
      Option.map start ~f:(fun start -> "start", [string_of_int start]);
      Option.map count ~f:(fun count -> "count", [string_of_int count]);
      Option.map startTime ~f:(fun st -> "startTime", [Time_ns.to_string st]);
      Option.map endTime ~f:(fun et -> "endTime", [Time_ns.to_string et]);
    ]
  in
  debug "GET %s" @@ Uri.to_string uri;
  let process_trades = function
  | `List trades ->
    Or_error.try_with (fun () -> List.map trades ~f:(Fn.compose Result.ok_or_failwith Trade.of_yojson))
  | json ->
    Or_error.error_string Yojson.Safe.(to_string json)
  in
  Rest.call ~name:"trades" ~f:(Client.get) uri >>| Or_error.bind ~f:process_trades

let dbs = String.Table.create ()

let load_instruments_exn () =
  let iter_instrument json =
    let open RespObj in
    let t = of_json json in
    let key = string_exn t "symbol" in
    String.Table.add_exn dbs ~key
      ~data:(DB.open_db @@ db_path key);
    info "Opened DB %s" (string_exn t "symbol")
  in
  let open Cohttp_async in
  let uri = Uri.with_path !base_uri "/api/v1/instrument/activeAndIndices" in
  debug "GET %s" @@ Uri.to_string uri;
  Rest.call ~name:"instrument" ~f:Client.get uri >>| function
  | Error err ->
    let err_str = Error.to_string_hum err in
    error "%s" err_str
  | Ok body -> begin match body with
    | `List instrs_json -> List.(iter instrs_json ~f:iter_instrument)
    | json -> invalid_arg Yojson.Safe.(to_string json)
    end

let mk_store_trade_in_db () =
  let stamps = String.Table.create () in
  fun { Trade.timestamp; symbol; price; size; side } ->
    if !dry_run || side = "" then ()
    else
    let db = String.Table.find_or_add dbs symbol
        ~default:(fun () -> DB.open_db @@ db_path symbol)
    in
    let ts = Time_ns.of_string timestamp in
    let ts = match String.Table.find stamps symbol with
    | None ->
      String.Table.add_exn stamps symbol (ts, 0); ts
    | Some (old_ts, _) when old_ts <> ts ->
      String.Table.set stamps symbol (ts, 0); ts
    | Some (_, n) ->
      String.Table.set stamps symbol (ts, succ n); Time_ns.(add ts @@ Span.of_int_ns @@ succ n)
    in
    let price = satoshis_int_of_float_exn price |> Int63.of_int in
    let qty = Int63.of_int_exn size in
    let side = Option.value_exn (side_of_bmex side) in (* OK because filtered out earlier *)
    DB.store_trade_in_db db ~ts ~price ~qty ~side

let store_trade_in_db = mk_store_trade_in_db ()

let bitmex_ws subscribed =
  let on_ws_msg json = match Ws.msg_of_yojson json with
  | Welcome -> info "WS: connected"
  | Error msg -> error "BitMEX: error %s" @@ Ws.show_error msg
  | Ok { request = { op = "subscribe" }; subscribe; success } ->
    info "BitMEX: subscribed to %s: %b" subscribe success;
    Ivar.fill_if_empty subscribed ();
  | Ok { success } -> error "BitMEX: unexpected response %s" (Yojson.Safe.to_string json)
  | Update { data } ->
    List.iter data ~f:begin fun t ->
      let t = Result.ok_or_failwith @@ Trade.of_yojson t in
      debug "%s %s %f %d" t.side t.symbol t.price t.size;
      store_trade_in_db t
    end
  in
  let ws = Ws.open_connection ~log:log_ws ~testnet:!use_testnet ~md:false ~topics:["trade"] () in
  Monitor.handle_errors
    (fun () -> Pipe.iter_without_pushback ~continue_on_error:true ws ~f:on_ws_msg)
    (fun exn -> error "%s" @@ Exn.to_string exn)

let pump startTime period =
  let rec loop startTime = trades ~count:500 ~startTime () >>= function
    | Error err ->
      error "pump: %s" @@ Error.to_string_hum err;
      if period < 600 then
        (* double the period and retry *)
        Clock_ns.after @@ Time_ns.Span.of_int_sec (2 * period) >>= fun () ->
        loop startTime
      else Deferred.unit
    | Ok trades ->
      let open Trade in
      let nb_trades, last_ts =
        List.fold_left ~init:(0, Time_ns.epoch) trades ~f:begin fun (nb_trades, last_ts) t ->
          store_trade_in_db t;
          succ nb_trades, Time_ns.of_string t.timestamp
        end
      in
      let last_ts_str = Time_ns.to_string last_ts in
      debug "pumped %d trades up to to %s" nb_trades last_ts_str;
      if nb_trades < 500 then Deferred.unit
      else begin
        if not !dry_run then Out_channel.write_all (db_path "start") ~data:last_ts_str;
        Clock_ns.after @@ Time_ns.Span.of_int_sec period >>= fun () ->
        loop Time_ns.(add last_ts @@ Span.of_int_ns 1)
      end
  in
  loop startTime

(* A DTC Historical Price Server. *)

let tickserver port sockfile =
  let lr, lr_cs =
    let open Logon in
    let lr = Response.create
        ~server_name:"BitMEX Historical"
        ~result:Success
        ~result_text:"Welcome to the BitMEX Historical Data Server"
        ~historical_price_data_supported:true
        ~market_depth_updates_best_bid_and_ask:true
        () in
    let cs = Cstruct.create Response.sizeof_cs in
    Response.to_cstruct cs lr;
    lr, cs in
  let process addr w cs scratchbuf =
    match msg_of_enum Cstruct.LE.(get_uint16 cs 2) with

    | Some LogonRequest ->
      let open Logon in
      let m = Request.read cs in
      debug "<-\n%s\n%!" (Request.show m);
      (* Send a logon_response message. *)
      debug "->\n%s\n%!" (Logon.Response.show lr);
      Writer.write_cstruct w lr_cs;
      `Continue

    | Some HistoricalPriceDataRequest ->
      let open HistoricalPriceData in
      let r_cs = Cstruct.of_bigarray scratchbuf ~off:0 ~len:Reject.sizeof_cs in
      let reject request_id k =
        Printf.ksprintf begin fun str ->
          Reject.write r_cs ~request_id k;
          debug "-> HistoricalPriceData reject %s" str
        end k;
        Writer.write_cstruct w r_cs in
      let open Request in
      let accept db m =
        (* SC sometimes uses negative timestamps. Correcting this. *)
        let record_interval = Time_ns.Span.of_int_sec m.record_interval in
        let hdr = Header.create ~request_id:m.request_id ~record_ival:0 ~int_price_divisor:1e8 () in
        let hdr_cs = Cstruct.of_bigarray scratchbuf ~off:0 ~len:Header.sizeof_cs in
        Header.to_cstruct hdr_cs hdr;
        Writer.write_cstruct w hdr_cs;

        (* Now sending tick responses. *)

        let start_key = Bytes.create 8 in
        let end_key = Bytes.create 8 in
        Binary_packing.pack_signed_64_big_endian start_key 0 (m.start_ts |> Time_ns.to_int63_ns_since_epoch |> Int63.to_int64 |> Int64.abs);
        Binary_packing.pack_signed_64_big_endian end_key 0 (m.end_ts |> Time_ns.to_int63_ns_since_epoch |> Int63.to_int64);
        Time_ns.(info "Starting streaming %s-%s from %s to %s, interval = %s" m.symbol m.exchange (to_string m.start_ts) (to_string m.end_ts) (Span.to_string record_interval));
        let nb_streamed = ref 0 in
        if m.record_interval = 0 then begin (* streaming tick responses *)
          let tick_cs = Cstruct.of_bigarray scratchbuf ~off:0 ~len:Tick.sizeof_cs in
          DB.iter_from begin fun ts data ->
            let ts = Binary_packing.unpack_signed_64_big_endian ts 0 |> Int63.of_int64_exn |> Time_ns.of_int63_ns_since_epoch in
            if Time_ns.(m.end_ts > Time_ns.epoch && ts > m.end_ts) then false
            else begin
              let t = Dtc.Tick.Bytes.read' ~ts ~data () in
              let side = Option.value_exn ~message:"side is undefined" t.side in
              let p = Int63.to_float t.p /. 1e8 in
              let v = Int63.to_float t.v in
              Tick.write tick_cs ~final:false ~request_id:m.request_id ~ts ~p ~v ~side;
              Writer.write_cstruct w tick_cs;
              incr nb_streamed;
              true
            end
          end db start_key;
          Tick.write ~ts:Time_ns.epoch ~p:0. ~v:0. ~request_id:m.request_id ~final:true tick_cs;
          Writer.write_cstruct w tick_cs;
          !nb_streamed, !nb_streamed
        end
        else begin (* streaming record responses *)
          let g = new Granulator.granulator ~request_id:m.request_id ~record_interval ~writer:w in
          DB.iter_from begin fun ts data ->
            let ts = Binary_packing.unpack_signed_64_big_endian ts 0 |> Int63.of_int64_exn |> Time_ns.of_int63_ns_since_epoch in
            if Time_ns.(m.end_ts > epoch && ts > m.end_ts) then false
            else begin
              let t = Dtc.Tick.Bytes.read' ~ts ~data () in
              let side = Option.value_exn ~message:"side is undefined" t.side in
              let p = Int63.to_float t.p /. 1e8 in
              let v = Int63.to_float t.v in
              g#add_tick ts p v side;
              true
            end
          end db start_key;
          g#final
        end
      in
      let m = read cs in
      debug "<-\n%s\n%!" (show m);
      let db = match !use_testnet, m.exchange with
      | true, "BMEX"
      | false, "BMEXT" -> None
      | _ -> String.Table.find dbs m.symbol
      in
      begin match db with
      | None ->
        error "No such symbol or DB for %s-%s" m.symbol m.exchange;
        reject m.request_id "No such symbol"
      | Some db -> don't_wait_for begin
        In_thread.run (fun () -> accept db m) >>| fun (nb_streamed, nb_processed) ->
        info "Streamed %d/%d records from %s-%s" nb_streamed nb_processed m.symbol m.exchange
        end
      end;
      `Continue

    | Some _
    | None -> (* Unknown message, send LOGOFF/ do not reconnect. *)
      let open Logon in
      let logoff_cs = Cstruct.of_bigarray scratchbuf ~off:0 ~len:Logoff.sizeof_cs in
      Logoff.write logoff_cs ~reconnect:false "Unknown or unsupported message";
      Writer.write_cstruct w logoff_cs;
      `Stop
  in
  let handler addr r w =
    let addr_str = Socket.Address.to_string addr in
    debug "Incoming connection from %s" addr_str;
    let scratchbuf = Bigstring.create 1024 in
    let latest_activity = ref Time_ns.max_value in
    let rec handle_chunk continue consumed buf ~pos ~len =
      let maybe_continue action =
        latest_activity := Time_ns.now ();
        match action with
        | `Continue -> return @@ `Consumed (consumed, `Need_unknown)
        | `Stop -> return @@ `Stop_consumed ((), consumed)
      in
      if len < 2 then maybe_continue continue
      else
        let msglen = Bigstring.unsafe_get_int16_le buf ~pos in
        debug "handle_chunk: pos=%d len=%d, msglen=%d" pos len msglen;
        if len < msglen then maybe_continue continue
        else begin
          let msg_cs = Cstruct.of_bigarray buf ~off:pos ~len:msglen in
          let continue = process addr w msg_cs scratchbuf in
          handle_chunk continue (consumed + msglen) buf (pos + msglen) (len - msglen)
        end
    in
    let watch () =
      let stop_ivar = Ivar.create () in
      let stop = Ivar.read stop_ivar in
      Clock_ns.every ~stop (Time_ns.Span.of_int_sec 300) begin fun () ->
        if Time_ns.(Span.(diff (now ()) !latest_activity > of_int_sec 300)) then Ivar.fill stop_ivar ()
      end;
      stop
    in
    Deferred.any_unit [
      watch ();
      Deferred.ignore @@ Reader.read_one_chunk_at_a_time r ~handle_chunk:(handle_chunk `Continue 0);
    ] >>| fun _ ->
    debug "Client %s disconnected" addr_str
  in
  let on_handler_error addr exn =
    error "Client %s disconnected." Socket.Address.(to_string addr)
  in
  Monitor.try_with_or_error (fun () -> Sys.remove sockfile) >>= fun _ ->
  let socket_server =
    Option.value_map port ~default:Deferred.unit ~f:begin fun port ->
      Deferred.ignore Tcp.(Server.create ~on_handler_error:(`Call on_handler_error) (on_port port) handler);
    end
  in
  let tcp_server = Deferred.ignore Tcp.(Server.create ~on_handler_error:(`Call on_handler_error) (on_file sockfile) handler) in
  Deferred.all_unit [socket_server; tcp_server]

let main dry_run' testnet start period port sockfile daemon datadir' pidfile logfile loglevel ll_ws =
  dry_run := dry_run';
  let sockfile = if testnet then add_suffix sockfile "_testnet" else sockfile in
  let pidfile = if testnet then add_suffix pidfile "_testnet" else pidfile in
  let logfile = if testnet then add_suffix logfile "_testnet" else logfile in
  let start = match start with
  | Some ts -> Time_ns.of_string ts
  | None -> try Time_ns.of_string @@ In_channel.read_all @@ db_path "start"
  with _ -> Time_ns.epoch
  in
  let run () =
    load_instruments_exn () >>= fun () ->
    info "Data server starting";
    let subscribed = Ivar.create () in
    don't_wait_for (Ivar.read subscribed >>= fun () -> tickserver port sockfile);
    don't_wait_for (Ivar.read subscribed >>= fun () -> pump start period);
    bitmex_ws subscribed
  in

  (* begin initialization code *)
  if testnet then begin
    use_testnet := true;
    base_uri := Uri.of_string "https://testnet.bitmex.com";
  end;
  datadir := datadir';
  set_level @@ loglevel_of_int loglevel;
  Log.set_level log_ws @@ loglevel_of_int ll_ws;
  if daemon then Daemon.daemonize ~cd:"." ();
  Signal.handle Signal.terminating ~f:begin fun _ ->
    info "Data server stopping";
    String.Table.iter dbs ~f:DB.close;
    info "Saved %d dbs" @@ String.Table.length dbs;
    don't_wait_for @@ Shutdown.exit 0
  end;
  don't_wait_for begin
    Lock_file.create_exn pidfile >>= fun () ->
    Writer.open_file ~append:true logfile >>= fun log_writer ->
    set_output Log.Output.[stderr (); writer `Text log_writer];
    Log.(set_output log_ws Output.[stderr (); writer `Text log_writer]);
    run ();
  end;
  never_returns @@ Scheduler.go ()

let command =
  let spec =
    let open Command.Spec in
    empty
    +> flag "-dry-run" no_arg ~doc:" Do not write trades in DBs"
    +> flag "-testnet" no_arg ~doc:" Use testnet"
    +> flag "-start" (optional string) ~doc:"ts Starting ts to get results (default: use start file)"
    +> flag "-period" (optional_with_default 5 int) ~doc:"seconds Period for HTTP GETs (5)"
    +> flag "-port" (optional int) ~doc:"int TCP port to use (not used)"
    +> flag "-sockfile" (optional_with_default "run/bitmex.sock" string) ~doc:"filename UNIX sock to use (run/bitmex.sock)"
    +> flag "-daemon" no_arg ~doc:" Run as a daemon"
    +> flag "-datadir" (optional_with_default "data" string) ~doc:"path Where to store DBs (data)"
    +> flag "-pidfile" (optional_with_default "run/bitmex_data.pid" string) ~doc:"filename Path of the pid file (run/bitmex_data.pid)"
    +> flag "-logfile" (optional_with_default "log/bitmex_data.log" string) ~doc:"filename Path of the log file (log/bitmex_data.log)"
    +> flag "-loglevel" (optional_with_default 1 int) ~doc:"1-3 loglevel"
    +> flag "-loglevel-ws" (optional_with_default 1 int) ~doc:"1-3 loglevel for the websocket library"
  in
  Command.basic ~summary:"BitMEX data aggregator" spec main

let () = Command.run command
