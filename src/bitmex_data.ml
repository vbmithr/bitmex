(* Continuously pump data from BitMEX and serves it with DTC *)

open Core
open Async
open Log.Global
open Cohttp_async

open Bs_devkit
open Bmex
module REST = Bmex_rest
module DTC = Dtc_pb.Dtcprotocol_piqi

module DB = struct
  include Tick.MakeLDB(LevelDB)
  let store_trade_in_db ?sync db ~ts ~price ~qty ~side =
    put_tick ?sync db @@ Tick.create ~ts ~side ~p:price ~v:qty ()
end

let buf = Bi_outbuf.create 4096

let use_testnet = ref false
let base_uri = ref @@ Uri.of_string "https://www.bitmex.com"
let datadir = ref "data"
let dry_run = ref false

let db_path symbol =
  let (//) = Filename.concat in
  !datadir // Uri.host_with_default !base_uri // symbol

let dbs = String.Table.create ()

let load_instruments () =
  let iter_instrument json =
    let open RespObj in
    let t = of_json json in
    let key = string_exn t "symbol" in
    String.Table.add_exn dbs ~key ~data:(DB.open_db @@ db_path key);
    info "Opened DB %s" (string_exn t "symbol")
  in
  REST.Instrument.active_and_indices
    ~buf ~log:(Lazy.force log) ~testnet:!use_testnet () >>|
  Or_error.map ~f:(List.iter ~f:iter_instrument)

let mk_store_trade_in_db () =
  let tss = String.Table.create () in
  fun { Trade.timestamp; symbol; price; size; side } ->
    if !dry_run || side = `buy_sell_unset then ()
    else
      let db = String.Table.find_or_add dbs symbol
          ~default:(fun () -> DB.open_db @@ db_path symbol) in
      let ts = match String.Table.find tss symbol with
        | None ->
          String.Table.add_exn tss symbol (timestamp, 0); timestamp
        | Some (old_ts, _) when old_ts <> timestamp ->
          String.Table.set tss symbol (timestamp, 0); timestamp
        | Some (_, n) ->
          String.Table.set tss symbol (timestamp, succ n);
          Time_ns.(add timestamp @@ Span.of_int_ns @@ succ n)
      in
      let price = satoshis_int_of_float_exn price |> Int63.of_int in
      let qty = Int63.of_int_exn size in
      DB.store_trade_in_db db ~ts ~price ~qty ~side

let store_trade_in_db = mk_store_trade_in_db ()

let rec pump startTime period =
  REST.Trade.get ~testnet:!use_testnet ~count:500 ~startTime () >>= function
  | Error err ->
    error "pump: %s" @@ Error.to_string_hum err ;
    Deferred.unit
  | Ok trades ->
    let nb_trades, last_ts =
      List.fold_left ~init:(0, Time_ns.epoch) trades ~f:begin fun (nb_trades, last_ts) t ->
        store_trade_in_db t;
        succ nb_trades, t.timestamp
      end in
    let last_ts_str = Time_ns.to_string last_ts in
    let period =
      if nb_trades = 500 then period else Time_ns.Span.of_int_sec 600 in
    debug "pumped %d trades up to to %s" nb_trades last_ts_str;
    if not !dry_run then Out_channel.write_all (db_path "start") ~data:last_ts_str;
    Clock_ns.after period >>= fun () ->
    pump Time_ns.(add last_ts @@ Span.of_int_ns 1) period

(* A DTC Historical Price Server. *)

let write_message w (typ : DTC.dtcmessage_type) gen msg =
  let typ =
    Piqirun.(DTC.gen_dtcmessage_type typ |> to_string |> init_from_string |> int_of_varint) in
  let msg = (gen msg |> Piqirun.to_string) in
  let header = Bytes.create 4 in
  Binary_packing.pack_unsigned_16_little_endian ~buf:header ~pos:0 (4 + String.length msg) ;
  Binary_packing.pack_unsigned_16_little_endian ~buf:header ~pos:2 typ ;
  Writer.write w header ;
  Writer.write w msg

let encoding_request addr w req =
  debug "<- [%s] Encoding Request" addr ;
  Dtc_pb.Encoding.(to_string (Response { version = 7 ; encoding = Protobuf })) |>
  Writer.write w ;
  debug "-> [%s] Encoding Response" addr

let accept_logon_request addr w req =
  let r = DTC.default_logon_response () in
  r.protocol_version <- Some 7l ;
  r.server_name <- Some "BitMEX Data" ;
  r.result <- Some `logon_success ;
  r.result_text <- Some "OK" ;
  r.symbol_exchange_delimiter <- Some "-" ;
  r.historical_price_data_supported <- Some true ;
  r.one_historical_price_data_request_per_connection <- Some true ;
  write_message w `logon_response DTC.gen_logon_response r ;
  debug "-> [%s] Logon Response" addr

let logon_request addr w msg =
  let req = DTC.parse_logon_request msg in
  debug "<- [%s] Logon Request" addr ;
  accept_logon_request addr w req

let heartbeat addr w msg =
  debug "<- [%s] Heartbeat" addr

let reject_historical_price_data_request ?reason_code w (req : DTC.Historical_price_data_request.t) k =
  let rej = DTC.default_historical_price_data_reject () in
  rej.request_id <- req.request_id ;
  rej.reject_reason_code <- reason_code ;
  Printf.ksprintf begin fun reject_text ->
    rej.reject_text <- Some reject_text ;
    write_message w `historical_price_data_reject
      DTC.gen_historical_price_data_reject rej ;
    debug "-> HistoricalPriceData reject %s" reject_text
  end k

let span_of_interval = function
  | `interval_10_seconds -> Time_ns.Span.of_int_sec 10
  | `interval_1_day -> Time_ns.Span.of_day 1.
  | `interval_1_minute -> Time_ns.Span.of_int_sec 60
  | `interval_1_second -> Time_ns.Span.of_int_sec 1
  | `interval_1_week -> Time_ns.Span.of_day 7.
  | `interval_2_seconds -> Time_ns.Span.of_int_sec 2
  | `interval_30_seconds -> Time_ns.Span.of_int_sec 30
  | `interval_4_seconds -> Time_ns.Span.of_int_sec 4
  | `interval_5_seconds -> Time_ns.Span.of_int_sec 5
  | `interval_tick -> Time_ns.Span.zero

let accept_historical_price_data_request
    w (req : DTC.Historical_price_data_request.t) db symbol =
  (* SC sometimes uses negative timestamps. Correcting this. *)
  let hdr = DTC.default_historical_price_data_response_header () in
  hdr.request_id <- req.request_id ;
  hdr.record_interval <- req.record_interval ;
  hdr.use_zlib_compression <- Some false ;
  hdr.int_to_float_price_divisor <- Some 1e8 ;
  write_message w `historical_price_data_response_header
      DTC.gen_historical_price_data_response_header hdr ;

  (* Now sending tick responses. *)
  let start_date_time =
    Int64.(max 0L (Option.value_map req.start_date_time ~default:0L ~f:(( * ) 1_000_000_000L))) in
  let end_date_time =
    Option.value_map req.end_date_time
      ~default:Int64.max_value ~f:Int64.(( * ) 1_000_000_000L) in
  let start_key = Bytes.create 8 in
  Binary_packing.pack_signed_64_big_endian start_key 0 start_date_time ;
  (* Time_ns.(info "Starting streaming %s from %s to %s, interval = %s" *)
  (*            symbol (to_string m.start_ts) (to_string m.end_ts) (Span.to_string record_interval)); *)
  let nb_streamed = ref 0 in
  (* if m.record_interval = 0 then begin (\* streaming tick responses *\) *)
  let resp = DTC.default_historical_price_data_tick_record_response () in
  resp.request_id <- req.request_id ;
  resp.is_final_record <- Some false ;
  DB.iter_from begin fun ts data ->
    let ts = Binary_packing.unpack_signed_64_big_endian ts 0 in
    if Int64.(ts > end_date_time) then false
    else begin
      let t = Tick.Bytes.read' ~ts:Time_ns.epoch ~data () in
      let p = Int63.to_float t.p /. 1e8 in
      let v = Int63.to_float t.v in
      let side = match t.side with
        | `buy -> `at_bid
        | `sell -> `at_ask
        | `buy_sell_unset -> `bid_ask_unset in
      resp.date_time <- Some Float.(of_int64 ts / 1e8) ;
      resp.price <- Some p ;
      resp.volume <- Some v ;
      resp.at_bid_or_ask <- Some side ;
      write_message w `historical_price_data_tick_record_response
        DTC.gen_historical_price_data_tick_record_response resp ;
      incr nb_streamed ;
      true
    end
  end db start_key;
  let resp = DTC.default_historical_price_data_tick_record_response () in
  resp.request_id <- req.request_id ;
  resp.is_final_record <- Some true ;
  write_message w `historical_price_data_tick_record_response
      DTC.gen_historical_price_data_tick_record_response resp ;
  !nb_streamed, !nb_streamed
  (* end *)
  (* else begin (\* streaming record responses *\) *)
  (*   let g = new Granulator.granulator ~request_id:m.request_id ~record_interval ~writer:w in *)
  (*   DB.iter_from begin fun ts data -> *)
  (*     let ts = Binary_packing.unpack_signed_64_big_endian ts 0 |> Int63.of_int64_exn |> Time_ns.of_int63_ns_since_epoch in *)
  (*     if Time_ns.(m.end_ts > epoch && ts > m.end_ts) then false *)
  (*     else begin *)
  (*       let t = Dtc.Tick.Bytes.read' ~ts ~data () in *)
  (*       let side = Option.value_exn ~message:"side is undefined" t.side in *)
  (*       let p = Int63.to_float t.p /. 1e8 in *)
  (*       let v = Int63.to_float t.v in *)
  (*       g#add_tick ts p v side; *)
  (*       true *)
  (*     end *)
  (*   end db start_key; *)
  (*   g#final *)
  (* end *)

let historical_price_data_request addr w msg =
  let req = DTC.parse_historical_price_data_request msg in
  debug "<- [%s] Historical Data Request" addr ;
  let record_span =
    Option.value_map ~default:Time_ns.Span.zero
      ~f:span_of_interval req.record_interval in
  if record_span <> Time_ns.Span.zero then begin
    reject_historical_price_data_request
      ~reason_code:`hpdr_general_reject_error
      w req "Only Tick record interval is currently supported" ;
    raise Exit
  end ;
  match req.symbol, req.exchange, !use_testnet with
  | None, _, _ ->
    reject_historical_price_data_request
      ~reason_code:`hpdr_unable_to_serve_data_do_not_retry
      w req "Symbol not specified" ;
    raise Exit
  | Some symbol, Some exchange, true when exchange = "BMEX" ->
    error "BMEX/BMEXT mismatch" ;
    reject_historical_price_data_request
      ~reason_code:`hpdr_unable_to_serve_data_do_not_retry
      w req "BMEX/BMEX mismatch" ;
    raise Exit
  | Some symbol, Some exchange, false when exchange = "BMEXT" ->
    error "BMEX/BMEXT mismatch" ;
    reject_historical_price_data_request
      ~reason_code:`hpdr_unable_to_serve_data_do_not_retry
      w req "BMEX/BMEX mismatch" ;
    raise Exit
  | Some symbol, _, _ ->
    match String.Table.find dbs symbol with
    | None ->
      reject_historical_price_data_request
        ~reason_code:`hpdr_unable_to_serve_data_do_not_retry
        w req "No such symbol" ;
      raise Exit
    | Some db ->
      don't_wait_for begin
        In_thread.run begin fun () ->
          accept_historical_price_data_request w req db symbol
        end >>| fun (nb_streamed, nb_processed) ->
        info "Streamed %d/%d records from %s"
          nb_streamed nb_processed symbol
      end

let dtcserver ~server ~port =
  let server_fun addr r w =
    let addr = Socket.Address.Inet.to_string addr in
    (* So that process does not allocate all the time. *)
    let rec handle_chunk consumed buf ~pos ~len =
      if len < 2 then return @@ `Consumed (consumed, `Need_unknown)
      else
        let msglen = Bigstring.unsafe_get_int16_le buf ~pos in
        debug "handle_chunk: pos=%d len=%d, msglen=%d" pos len msglen;
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
                | None -> error "Invalid encoding request received"
                | Some msg -> encoding_request addr w msg
              end
            | `logon_request -> logon_request addr w msg
            | `heartbeat -> heartbeat addr w msg
            | `historical_price_data_request -> historical_price_data_request addr w msg
            | #DTC.dtcmessage_type ->
              error "Unknown msg type %d" msgtype_int
          end ;
          handle_chunk (consumed + msglen) buf (pos + msglen) (len - msglen)
        end
    in
    let on_connection_io_error exn =
      error "on_connection_io_error (%s): %s" addr Exn.(to_string exn)
    in
    let cleanup () =
      info "client %s disconnected" addr ;
      Deferred.all_unit [Writer.close w; Reader.close r]
    in
    Deferred.ignore @@ Monitor.protect ~finally:cleanup begin fun () ->
      Monitor.detach_and_iter_errors Writer.(monitor w) ~f:on_connection_io_error;
      Reader.(read_one_chunk_at_a_time r ~handle_chunk:(handle_chunk 0))
    end
  in
  let on_handler_error_f addr exn =
    error "on_handler_error (%s): %s"
      Socket.Address.(to_string addr) Exn.(to_string exn)
  in
  Conduit_async.serve
    ~on_handler_error:(`Call on_handler_error_f)
    server (Tcp.on_port port) server_fun

let run port start period =
  load_instruments () >>= function
  | Error err -> raise (Error.to_exn err)
  | Ok () ->
    info "Data server starting";
    dtcserver ~server:`TCP ~port >>= fun server ->
    Deferred.all_unit [
      Tcp.Server.close_finished server ;
      pump start period
    ]

let main dry_run' testnet start period port daemon datadir' pidfile logfile loglevel () =
  dry_run := dry_run';
  let pidfile = if testnet then add_suffix pidfile "_testnet" else pidfile in
  let logfile = if testnet then add_suffix logfile "_testnet" else logfile in
  let period = Time_ns.Span.of_int_sec period in

  (* begin initialization code *)
  if testnet then begin
    use_testnet := true;
    base_uri := Uri.of_string "https://testnet.bitmex.com";
  end;
  datadir := datadir';
  set_level @@ loglevel_of_int loglevel;
  if daemon then Daemon.daemonize ~cd:"." ();

  let start = match start with
  | Some days -> Time_ns.(sub (now ()) (Span.of_day days))
  | None ->
    try Time_ns.of_string @@ In_channel.read_all @@ db_path "start"
    with exn ->
      error "Unable to read start file, using epoch %s" (Exn.to_string exn) ;
      Time_ns.epoch in

  Signal.handle Signal.terminating ~f:begin fun _ ->
    info "Data server stopping";
    String.Table.iter dbs ~f:DB.close;
    info "Saved %d dbs" @@ String.Table.length dbs;
    don't_wait_for @@ Shutdown.exit 0
  end ;
  stage begin fun `Scheduler_started ->
    Lock_file.create_exn pidfile >>= fun () ->
    Writer.open_file ~append:true logfile >>= fun log_writer ->
    set_output Log.Output.[stderr (); writer `Text log_writer];
    run port start period
  end

let command =
  let spec =
    let open Command.Spec in
    empty
    +> flag "-dry-run" no_arg ~doc:" Do not write trades in DBs"
    +> flag "-testnet" no_arg ~doc:" Use testnet"
    +> flag "-start" (optional float) ~doc:"float Start gathering history N days in the past (default: use start file)"
    +> flag "-period" (optional_with_default 5 int) ~doc:"seconds Period for HTTP GETs (5)"
    +> flag "-port" (optional_with_default 5568 int) ~doc:"int TCP port to use (5568)"
    +> flag "-daemon" no_arg ~doc:" Run as a daemon"
    +> flag "-datadir" (optional_with_default "data" string) ~doc:"path Where to store DBs (data)"
    +> flag "-pidfile" (optional_with_default "run/bitmex_data.pid" string) ~doc:"filename Path of the pid file (run/bitmex_data.pid)"
    +> flag "-logfile" (optional_with_default "log/bitmex_data.log" string) ~doc:"filename Path of the log file (log/bitmex_data.log)"
    +> flag "-loglevel" (optional_with_default 1 int) ~doc:"1-3 loglevel"
  in
  Command.Staged.async ~summary:"BitMEX data aggregator" spec main

let () = Command.run command
