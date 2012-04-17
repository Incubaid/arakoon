open Mem_store 
open Bstore
(* open Hub *)
open Lwt
open Mp_driver
open Dispatcher
open Pq 
open Node_cfg.Node_cfg
open Mp

(*module MyHub = HUB(BStore)*)

module BADispatcher = ADispatcher(BStore)
module FSMDriver = MPDriver(BADispatcher)

module DISPATCHER = BADispatcher
module DRIVER = FSMDriver

let _log f =  Printf.kprintf Lwt_io.printl f

let gen_request_id =
  let c = ref 0 in
  fun () -> let r = !c in 
            let () = incr c in 
            r

module C = struct
  open Common
  type t = TODO

  let prologue (ic,oc) = 
    let check magic version = 
      if magic = _MAGIC && 
        version = _VERSION 
      then Lwt.return ()
      else Llio.lwt_failfmt "MAGIC %lx or VERSION %x mismatch" magic version
    in
    let check_cluster cluster_id = 
      let ok = true in
      if ok then Lwt.return ()
      else Llio.lwt_failfmt "WRONG CLUSTER: %s" cluster_id
    in
    Llio.input_int32  ic >>= fun magic ->
    Llio.input_int    ic >>= fun version ->
    check magic version  >>= fun () ->
    Llio.input_string ic >>= fun cluster_id ->
    check_cluster cluster_id >>= fun () ->
    Lwt.return ()
    
  let __do_unit_update driver q =
    DRIVER.push_cli_req driver q >>= fun a ->
    match a with 
      | Core.UNIT -> Lwt.return ()
      | Core.FAILURE (rc, msg) -> failwith msg

  let _set driver k v = 
    let q = Core.SET(k,v) in
    __do_unit_update driver q

  let _delete driver k =
    let q = Core.DELETE k in
    __do_unit_update driver q

  let _get driver k = DRIVER.get driver k

  let _get_meta driver = DRIVER.get_meta driver 

  let one_command driver ((ic,oc) as conn) = 
    Client_protocol.read_command conn >>= fun comm ->
    match comm with
      | WHO_MASTER ->
        _log "who master" >>= fun () ->
        _get_meta driver >>= fun ms ->
        let mo =
        begin 
          match ms with
            | None -> None
            | Some s -> 
              begin
                let m, off = Llio.string_from s 0 in
                let e, off = Llio.float_from s off in
                if e > ( Unix.gettimeofday() ) 
                then
                  Some m
                else 
                  None 
              end
        end
        in
        Llio.output_int32 oc 0l >>= fun () ->
        Llio.output_string_option oc mo >>= fun () ->
        Lwt.return false
      | SET -> 
        begin
          Llio.input_string ic >>= fun key ->
          Llio.input_string ic >>= fun value ->
          _log "set %S %S" key value >>= fun () ->
          Lwt.catch
            (fun () -> 
              _set driver key value >>= fun () ->
              Client_protocol.response_ok_unit oc)
            (Client_protocol.handle_exception oc)
        end
      | GET ->
        begin
          Llio.input_bool ic >>= fun allow_dirty ->
          Llio.input_string ic >>= fun key ->
          _log "get %S" key >>= fun () ->
          Lwt.catch 
            (fun () -> 
              _get driver key >>= fun value ->
              Client_protocol.response_rc_string oc 0l value)
            (Client_protocol.handle_exception oc)
        end 

  let protocol driver (ic,oc) =   
    let rec loop () = 
      begin
        one_command driver (ic,oc) >>= fun stop ->
        if stop
        then _log "end of session"
        else 
          begin
            Lwt_io.flush oc >>= fun () ->
            loop ()
          end
      end
    in
    _log "session started" >>= fun () ->
    prologue(ic,oc) >>= fun () -> 
    _log "prologue ok" >>= fun () ->
    loop ()

end
(*
module MC = struct
  (* TODO Copied *)
  let __do_unit_update hub q =
    let id = gen_request_id () in
    MyHub.update hub id q >>= fun a ->
    match a with
      | Core.UNIT -> Lwt.return ()
      | Core.FAILURE (rc, msg) -> failwith msg

  let _set hub k v =
    let q = Core.SET(k,v) in
    __do_unit_update hub q

  let _delete hub k =
    let q = Core.DELETE k in
    __do_unit_update hub q

  let one_command hub ((ic, oc) as conn) =
    Memcache_protocol.read_command conn >>= fun comm ->
    match comm with
      | Memcache_protocol.GET keys ->
          begin
            _log "Memcache GET" >>= fun () ->
            Lwt.catch
              (fun () ->
                (* TODO This pulls everything in memory first. We might want to
                 * emit key/value pairs one by one instead *)
                Lwt_list.fold_left_s
                  (fun acc key ->
                    Lwt.catch
                      (fun () ->
                        MyHub.get hub key >>= fun value ->
                        Lwt.return ((key, value) :: acc))
                      (fun _ ->
                        Lwt.return acc))
                  [] keys
                >>=
                Memcache_protocol.response_get oc)
              (Memcache_protocol.handle_exception oc)
          end
      | Memcache_protocol.SET (key, value, noreply) ->
          begin
            _log "Memcache SET" >>= fun () ->
            Lwt.catch
            (fun () ->
              _set hub key value >>= fun () ->
              if noreply
              then
                Lwt.return false
              else
                Memcache_protocol.response_set oc)
            (Memcache_protocol.handle_exception oc)
          end
      | Memcache_protocol.DELETE (key, noreply) ->
          begin
            _log "Memcache DELETE" >>= fun () ->
            Lwt.catch
            (fun () ->
              _delete hub key >>= fun () ->
              (* TODO Handle NOT_FOUND *)
              if noreply
              then
                Lwt.return false
              else
                Memcache_protocol.response_delete oc true)
            (Memcache_protocol.handle_exception oc)
          end
      | Memcache_protocol.VERSION ->
          begin
            _log "Memcache VERSION" >>= fun () ->
            Memcache_protocol.response_version oc Version.git_info
          end
      | Memcache_protocol.QUIT ->
          begin
            _log "Memcache QUIT" >>= fun () ->
            Lwt.return true
          end
      | Memcache_protocol.ERROR ->
          begin
            _log "Memcache ERROR" >>= fun () ->
            Lwt.return true
          end

  let protocol hub (ic, oc) =
    _log "Memcache session started" >>= fun () ->
    let rec loop () =
      begin
        one_command hub (ic, oc) >>= fun stop ->
        if stop
        then _log "End of memcache session"
        else
          begin
            Lwt_io.flush oc >>= fun () ->
            loop ()
          end
      end
    in
    loop ()
end
*)
let server_t driver host port =
  let inner = Server.make_server_thread host port (C.protocol driver) in
  inner ()

(*
let mc_server_t hub =
  let host = "127.0.0.1"
  and port = 11211 in
  let inner = Server.make_server_thread host port (MC.protocol hub) in
  inner ()
*)
let log_prelude () = 
  _log "--- NODE STARTED ---" >>= fun () ->
  _log "git info: %s " Version.git_info >>= fun () ->
  _log "compile_time: %s " Version.compile_time >>= fun () ->
  _log "machine: %s " Version.machine 

let create_msging me others cluster_id =
  let cookie = cluster_id in
  let mapping = List.map
    (fun cfg -> (cfg.node_name, (cfg.ip, cfg.messaging_port)))
    (me::others)
  in
  let drop_it = (fun _ _ _ -> false) in
  let messaging = new Tcp_messaging.tcp_messaging 
    (me.ip, me.messaging_port) cookie drop_it in
  messaging # register_receivers mapping;
  (messaging :> Messaging.messaging)

let get_db_name cfg myname = 
  cfg.home ^ "/" ^ myname ^ ".bs"
  
let create_store cfg myname =
  let fn = get_db_name cfg myname in
  BStore.create fn  

let create_timeout_q () =
  PQ.create ()
  
let create_dispatcher cluster_id myname mycfg others store msging =
  let timeout_q = create_timeout_q () in
  let disp = DISPATCHER.create msging store timeout_q in
  (disp, timeout_q)

let range n = 
  let rec range_inner = function
    | 0 -> []
    | i -> (n-i) :: range_inner (i-1)
  in 
  range_inner n
  
let build_mpc mycfg others =
  let myname = mycfg.node_name in
  let other_cnt = List.length others in
  let name_others = List.fold_left 
    (fun acc cfg -> cfg.node_name :: acc) [] others
  in 
  let node_cnt = other_cnt + 1 in
  let q = 1 + (node_cnt / 2 ) in
  let lease = mycfg.lease_period in
  let all_nodes = List.fold_left (fun acc cfg-> cfg.node_name :: acc) [myname] others in
  let all_nodes_s = List.sort String.compare all_nodes in
  let nodes_with_ixs = List.combine all_nodes_s (range node_cnt) in
  let my_ix = List.assoc myname nodes_with_ixs in 
  MULTI.build_mp_constants q myname name_others (float_of_int lease) my_ix node_cnt 
  
let create_driver disp q = 
  DRIVER.create disp q (PQ.create())

let build_start_state store mycfg others =
  let n = Core.start_tick in
  BStore.last_update store >>= fun m_last ->
  let i,u =
    begin
      match m_last with
        | Some (i_time, u) -> Core.TICK i_time, Some u
        | None -> Core.start_tick, None
    end
  in
  let c = build_mpc mycfg others in
  let s = MULTI.build_state c n i i u in
  Lwt.return s 

let rec pass_msg msging q target =
  msging # recv_message ~target >>= fun (msg_s, id) ->
  let msg = MULTI.msg_of_string msg_s in
  PQ.push q msg ;
  pass_msg msging q target

type action_type =
  | ShowUsage
  | ShowVersion
  | RunNode
  | InitDb

let split_cfgs cfg myname =
  let (others, me_as_list) = List.partition (fun cfg -> cfg.node_name <> myname) cfg.cfgs in
  begin
    match me_as_list with
      | [] -> 
        Llio.lwt_failfmt "Node '%s' does not exist in config" myname 
      | cfg :: [] -> Lwt.return (others, cfg)
      | _ -> 
        Llio.lwt_failfmt "Node '%s' occurs multiple times in config" myname
  end

let run_node myname config_file daemonize =          
  let cfg = read_config config_file in
  let () = if daemonize then Lwt_daemon.daemonize () in
  split_cfgs cfg myname >>= fun (others, mycfg) ->
  let cluster_id = cfg.cluster_id in 
  let msging = create_msging mycfg others cluster_id in
  create_store mycfg myname >>= fun store ->
  let disp, q = create_dispatcher cluster_id myname mycfg others store msging in
  let driver = create_driver disp q in
  let service driver = server_t driver mycfg.ip mycfg.client_port in
  log_prelude () >>= fun () ->
  build_start_state store mycfg others >>= fun s ->
  let timeout = MULTI.M_LEASE_TIMEOUT Core.start_tick in
  DRIVER.push_msg driver timeout ;
  let other_names = List.fold_left (fun acc c -> c.node_name :: acc) [] others in
  let pass_msgs = List.map (pass_msg msging q) (myname :: other_names) in
  Lwt.pick [ DRIVER.serve driver s None ;
             service driver;
             msging # run ();
             Lwt.join pass_msgs
           ];;

let init_db myname config_file =
  let cfg = read_config config_file in
  split_cfgs cfg myname >>= fun (_, mycfg) ->
  let fn = get_db_name mycfg myname in
  BStore.init fn

let show_version ()=
  Lwt_io.printlf "git_info: %S" Version.git_info >>= fun () ->
  Lwt_io.printlf "compiled: %S" Version.compile_time >>= fun () ->
  Lwt_io.printlf "machine: %S" Version.machine 
  
let main_t () =
  let node_id = ref "" 
  and action = ref (ShowUsage) 
  and config_file = ref "cfg/arakoon.ini"
  and daemonize = ref false
  in
  let set_action a = Arg.Unit (fun () -> action := a) in
  let actions = [
    ("--node", 
     Arg.Tuple [set_action RunNode; Arg.Set_string node_id;],
     "Runs a node");
    ("-config", Arg.Set_string config_file,
     "Specifies config file (default = cfg/arakoon.ini)");
    ("-daemonize", Arg.Set daemonize,
     "add if you want the process to daemonize (only for --node)");
    ("--init-db", 
     Arg.Tuple [set_action InitDb; Arg.Set_string node_id;],
     "Initialize the database for the given node");
    ("--version",
     Arg.Tuple [set_action ShowVersion],
     "returns version info")
  ] in
  
  Arg.parse actions  
    (fun x -> raise (Arg.Bad ("Bad argument : " ^ x)))
    "";
  begin 
    match !action with
      | RunNode -> run_node !node_id !config_file !daemonize
      | ShowUsage -> Lwt.return (Arg.usage actions "")
      | InitDb -> init_db !node_id !config_file
      | ShowVersion -> show_version()
  end

let () =  Lwt_main.run (main_t())
  
