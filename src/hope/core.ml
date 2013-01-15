module type SM = sig
  type state
  type action
  type msg

  val state2s : state -> string
  val action2s : action -> string
  val msg2s : msg -> string

  val start : state
  val step: msg -> state -> action * state
end

type k = string
type v = string

type update = 
  | SET of k * v
  | DELETE of k
  | ASSERT of k * v option
  | ASSERT_EXISTS of k
  | ADMIN_SET of k * v option
  | USER_FUNCTION of string * string option
  | SEQUENCE of update list
  | DELETE_PREFIX of k

let update2s = function
  | SET (k, _) -> Printf.sprintf "U_SET (%S,_)" k
  | DELETE k -> Printf.sprintf "U_DEL (%S)" k
  | ASSERT (k, _) -> Printf.sprintf "U_ASSERT (%S,_)" k
  | ASSERT_EXISTS (k) -> Printf.sprintf "U_ASSERT_EXISTS (%S,_)" k
  | ADMIN_SET (k, _) -> Printf.sprintf "U_ADMINSET (%S,_)" k
  | USER_FUNCTION(n,po) -> Printf.sprintf "U_USER_FUNCTION(%S,_)" n
  | SEQUENCE s -> Printf.sprintf "U_SEQ (...)"
  | DELETE_PREFIX k -> Printf.sprintf "U_DELETE_PREFIX %S" k

let rec update_to buf = function
  | SET (k,v) ->
    Llio.int_to buf 1;
    Llio.string_to buf k;
    Llio.string_to buf v
  | DELETE k ->
    Llio.int_to buf 2;
    Llio.string_to buf k
  | SEQUENCE s ->
    Llio.int_to buf 5;
    Llio.int_to buf (List.length s);
    List.iter (fun u -> update_to buf u) s
  | ASSERT (k, m_v) ->
    Llio.int_to buf 8;
    Llio.string_to buf k ;
    Llio.string_option_to buf m_v
  | ADMIN_SET (k, m_v) ->
    Llio.int_to buf 9;
    Llio.string_to buf k;
    Llio.string_option_to buf m_v
  | DELETE_PREFIX k ->
    Llio.int_to buf 14;
    Llio.string_to buf k
  | ASSERT_EXISTS (k) ->
    Llio.int_to buf 15;
    Llio.string_to buf k

let rec update_from buf off =
  let kind, off = Llio.int_from buf off in
  match kind with
    | 1 ->
      let k, off = Llio.string_from buf off in
      let v, off = Llio.string_from buf off in
      SET(k,v), off
    | 2 ->
      let k, off = Llio.string_from buf off in
      DELETE k, off
    | 5 ->
      let slen, off = Llio.int_from buf off in
      begin
        let rec loop acc off = function
          | 0 -> acc, off
          | i -> 
            let u, off = update_from buf off in
            loop (u :: acc) off (i-1) 
        in
        let ups, off = loop [] off slen in
        SEQUENCE (List.rev ups), off
      end
    | 8 -> 
      let k, off = Llio.string_from buf off in
      let m_v, off = Llio.string_option_from buf off in
      ASSERT (k, m_v), off
    | 9 -> 
      let k, off = Llio.string_from buf off in
      let m_v, off = Llio.string_option_from buf off in
      ADMIN_SET (k, m_v), off
    | 14 ->
      let k, off = Llio.string_from buf off in
      DELETE_PREFIX k, off
    | 15 -> 
      let k, off = Llio.string_from buf off in
      ASSERT_EXISTS (k), off
    | i -> let msg = Printf.sprintf "Unknown update type %d" i in failwith msg

type result = 
  | VOID
  | VALUE of v
  | FAILURE of Arakoon_exc.rc * string


let result2s = function
  | VOID -> "VOID"
  | VALUE v -> Printf.sprintf "VALUE %s" v
  | FAILURE (rc,msg) -> Printf.sprintf "FAILURE(_,%S)" msg


type tick = TICK of int64
  
let (<:) (TICK i0) (TICK i1) = i0 < i1

let start_tick = TICK 0L
let next_tick (TICK i) = TICK (Int64.succ i)
let prev_tick (TICK i) = 
  begin 
    match i with
      | 0L -> start_tick
      | i -> TICK (Int64.pred i)
  end
    
let add_tick (TICK l) (TICK r) = TICK (Int64.add l r)
let tick2s (TICK t) = 
  Int64.to_string t


type tx_result = (v option, Arakoon_exc.rc * k) Baardskeerder.result

module type STORE = sig
  type t
  
  val create : string -> bool -> t Lwt.t
  val init : string -> unit Lwt.t
  val commit : t -> tick -> unit Lwt.t
  val log : t -> bool -> update -> tx_result Lwt.t
  val get : t -> k -> v option Lwt.t
  val admin_get: t -> k -> v option Lwt.t
  
  val range: t -> string option -> bool -> string option -> bool -> int option 
    -> string list Lwt.t
  val range_entries: t -> string option -> bool -> string option -> bool -> int option 
    -> (string*string) list Lwt.t
  val rev_range_entries: t -> string option -> bool -> string option -> bool -> int option
    -> (string*string) list Lwt.t

  val admin_prefix_keys: t -> string -> k list Lwt.t
  val prefix_keys: t -> string -> int option -> string list Lwt.t
  val is_read_only: t -> bool

  val last_entries: t -> tick -> Lwtc.oc -> unit Lwt.t
  val last_update: t -> (tick * update option) option Lwt.t
  val get_key_count : t -> int Lwt.t
  val get_meta: t -> string option Lwt.t
  val set_meta: t -> string -> unit Lwt.t
  val close : t -> unit Lwt.t
  val dump : t -> unit Lwt.t
  val raw_dump : t -> Lwtc.oc -> unit Lwt.t
end

(* output_action & input action are only in last_entries *)

let output_action (oc:Lwtc.oc) (action:Baardskeerder.action) = 
  let (>>=) = Lwt.(>>=) in
  match action with
    | Baardskeerder.Set (k,v) -> 
      begin
        Lwt_io.write_char oc 's' >>= fun () ->
        Lwtc.log "set : %s" k >>= fun () -> 
        Llio.output_string oc k >>= fun () ->
        Llio.output_string oc v 
      end
    | Baardskeerder.Delete k  -> 
      begin
        Lwt_io.write_char oc 'd' >>= fun () ->
        Lwtc.log "del : %s" k >>= fun () ->
        Llio.output_string oc k 
      end

let input_action (ic:Lwtc.ic) = 
  let (>>=) = Lwt.(>>=) in
  Lwt_io.read_char ic >>= function
    | 's' -> 
      Llio.input_string ic >>= fun k ->
      Llio.input_string ic >>= fun v ->
      Lwt.return (Baardskeerder.Set(k,v))
    | 'd' ->
      Llio.input_string ic >>= fun k ->
      Lwt.return (Baardskeerder.Delete k)
    | c -> Llio.lwt_failfmt "input_action '%C' does not yield an action" c

let extract_master_info = function
  | None -> None
  | Some s -> let m, _ = Llio.string_option_from s 0 
              in m
              

module BS = Baardskeerder.Baardskeerder(Baardskeerder.Logs.Flog0)(Baardskeerder.Stores.Lwt)

let __prefix = "@"
let __admin_prefix = "*"

let pref_key ?(_pf = __prefix) k = _pf ^ k
let unpref_key ?(_pf = __prefix) k = 
  let to_cut = String.length _pf in
  String.sub k to_cut ((String.length k) - to_cut)
