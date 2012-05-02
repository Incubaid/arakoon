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
  | SEQUENCE of update list

let update2s = function
  | SET (k,v) -> Printf.sprintf "U_SET (k: %s)" k
  | DELETE k -> Printf.sprintf "U_DEL (k: %s)" k
  | SEQUENCE s -> Printf.sprintf "U_SEQ (...)"

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
      
          
    | i -> failwith "Unknown update type"

type result = 
  | UNIT
  | FAILURE of Arakoon_exc.rc * string
  | VALUE of v

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

module type STORE = sig
  type t
  val create : string -> t Lwt.t
  val init : string -> unit Lwt.t
  val commit : t -> tick -> result Lwt.t
  val log : t -> bool -> update -> result Lwt.t
  val get : t -> k -> v option Lwt.t
  val range: t -> string option -> bool -> string option -> bool -> int -> string list Lwt.t
  val range_entries: t -> string option -> bool -> string option -> bool -> int -> (string*string) list Lwt.t
  val rev_range_entries: t -> string option -> bool -> string option -> bool -> int -> (string*string) list Lwt.t
  val last_entries: t -> tick -> Lwtc.oc -> unit Lwt.t
  val last_update: t -> (tick * update option) option Lwt.t
  val get_meta: t -> string option Lwt.t
  val set_meta: t -> string -> unit Lwt.t
  val close : t -> unit Lwt.t
  val dump : t -> unit Lwt.t
end

let output_action (oc:Lwtc.oc) (action:Baardskeerder.action) = 
  let (>>=) = Lwt.(>>=) in
  match action with
    | Baardskeerder.Set (k,v) -> 
      begin
        Lwt_io.write_char oc 's' >>= fun () ->
        Llio.output_string oc k >>= fun () ->
        Llio.output_string oc v 
      end
    | Baardskeerder.Delete k  -> 
      begin
        Lwt_io.write_char oc 'd' >>= fun () ->
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

