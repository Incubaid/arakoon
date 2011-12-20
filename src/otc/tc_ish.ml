
module type TC_ISH = sig
  type t
  type db
  type cursor

  val set : db -> string -> string -> unit 
  val get : db -> string -> string
  val delete_val : db -> string -> unit
  val range: db -> string option -> bool -> string option -> bool -> int -> string array
  val prefix_keys: db -> string -> int -> string array
  val get_key_count: db -> int64
  val transaction : t -> (db -> 'a Lwt.t) -> 'a Lwt.t
    
  val create: ?mode:int -> string -> t Lwt.t 
  val open_t: t -> int -> unit
    
  val read : t -> (db -> 'a Lwt.t) -> 'a Lwt.t
  val get_bdb: t -> db 
    
  val delete :t -> unit Lwt.t
  val close  :t -> unit Lwt.t
  val filename : t -> string
    
  val reopen: t -> (unit -> unit Lwt.t) -> int -> unit Lwt.t
    
  val first : db -> cursor -> unit
  val next: db -> cursor -> unit
  val prev: db -> cursor -> unit
  val last: db -> cursor -> unit

  val key:db -> cursor -> string
  val value :db -> cursor -> string

  val with_cursor: db -> (db -> cursor -> 'a Lwt.t) -> 'a Lwt.t
    
  val batch : t -> int -> string -> string option -> (string * string) list Lwt.t

end

