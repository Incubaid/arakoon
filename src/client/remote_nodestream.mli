open Update
open Routing
class type nodestream = object
  method iterate: 
    Sn.t -> (Sn.t * Update.t -> unit Lwt.t) ->
    Tlogcollection.tlog_collection ->
    head_saved_cb:(string -> unit Lwt.t) -> unit Lwt.t
      
  method collapse: int -> unit Lwt.t

  method set_routing: Routing.t -> unit Lwt.t
  method get_routing: unit -> Routing.t Lwt.t
  
  method get_db: string -> unit Lwt.t
end

val make_remote_nodestream : 
  string -> Lwt_io.input_channel * Lwt_io.output_channel -> nodestream Lwt.t
