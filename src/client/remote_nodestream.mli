open Update
class type nodestream = object
  method iterate: 
    Sn.t -> (Sn.t * Update.t -> unit Lwt.t) ->
    Tlogcollection.tlog_collection ->
    head_saved_cb:(string -> unit Lwt.t) -> unit Lwt.t
end

val make_remote_nodestream : 
  string -> Lwt_io.input_channel * Lwt_io.output_channel -> nodestream Lwt.t
