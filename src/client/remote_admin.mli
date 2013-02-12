
open Routing
open Interval
open Client_cfg
open Ncfg

class type admin = object
      
  method compact: int -> unit Lwt.t (* number of transactions to keep *)

  method set_routing: Routing.t -> unit Lwt.t
  method set_routing_delta: string -> string -> string -> unit Lwt.t
  method get_routing: unit -> Routing.t Lwt.t
  
  method get_db: string -> unit Lwt.t

  method get_fringe: string option -> Routing.range_direction -> ((string * string) list) Lwt.t
  method set_interval : Interval.t -> unit Lwt.t
  method get_interval : unit -> Interval.t Lwt.t
  
  method store_cluster_cfg: string -> ClientCfg.t -> unit Lwt.t
  
  method get_nursery_cfg: unit -> NCFG.t Lwt.t
  
end

val make : 
  string -> Lwt_io.input_channel * Lwt_io.output_channel -> admin Lwt.t