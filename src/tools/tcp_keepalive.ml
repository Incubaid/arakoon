(*
Copyright 2015 iNuron NV

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*)

type socket = Unix.file_descr

type t = {
    (* see http://www.tldp.org/HOWTO/html_single/TCP-Keepalive-HOWTO/#usingkeepalive
     * for the meaning of these values
     *)
    enable_tcp_keepalive : bool;
    tcp_keepalive_time : int;
    tcp_keepalive_intvl : int;
    tcp_keepalive_probes : int;
  }

let default_tcp_keepalive = {
    enable_tcp_keepalive = true;
    tcp_keepalive_time = 20;
    tcp_keepalive_intvl = 20;
    tcp_keepalive_probes = 3;
  }

external set_tcp_keepalive_time : socket -> int -> unit = "arakoon_tcp_set_tcp_keepalive_time"
external set_tcp_keepalive_intvl : socket -> int -> unit = "arakoon_tcp_set_tcp_keepalive_intvl"
external set_tcp_keepalive_probes : socket -> int -> unit = "arakoon_tcp_set_tcp_keepalive_probes"

let apply fd t =
  if t.enable_tcp_keepalive
  then
    begin
      Lwt_unix.setsockopt fd Unix.SO_KEEPALIVE true;
      let unix_fd = Unix_fd.lwt_unix_fd_to_unix_fd fd in
      set_tcp_keepalive_time unix_fd t.tcp_keepalive_time;
      set_tcp_keepalive_intvl unix_fd t.tcp_keepalive_intvl;
      set_tcp_keepalive_probes unix_fd t.tcp_keepalive_probes;
    end
