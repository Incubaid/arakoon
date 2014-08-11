(*
Copyright (2010-2014) INCUBAID BVBA

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

(** Binding to C library calls to retrieve TCP socket information *)

type u_int8_t = int
type u_int32_t = int

(** Record of TCP socket information

This represents a {i struct tcp_info } value. *)
type t = { state : u_int8_t
         ; ca_state : u_int8_t
         ; retransmits : u_int8_t
         ; probes : u_int8_t
         ; backoff : u_int8_t
         ; options : u_int8_t
         ; snd_wscale : u_int8_t
         ; rcv_wscale : u_int8_t

         ; rto : u_int32_t
         ; ato : u_int32_t
         ; snd_mss : u_int32_t
         ; rcv_mss : u_int32_t

         ; unacked : u_int32_t
         ; sacked : u_int32_t
         ; lost : u_int32_t
         ; retrans : u_int32_t
         ; fackets : u_int32_t

         ; last_data_sent : u_int32_t
         ; last_ack_sent : u_int32_t
         ; last_data_recv : u_int32_t
         ; last_ack_recv : u_int32_t

         ; pmtu : u_int32_t
         ; rcv_ssthresh : u_int32_t
         ; rtt : u_int32_t
         ; rttvar : u_int32_t
         ; snd_ssthresh : u_int32_t
         ; snd_cwnd : u_int32_t
         ; advmss : u_int32_t
         ; reordering : u_int32_t

         ; rcv_rtt : u_int32_t
         ; rcv_space : u_int32_t

         ; total_retrans : u_int32_t
         }

(** Create a {! string } representation of a {! t } *)
let to_string t =
  let open To_string in
  record [ "state", int t.state
         ; "ca_state", int t.ca_state
         ; "retransmits", int t.retransmits
         ; "probes", int t.probes
         ; "backoff", int t.backoff
         ; "options", int t.options
         ; "snd_wscale", int t.snd_wscale
         ; "rcv_wscale", int t.rcv_wscale
         ; "rto", int t.rto
         ; "ato", int t.ato
         ; "snd_mss", int t.snd_mss
         ; "rcv_mss", int t.rcv_mss
         ; "unacked", int t.unacked
         ; "sacked", int t.sacked
         ; "lost", int t.lost
         ; "retrans", int t.retrans
         ; "fackets", int t.fackets
         ; "last_data_sent", int t.last_data_sent
         ; "last_ack_sent", int t.last_ack_sent
         ; "last_data_recv", int t.last_data_recv
         ; "last_ack_recv", int t.last_ack_recv
         ; "pmtu", int t.pmtu
         ; "rcv_ssthresh", int t.rcv_ssthresh
         ; "rtt", int t.rtt
         ; "rttvar", int t.rttvar
         ; "snd_ssthresh", int t.snd_ssthresh
         ; "snd_cwnd", int t.snd_cwnd
         ; "advmss", int t.advmss
         ; "reordering", int t.reordering
         ; "rcv_rtt", int t.rcv_rtt
         ; "rcv_space", int t.rcv_space
         ; "total_retrans", int t.total_retrans
         ]

type socket = Unix.file_descr

(** Retrieve TCP connection information from a {! socket }

This uses {i getsockopt } with {i SOL_TCP } and {i TCP_INFO } underneath. *)
external tcp_info : socket -> t = "arakoon_tcp_info"
