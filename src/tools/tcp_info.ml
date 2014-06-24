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

type u_int8_t = int
type u_int32_t = int

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

let to_string t =
  Printf.sprintf "{ state=%d; ca_state=%d; retransmits=%d; probes=%d; backoff=%d; options=%d; snd_wscale=%d; rcv_wscale=%d; rto=%d; ato=%d; snd_mss=%d; rcv_mss=%d; unacked=%d; sacked=%d; lost=%d; retrans=%d; fackets=%d; last_data_sent=%d; last_ack_sent=%d; last_data_recv=%d; last_ack_recv=%d; pmtu=%d; rcv_ssthresh=%d; rtt=%d; rttvar=%d; snd_ssthresh=%d; snd_cwnd=%d; advmss=%d; reordering=%d; rcv_rtt=%d; rcv_space=%d; total_retrans=%d; }"
    t.state t.ca_state t.retransmits t.probes t.backoff t.options t.snd_wscale
    t.rcv_wscale t.rto t.ato t.snd_mss t.rcv_mss t.unacked t.sacked t.lost
    t.retrans t.fackets t.last_data_sent t.last_ack_sent t.last_data_recv
    t.last_ack_recv t.pmtu t.rcv_ssthresh t.rtt t.rttvar t.snd_ssthresh
    t.snd_cwnd t.advmss t.reordering t.rcv_rtt t.rcv_space t.total_retrans

type socket = Unix.file_descr
external tcp_info : socket -> t = "arakoon_tcp_info"
