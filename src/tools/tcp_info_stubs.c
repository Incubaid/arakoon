/*
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
 */

#include <assert.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>

#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/mlvalues.h>
#include <caml/unixsupport.h>

value arakoon_tcp_info(value fd) {
        CAMLparam1(fd);
        CAMLlocal1(t);

        int c_fd = 0, ret = -1, idx = 0;
        socklen_t len = 0;
        struct tcp_info info;

        c_fd = Int_val(fd);
        len = sizeof(info);

        ret = getsockopt(c_fd, SOL_TCP, TCP_INFO, &info, &len);

        if(ret == -1) {
                uerror("getsockopt", Nothing);
        }

#define NUM_FIELDS 32

        t = caml_alloc(NUM_FIELDS, 0);

#define STORE(field) \
        do { \
                assert(idx < NUM_FIELDS); \
                Store_field(t, idx, Val_int(info.tcpi_ ##field )); \
                idx++; \
        } while(0)
        STORE(state);
        STORE(ca_state);
        STORE(retransmits);
        STORE(probes);
        STORE(backoff);
        STORE(options);
        STORE(snd_wscale);
        STORE(rcv_wscale);

        STORE(rto);
        STORE(ato);
        STORE(snd_mss);
        STORE(rcv_mss);

        STORE(unacked);
        STORE(sacked);
        STORE(lost);
        STORE(retrans);
        STORE(fackets);

        STORE(last_data_sent);
        STORE(last_ack_sent);
        STORE(last_data_recv);
        STORE(last_ack_recv);

        STORE(pmtu);
        STORE(rcv_ssthresh);
        STORE(rtt);
        STORE(rttvar);
        STORE(snd_ssthresh);
        STORE(snd_cwnd);
        STORE(advmss);
        STORE(reordering);

        STORE(rcv_rtt);
        STORE(rcv_space);

        STORE(total_retrans);

        assert(idx == NUM_FIELDS);

        CAMLreturn(t);
}
