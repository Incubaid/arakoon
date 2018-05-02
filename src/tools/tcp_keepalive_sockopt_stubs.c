/*
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
 */

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>

#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/mlvalues.h>
#include <caml/unixsupport.h>

CAMLprim value arakoon_tcp_set_tcp_sock_opt(value fd, value v, int option) {
  CAMLparam2(fd, v);

  int c_fd = Int_val(fd);
  int c_v  = Int_val(v);

  int ret = setsockopt(c_fd, SOL_TCP, option, &c_v, sizeof(c_v));

  if(ret == -1) {
    uerror("arakoon_tcp_set_tcp_sock_opt", Nothing);
  }

  CAMLreturn(Val_unit);
}

CAMLprim value arakoon_tcp_set_tcp_keepalive_time(value fd, value v) {
  return arakoon_tcp_set_tcp_sock_opt(fd, v, TCP_KEEPIDLE);
}

CAMLprim value arakoon_tcp_set_tcp_keepalive_intvl(value fd, value v) {
  return arakoon_tcp_set_tcp_sock_opt(fd, v, TCP_KEEPINTVL);
}

CAMLprim value arakoon_tcp_set_tcp_keepalive_probes(value fd, value v) {
  return arakoon_tcp_set_tcp_sock_opt(fd, v, TCP_KEEPCNT);
}
