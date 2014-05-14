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

#include <sys/resource.h>

#include <caml/mlvalues.h>
#include <caml/bigarray.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/custom.h>
#include <caml/fail.h>


CAMLprim value arakoon_get_rlimit(value v_resource, value soft) {
  CAMLparam2(v_resource,soft);

  int res = 0;
  struct rlimit rlim;

  res = getrlimit(Int_val(v_resource), &rlim);

  if (res != 0) {
    caml_failwith("get_rlimit");
  }

  if (Int_val(soft) == 0) {
    res = rlim.rlim_cur;
  } else {
    res = rlim.rlim_max;
  }

  CAMLreturn(Val_int(res));
}

CAMLprim value arakoon_get_maxrss(value unit) {
  CAMLparam1 (unit);

  int res = 0, who = RUSAGE_SELF;
  struct rusage usage;

  res = getrusage(who, &usage);

  if (res != 0){
    caml_failwith ("get_rusage");
  }

  CAMLreturn(Val_int(usage.ru_maxrss));
}
