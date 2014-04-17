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


CAMLprim value arakoon_get_rlimit(value v_resource, value soft){

  CAMLparam2(v_resource,soft);
  struct rlimit rlim;

  int resource = Int_val(v_resource);
  int res = getrlimit(resource, &rlim);

  if (res != 0) {

    caml_failwith("get_rlimit");
  }
  int res2 = 0;
  if (Int_val(soft) == 0) {
    res2 = rlim.rlim_cur;
  } else {
    res2 = rlim.rlim_max;
  }
  CAMLreturn(Val_int(res2));
}

CAMLprim value arakoon_get_maxrss(value unit){
  CAMLparam1 (unit);
  int who = RUSAGE_SELF;
  struct rusage usage;
  int res = getrusage(who, &usage);
  if (res != 0){
    caml_failwith ("get_rusage");
  }
  int res2 = usage.ru_maxrss;
  CAMLreturn(Val_int(res2));
}
