/*
This file is part of Arakoon, a distributed key-value store. Copyright
(C) 2010-2014 Incubaid BVBA

Licensees holding a valid Incubaid license may use this file in
accordance with Incubaid's Arakoon commercial license agreement. For
more information on how to enter into this agreement, please contact
Incubaid (contact details can be found on www.arakoon.org/licensing).

Alternatively, this file may be redistributed and/or modified under
the terms of the GNU Affero General Public License version 3, as
published by the Free Software Foundation. Under this license, this
file is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.

See the GNU Affero General Public License for more details.
You should have received a copy of the
GNU Affero General Public License along with this program (file "COPYING").
If not, see <http://www.gnu.org/licenses/>.
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
