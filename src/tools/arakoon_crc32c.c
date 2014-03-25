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
#include <stdio.h>
#include <assert.h>
#include <caml/mlvalues.h>
#include <caml/bigarray.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include "bsd_crc32c.h"
#include "cpudetect.h"

// these are available externally
CAMLprim value calculate_crc32c(value buffer, value offset, value length);
CAMLprim value update_crc32c(value crc32c, value buffer, value offset, value length);

CAMLprim value calculate_crc32c(value buffer, value offset, value length)
{
  CAMLparam3(buffer,offset,length);
  const int32_t ilength = Int_val(length);
  const int32_t ioffset = Int_val(offset);
  const uint8_t * buffer2 = (const uint8_t *) String_val(buffer) + ioffset;
  uint32_t res = 0;
  if (has_sse_4_2) {
    res = sse4_2_crc32c(buffer2, ilength);
  } else {
    res = bsd_calculate_crc32c(buffer2, ilength);
  }
  CAMLreturn(caml_copy_int32(res));
}

CAMLprim value update_crc32c(value crc32c, value buffer, value offset, value length)
{
  CAMLparam4(crc32c,buffer, offset, length);
  const int32_t ilength = Int_val(length);
  const int32_t ioffset = Int_val(offset);
  const uint8_t * buffer2 = (const uint8_t *) String_val(buffer) + ioffset;
  int32_t crc32c2 = Int32_val(crc32c);
  const uint32_t res = bsd_update_crc32c(crc32c2, buffer2, ilength);
  CAMLreturn(caml_copy_int32(res));
}
