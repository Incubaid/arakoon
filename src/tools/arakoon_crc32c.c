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

#include <cpuid.h>

#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/mlvalues.h>

#include "bsd_crc32c.h"

/* CPU SSE4.2 support detection routines */
int has_sse_4_2 = 0;

void __cpu_detect(void) __attribute__((constructor));

void __cpu_detect(void)
{
  unsigned int a = 0, b = 0, c = 0, d = 0, func = 1;
  int r = 0;

  r = __get_cpuid(func, &a, &b, &c, &d);
  if(r == 0) {
    /* Lookup failed */
    c = 0;
  }

  has_sse_4_2 = (c & bit_SSE4_2) != 0;
}

/* these are available externally */
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
  uint32_t res = 0;
  if (has_sse_4_2) {
    res = crc32c_sse4_2(crc32c2, buffer2, ilength);
  } else {
    res = bsd_update_crc32c(crc32c2, buffer2, ilength);
  }
  CAMLreturn(caml_copy_int32(res));
}
