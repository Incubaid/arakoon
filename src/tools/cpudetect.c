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

#include <stdio.h>

#define cpuid(func,ax,bx,cx,dx)					\
  __asm__ __volatile__ ("cpuid":				\
			"=a" (ax), "=b" (bx), "=c" (cx), "=d" (dx) : "a" (func));

int has_sse_2 = 0;
int has_sse_3 = 0;
int has_sse_4_2 = 0;

void __cpu_detect(void) __attribute__((constructor));

void __cpu_detect(void)
{
  int a,b,c,d;
  cpuid(1, a, b, c, d);

  has_sse_2 = ((1 << 26) & d) != 0;
  has_sse_3 = ((1 << 0) & c) != 0;
  has_sse_4_2 = ((1 << 20) & c) != 0;

#if 0
  if (has_sse_2) {
    fprintf(stderr, "SSE2 detected.\n");
  }
  if (has_sse_3) {
    fprintf(stderr, "SSE3 detected.\n");
  }
  if (has_sse_4_2) {
    fprintf(stderr, "SSE4.2 detected. %x\n", c);
  }
#endif

}

#include <caml/mlvalues.h>
#include <caml/bigarray.h>
#include <caml/alloc.h>
#include <caml/memory.h>

value ml_has_sse2(value unit)
{
  CAMLparam1(unit);
  CAMLreturn(has_sse_2?Val_true:Val_false);
}

value ml_has_sse3(value unit)
{
  CAMLparam1(unit);
  CAMLreturn(has_sse_3?Val_true:Val_false);
}

value ml_has_sse4_2(value unit)
{
  CAMLparam1(unit);
  CAMLreturn(has_sse_4_2?Val_true:Val_false);
}
