#include <stdio.h>
#include <stdint.h>
#include <caml/memory.h>
#include <caml/mlvalues.h>

value Caml_int64_to_buffer(value v_buf,
			   value v_pos,
			   value v_i64){
  CAMLparam3(v_buf, v_pos, v_i64);
  char * buf = String_val(v_buf);
  int pos = Int_val(v_pos);
  int64_t i64 = Int64_val(v_i64);
  char* buf_off = &buf[pos];
  int64_t* casted = (int64_t*) buf_off;
  casted[0] = i64;
  CAMLreturn(Val_unit);
}
