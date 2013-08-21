#include <stdio.h>
#include <stdint.h>
#include <caml/memory.h>
#include <caml/mlvalues.h>
#include <caml/alloc.h>

value Caml_int64_to_buffer(value v_buf,
			   value v_pos,
			   value v_i64){
   CAMLparam3(v_buf, v_pos, v_i64);
    /* the above macro is not needed as we can't trigger gc.*/
  char * buf = String_val(v_buf);
  int pos = Int_val(v_pos);
  int64_t i64 = Int64_val(v_i64);
  char* buf_off = &buf[pos];
  int64_t* casted = (int64_t*) buf_off;
  casted[0] = i64;
  CAMLreturn(Val_unit);
}

value Caml_int32_from(value v_buf,
		      value v_pos
		     ){
    char* buf = String_val(v_buf);
    int pos = Int_val(v_pos);
    char* buf_off = &buf[pos];
    int32_t* i = (int32_t*) buf_off;
    int32_t i32 = *i;
    return caml_copy_int32(i32);
}
