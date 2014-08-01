let serialize_string  b s = Llio.string_to b s
let serialize_hashtbl b f h = Llio.hashtbl_to b f h
let serialize_string_list b sl = Llio.string_list_to b sl

type input = Llio.buffer
let make_input s i = Llio.make_buffer s i

let deserialize_string i = Llio.string_from i
let deserialize_string_list i = Llio.string_list_from i
let deserialize_hashtbl i f = Llio.hashtbl_from i f

let generic_f f x =
  let k s = Lwt.ignore_result (f s) in
  Printf.kprintf k x

let debug_f x = generic_f Client_log.debug x
