
(* calculate crc32_c buffer offset length -> crc32 *)

external calculate_crc32c : string -> int -> int -> int32 = "calculate_crc32c"

external update_crc32c : int32 -> string -> int -> int -> int32 = "update_crc32c"
