open Lwt
open Registry

let s2i = int_of_string
let i2s = string_of_int

let update_max db po =
  let _k = "max" in
  let v =
    try let s = db#get _k in s2i s
    with Not_found -> 0
  in
  let v' = match po with
    | None -> 0
    | Some s ->
      try s2i s
      with _ -> raise (Arakoon_exc.Exception(Arakoon_exc.E_UNKNOWN_FAILURE, "invalid arg"))
  in
  let m = max v v' in
  let ms = i2s m in
  db#set _k ms;
  Some (i2s m)




let () = Registry.register "update_max" update_max
