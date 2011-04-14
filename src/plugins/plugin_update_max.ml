open Lwt
open Otc

let s2i = int_of_string
let i2s = string_of_int

let update_max bdb po = 
  let _k = "max" in
  let v = 
    try let s = Bdb.get bdb _k in s2i s 
    with Not_found -> 0 
  in
  let v' = match po with
    | None -> 0
    | Some s -> s2i s 
  in
  let m = max v v' in
  let ms = i2s m in
  Bdb.put bdb _k ms;
  Some (i2s m)


open Registry

let () = Registry.register "update_max" update_max
