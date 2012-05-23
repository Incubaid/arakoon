open Lwt

open Userdb

let s2i = int_of_string
let i2s = string_of_int

let update_max tx po = 
  let _k = "max" in
  let vo2i = function
    | None -> 0
    | Some v -> s2i v
  in
  UserDB.get tx _k >>= fun vr ->
  let i2 = match vr with
    | Baardskeerder.OK v -> s2i v
    | Baardskeerder.NOK k -> 0
  in
  let i = vo2i po in
  let m  = max i i2 in
  let ms = i2s m in
  UserDB.set tx _k ms >>= fun () ->
  Lwt.return (Some ms)


let () = Userdb.Registry.register "update_max" update_max
