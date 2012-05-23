open Lwt
open Registry

let s2i = int_of_string
let i2s = string_of_int

module UserDB = struct
  let get tx (k:string) = Lwt.return None
  let set tx (k:string) (v:string) = Lwt.return ()
end

let update_max tx po = 
  let _k = "max" in
  let vo2i = function
    | None -> 0
    | Some v -> s2i v
  in
  UserDB.get tx _k >>= fun vo ->
  let i = vo2i vo in
  let i2 = vo2i po in
  let m  = max i i2 in
  let ms = i2s m in
  UserDB.set tx _k ms >>= fun () ->
  Lwt.return (Some ms)


let () = Registry.register "update_max" update_max
