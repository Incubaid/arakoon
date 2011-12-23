open Lwt
open Btree_ish

module BS_btree = (struct

  type t = { bs : Baardskeerder.t; fn: string}
  type db = Baardskeerder.t
  type cursor = int

  let set db k v =
    let action tx = Baardskeerder.set tx k v in
    Baardskeerder.with_tx db action

  let get db k = 
    try
      Baardskeerder.get_latest db k
    with 
      | Baardskeerder.NOT_FOUND _ -> raise Not_found
      | e -> let () = Printf.printf "XXX: %s\n" (Printexc.to_string e) in raise e

  let delete_val db k = 
    let action tx = Baardskeerder.delete tx k in
    Baardskeerder.with_tx db action

  let range db fo fi lo li max = failwith "todo"
  let prefix_keys db prefix max = failwith "todo"
  let get_key_count db = failwith "todo"
  let first db c = failwith "todo"
  let next db c = failwith "todo"
  let prev db c = failwith "todo"
  let last db c = failwith "todo"
  let key db c = failwith "todo"
  let value db c = failwith "todo"

  let create ?(mode=0) fn = 
    let () = if not (Sys.file_exists fn) then Baardskeerder.init fn in
    let bs = Baardskeerder.make fn in
    Lwt.return {bs; fn}
  
  let open_t t i = failwith "open_t"

  let _lwt_fail s = Lwt.fail (Failure s) 

  let read t (f:db -> 'a) = f t.bs 

  let get_bdb t = t.bs
    
  let delete t = _lwt_fail "delete"
  let close t = _lwt_fail "close"
  let filename t = t.fn
  let reopen t when_closed mode = _lwt_fail "reopen"


  let with_cursor db f = _lwt_fail "with_cursor"
  let batch db  batch_size prefix start = _lwt_fail "batch"
  let transaction (t:t) (f:db -> 'a) = 
    Lwt_log.debug "transaction" >>= fun () ->
    Lwt.catch
      (fun () -> f t.bs)
      (fun e -> Lwt.fail e)




end: BTREE_ISH)
