open Lwt
module PQ = struct
  type 'a q = { mutable xs : 'a Queue.t; c: unit Lwt_condition.t;}
  let create () = { xs = Queue.create() ; c = Lwt_condition.create ()}


  let rec pull q = 
    if Queue.is_empty q.xs 
    then Lwt_condition.wait q.c >>= fun () -> pull q
    else let x = Queue.take q.xs in Lwt.return x

  let push q x = 
    let () = Queue.add x q.xs in
    Lwt_condition.broadcast q.c ()

  let length q = Queue.length q.xs

  let is_empty q = length q = 0

  let wait_for q = Lwt_condition.wait q.c

  let pop q = Queue.take q.xs

end
