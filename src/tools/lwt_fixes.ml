open Lwt


let with_lock l f =
    Lwt_mutex.with_lock l ( fun () -> Lwt_unix.yield () >>= fun () -> f () )