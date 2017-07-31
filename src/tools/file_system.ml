(*
Copyright (2010-2014) INCUBAID BVBA

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*)



open Lwt

let section = Logger.Section.main

let lwt_directory_list dn =
  Lwt.catch
    (fun () ->
      Lwt_unix.opendir dn >>= fun h ->
      let rec loop acc  =
        Lwt.catch
          (fun () -> Lwt_unix.readdir h >>= fun x -> Lwt.return (Some x))
          (function
            | End_of_file -> Lwt.return None
            | exn -> Lwt.fail exn
          )
        >>= function
        | None -> Lwt.return (List.rev acc)
        | Some x ->
           begin
            match x with
              | "." | ".." -> loop acc
              | s' -> loop (s' :: acc)
           end
      in
      Lwt.finalize
        (fun () -> loop [])
        (fun () -> Lwt_unix.closedir h)
    )
    (fun exn -> Logger.debug_f_ ~exn "lwt_directory_list %s" dn >>= fun () -> Lwt.fail exn)

let fsync_dir dir =
  let t0 = Unix.gettimeofday() in
  Lwt_unix.openfile dir [Unix.O_RDONLY] 0640 >>= fun dir_descr ->
  Lwt.finalize
    (fun () -> Lwt_unix.fsync dir_descr)
    (fun () -> Lwt_unix.close dir_descr
               >>= fun () ->
               let t1 = Unix.gettimeofday() in
               let d = t1 -. t0 in
               if d < 1.0
               then Lwt.return ()
               else Logger.warning_f_ "fsync_dir of %S took : %f" dir d
    )

let fsync_dir_of_file filename =
  fsync_dir (Filename.dirname filename)

let rename source target =
  Logger.info_f_ "rename %s -> %s" source target >>= fun () ->
  Lwt_unix.rename source target >>= fun () ->
  fsync_dir_of_file target

let mkdir name = Lwt_unix.mkdir name

let unlink ?(verbose = true) name =
  Lwt.catch
    (fun () ->
     Logger.info_f_ "Unlinking %S" name >>= fun () ->
     Lwt_unix.unlink name)
    (function
      | Unix.Unix_error(Unix.ENOENT, _, _) ->
         if verbose
         then Logger.info_f_ "Unlink of %S failed with ENOENT" name
         else Lwt.return_unit
      | e ->
         Lwt.fail e)

let rmdir name = Lwt_unix.rmdir name

let stat filename =
  Logger.debug_f_ "stat %S" filename >>= fun () ->
  Lwt_unix.stat filename

let exists filename =
  Lwt.catch
    (fun () -> stat filename >>= fun _ -> Lwt.return true)
    (function
      | Unix.Unix_error (Unix.ENOENT,_,_) -> Lwt.return false
      | e -> Lwt.fail e
    )

let with_tmp_file tmp dest f =
  Lwt_unix.openfile tmp [Unix.O_WRONLY; Unix.O_CREAT; Unix.O_EXCL] 0o644 >>= fun fd ->
  Lwt.finalize
    (fun () ->
     let mode = Lwt_io.output
     and close () = Lwt.return () in
     let oc = Lwt_io.of_fd ~mode ~close fd in

     f oc >>= fun () ->

     Lwt_io.close oc >>= fun () ->
     Lwt_unix.fsync fd >>= fun () ->
     rename tmp dest)
    (fun () -> Lwt_unix.close fd)

let with_fd filename ~flags ~perm f =
  Lwt_unix.openfile filename flags perm >>= fun fd ->
  Lwt.finalize
    (fun () -> f fd)
    (fun () -> Lwt_unix.close fd)

let copy_file source target ~overwrite ~throttling =
  (* LOOKS LIKE Clone.copy_stream ... *)
  let bs = 1024 * 1024 in
  Logger.info_f_ "copy_file %s %s (overwrite=%b,throttling=%f) buffer_size:%i"
                 source target overwrite throttling bs >>= fun () ->

  let throttle =
    match throttling with
    | 0.0   -> fun f  -> f ()
    | factor ->
       fun f ->
       let t0 = Unix.gettimeofday () in
       f () >>= fun r ->
       let t1 = Unix.gettimeofday () in
       let dt = t1 -. t0 in
       Lwt_unix.sleep (factor *. dt) >>= fun () ->
       Lwt.return r
  in
  let copy_all buffer ~fd_in ~fd_out =
    let rec loop () =
      throttle
        (fun () ->
         Lwt_bytes.read fd_in buffer 0 bs >>= fun bytes_read ->
         if bytes_read > 0
         then
           begin
             let rec inner offset todo =
               Lwt_bytes.write fd_out buffer offset todo >>= fun bytes_written ->
               let todo = todo - bytes_written in
               if todo > 0
               then inner (offset + bytes_written) todo
               else Lwt.return_unit
             in
             inner 0 bytes_read >>= fun () ->
             Lwt.return `Continue
           end
         else
           Lwt.return `Stop)
      >>= function
        | `Continue -> loop ()
        | `Stop -> Lwt.return_unit
    in
    loop () >>= fun () ->
    Logger.info_ "done: copy_file"
  in
  exists target >>= fun target_exists ->
  if target_exists && not overwrite
  then
    Logger.info_f_ "Not copying %s to %s because target already exists" source target >>= fun () ->
    Lwt.return_false
  else
    begin
      let tmp_file = target ^ ".tmp" in
      unlink tmp_file >>= fun () ->
      with_fd
        source
        ~flags:[ Unix.O_RDONLY; Unix.O_NONBLOCK; ]
        ~perm:0
        (fun fd_in ->
          with_fd
            tmp_file
            ~flags:[ Unix.O_WRONLY; Unix.O_CREAT; Unix.O_TRUNC; Unix.O_NONBLOCK; Unix.O_EXCL; ]
            ~perm:0o644
            (fun fd_out ->
              let buffer = Lwt_bytes.create bs in
              Lwt.finalize
                (fun () -> copy_all buffer ~fd_in ~fd_out)
                (fun () -> Core_kernel.Bigstring.unsafe_destroy buffer;
                           Lwt.return_unit)
              >>= fun () ->
              Lwt_unix.fsync fd_out
            )
        )
      >>= fun () ->
      rename tmp_file target >>= fun () ->
      Lwt.return_true
    end

let safe_rename src dest =
  Lwt_unix.stat src >>= fun ss ->
  Lwt_unix.stat dest >>= fun ds ->
  if ss.Lwt_unix.st_dev <> ds.Lwt_unix.st_dev
    then
      let msg = Printf.sprintf
        "File_system.safe_rename: src %S and dest %S on different filesystem"
        src dest
      in
      Lwt.fail (Failure msg)
    else
      (* Make sure this is the 'rename' from this module, not Lwt_unix.rename *)
      rename src dest
