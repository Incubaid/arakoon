(*
This file is part of Arakoon, a distributed key-value store.
Copyright (C) 2012 Incubaid BVBA

Licensees holding a valid Incubaid license may use this file in
accordance with Incubaid's Arakoon commercial license agreement. For
more information on how to enter into this agreement, please contact
Incubaid (contact details can be found on www.arakoon.org/licensing).

Alternatively, this file may be redistributed and/or modified under
the terms of the GNU Affero General Public License version 3, as
published by the Free Software Foundation. Under this license, this
file is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.

See the GNU Affero General Public License for more details.
You should have received a copy of the
GNU Affero General Public License along with this program (file "COPYING").
If not, see <http://www.gnu.org/licenses/>.
*)

(* Memcache protocol implementation based on
 * http://code.sixapart.com/svn/memcached/trunk/server/doc/protocol.txt r845
 *
 * This is not fully compatible (at all):
 * - Some commands are not implemented
 * - Some command arguments are not supported
 * - The server will close connections in error scenarios where memcached
 *   wouldn't
 *)

open Lwt

let _log f = Printf.kprintf Lwt_io.printl f


let response_get oc pairs =
  Lwt_list.iter_s
    (fun (k, v) ->
      Lwt_io.write oc
      (Printf.sprintf "VALUE %s 0 %u\r\n%s\r\n" k (String.length v) v))
    pairs
  >>= fun () ->
  Lwt_io.write oc "END\r\n" >>= fun () ->
  Lwt.return false

let response_set oc =
  Lwt_io.write oc "STORED\r\n" >>= fun () ->
  Lwt.return false

let response_delete oc success =
  Lwt_io.write oc (if success then "DELETED\r\n" else "NOT_FOUND\r\n") >>= fun () ->
  Lwt.return false

let response_version oc v =
  let s = Printf.sprintf "VERSION %s\r\n" v in
  Lwt_io.write oc s >>= fun () ->
  Lwt.return false


let handle_error oc =
  Lwt_io.write oc ("ERROR\r\n")
let handle_client_error oc e =
  Lwt_io.write oc ("CLIENT_ERROR " ^ e ^ "\r\n")
let handle_server_error oc e =
  Lwt_io.write oc ("SERVER_ERROR " ^ e ^ "\r\n")


let handle_exception oc exc =
  let msg = Printexc.to_string exc in
  Lwt_log.error_f "Memcache exception during client request: %s" msg >>= fun () ->
  handle_server_error oc msg >>= fun () ->
  Lwt.return true


type command = GET of string list
             | SET of string * string * bool
             | DELETE of string * bool
             | VERSION
             | QUIT
             | ERROR

let take n l =
  let rec loop acc n' = function
    | [] -> acc
    | (x :: xs) ->
        if n' = 0
        then acc
        else loop (x :: acc) (n' - 1) xs
  in
  List.rev (loop [] n l)

let read_command =
  let re = Str.regexp "[ \t]+" in
  fun (ic, oc) ->

  Lwt_io.read_line ic >>= fun line ->

  let res = Str.bounded_split re line 2 in

  match res with
    | [cmd] -> 
        begin
          match cmd with
            | "version" -> Lwt.return VERSION
            | "quit" -> Lwt.return QUIT
            | _ ->
                handle_error oc >>= fun () ->
                Lwt.return ERROR
        end
    | [cmd; args] ->
        begin
          match cmd with
            | "get" ->
                let keys = Str.split re args in
                Lwt.return (GET keys)
            | "set" ->
                begin
                  let parts = Str.split re args in
                  match List.length parts with
                    | 4 | 5 ->
                        let [key; flags; exptime; bytes] = take 4 parts in
                        let bytes' = Scanf.sscanf bytes "%u" (fun i -> i) in
                        let value = String.create bytes'
                        and tail = String.create 2 in
                        Lwt_io.read_into_exactly ic value 0 bytes' >>= fun () ->
                        Lwt_io.read_into_exactly ic tail 0 2 >>= fun () ->
                        if List.length parts = 4
                        then
                          Lwt.return (SET (key, value, false))
                        else
                          let nr = List.nth parts 4 in
                          if nr <> "noreply"
                          then
                            handle_client_error oc "Invalid 'noreply' option" >>= fun () ->
                            Lwt.return ERROR
                          else
                            Lwt.return (SET (key, value, true))
                    | _ ->
                        handle_client_error oc "Invalid number of arguments" >>= fun () ->
                        Lwt.return ERROR
                end
            | "delete" ->
                begin
                  let parts = Str.split re args in
                  match List.length parts with
                    | 1 ->
                        let [key] = parts in
                        Lwt.return (DELETE (key, false))
                    | 2 ->
                        let [key; time] = parts in
                        (
                          if time <> "0"
                          then
                            _log "Memcache: 'time' argument in delete call not supported"
                          else
                            Lwt.return ()
                        ) >>= fun () ->
                        Lwt.return (DELETE (key, false))
                    | 3 ->
                        let [key; time; nr] = parts in
                        (
                          if time <> "0"
                          then
                            _log "Memcache: 'time' argument in delete call not supported"
                          else
                            Lwt.return ()
                        ) >>= fun () ->
                        if nr <> "noreply"
                        then
                          handle_client_error oc "Invalid 'noreply' option" >>= fun () ->
                          Lwt.return ERROR
                        else
                          Lwt.return (DELETE (key, true))
                    | _ ->
                        handle_client_error oc "Invalid number of arguments" >>= fun () ->
                        Lwt.return ERROR
                end
            | _ ->
                handle_error oc >>= fun () ->
                Lwt.return ERROR
        end
