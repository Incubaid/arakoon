(*
This file is part of Arakoon, a distributed key-value store. Copyright
(C) 2010-2014 Incubaid BVBA

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

open Update

type t =
  | Vc of (Update.t list * bool) (* is_synced *)
  | Vm of (string * float)

let create_client_value (us:Update.t list) (synced:bool) = Vc (us, synced)
let create_master_value (m,l) = Vm (m,l)

let is_master_set  = function
  | Vm _ ->  true
  | _    -> false

let is_synced = function
  | Vm _ -> false
  | Vc (_,s) -> s

let clear_self_master_set me = function
  | Vm (m,l) when m = me -> Vm(m, 0.0)
  | v        -> v

let fill_if_master_set = function
  | Vm (m,_) -> let now = Unix.gettimeofday () in
    Vm(m,now)
  | v -> v

let updates_from_value = function
  | Vc (us,s)     -> us
  | Vm (m,l)      -> [Update.MasterSet(m,l)]

let value_to buf v=
  let () = Llio.int_to buf 0xff in
  match v with
    | Vc (us,synced)     ->
      Llio.char_to buf 'c';
      Llio.bool_to buf synced;
      Llio.list_to buf Update.to_buffer us
    | Vm (m,l) ->
      begin
        Llio.char_to buf 'm';
        Llio.string_to buf m;
        Llio.int64_to buf (Int64.of_float l)
      end

let value_from b =
  let pos = Llio.buffer_pos b in
  let i0 = Llio.int_from b in
  if i0 = 0xff
  then
    let c = Llio.char_from b in
    match c with
      | 'c' ->
        let synced = Llio.bool_from b  in
        let us     = Llio.list_from b Update.from_buffer in
        let r = Vc(us,synced) in
        r
      | 'm' -> let m = Llio.string_from b in
        let l = Llio.int64_from b in
        Vm (m,Int64.to_float l)
      | _ -> failwith "demarshalling error"
  else
    begin
      (* this is for backward compatibility:
         formerly, we logged updates iso values *)
      let () = Llio.buffer_set_pos b pos in
      let u = Update.from_buffer b in
      let synced = Update.is_synced u in
      let r = Vc ([u], synced) in
      r
    end



let value2s ?(values=false) = function
  | Vc (us,synced)  ->
    let uss = Log_extra.list2s (fun u -> Update.update2s u ~values) us in
    Printf.sprintf "(Vc (%s,%b)" uss synced
  | Vm (m,l)        -> Printf.sprintf "(Vm (%s,%f))" m l
