(*
This file is part of Arakoon, a distributed key-value store. Copyright
(C) 2010 Incubaid BVBA

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

open Log_extra

module Update = struct
  type t =
    | Set of string * string
    | Delete of string
    | MasterSet of string * int64
    | TestAndSet of string * string option * string option
    | Sequence of t list
    | Nop
    | Assert of string * string option

  let make_master_set me maybe_lease =
    match maybe_lease with
      | None -> MasterSet (me,0L)
      | Some lease -> MasterSet (me,lease)

  let _size_of = function
    | None -> 0
    | Some w -> String.length w

  let rec string_of = function
    | Set (k,v)             -> Printf.sprintf "Set       ;%S;%i" k (String.length v)
    | Delete k              -> Printf.sprintf "Delete    ;%S" k
    | MasterSet (m,i)       -> Printf.sprintf "MasterSet ;%S;%Ld" m i
    | TestAndSet (k, _, wo) -> Printf.sprintf "TestAndSet;%S;%i" k (_size_of wo)
    | Sequence updates ->
      let buf = Buffer.create (64 * List.length updates) in
      let () = Buffer.add_string buf "Sequence([" in
      let rec loop = function
	| [] -> Buffer.add_string buf "])"
	| u::us ->
	  let () = Buffer.add_string buf (string_of u) in
	  let () = if (us <> []) then Buffer.add_string buf "; "
	  in
	  loop us
      in
      let () = loop updates in
      Buffer.contents buf
    | Nop -> "NOP"
    | Assert (key,vo)       -> Printf.sprintf "Assert    ;%S;%i" key (_size_of vo) 

  let rec to_buffer b t =
    match t with
      | Set(k,v) ->
	Llio.int_to    b 1;
	Llio.string_to b k;
	Llio.string_to b v
      | Delete k ->
	Llio.int_to    b 2;
	Llio.string_to b k
      | TestAndSet (k,e,w) ->
	Llio.int_to    b 3;
	Llio.string_to b k;
	Llio.string_option_to b e;
	Llio.string_option_to b w
      | MasterSet (m,i) ->
	Llio.int_to    b 4;
	Llio.string_to b m;
	Llio.int64_to b i
      | Sequence us ->
	Llio.int_to b 5;
	Llio.int_to b (List.length us);
	let f = to_buffer b in
	List.iter f us
      | Nop -> 
	Llio.int_to b 6
      | Assert (k,vo) ->
	Llio.int_to b 8;
	Llio.string_to b k;
	Llio.string_option_to b vo



  let rec from_buffer b pos =
    let kind, pos1 = Llio.int_from b pos in
    match kind with
      | 1 -> 
        let k,pos2 = Llio.string_from b pos1 in
        let v,pos3 = Llio.string_from b pos2 in
        Set(k,v), pos3
      | 2 -> 
        let k, pos2 = Llio.string_from b pos1 in
		    Delete k, pos2
      | 3 ->
        let k, pos2 = Llio.string_from b pos1 in
        let e, pos3 = Llio.string_option_from b pos2 in
        let w, pos4 = Llio.string_option_from b pos3 in
        TestAndSet(k,e,w), pos4
      | 4 -> 
        let m,pos2 = Llio.string_from b pos1 in
        let i,pos3 = Llio.int64_from b pos2 in
        MasterSet (m,i), pos3
      | 5 ->
        let n,pos2 = Llio.int_from b pos1 in
        let rec loop i acc pos=
        if i = n 
        then 
          Sequence (List.rev acc), pos
        else
          let u, next_pos = from_buffer b pos in
          loop (i+1) (u::acc) next_pos in
          loop 0 [] pos2
      | 6 -> Nop, pos1
      | 8 ->
	let k, pos2 = Llio.string_from b pos1 in
	let vo,pos3 = Llio.string_option_from b pos2 in
	Assert (k,vo) , pos3
      | _ -> failwith (Printf.sprintf "%i:not an update" kind)


  let make_update_value update =
    let b = Buffer.create 64 in
    let () = to_buffer b update in
    let s = Buffer.contents b in
    Value.create s

  let is_master_set (Value.V buf) = 
    let kind,_ = Llio.int_from buf 0 in
    kind = 4


end
