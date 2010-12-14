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

open Message

module MPMessage = struct
  type n = Sn.t
  type t =
    | Prepare of n * n (* n, i *)
    | Promise of n * n * Value.t option
    | Accept of n * n * Value.t
    | Accepted of n * n
    | Nak of n * (n *n) (* n,(n',i') *)

  let string_of = function
    | Prepare (n,i) -> 
      "Prepare(" ^ (Sn.string_of n) ^", " ^ (Sn.string_of i) ^ ")"
    | Promise (n,i,v) ->
      let ns = Sn.string_of n
      and is = Sn.string_of i
      and vs = match v with
	| None -> "None"
	| Some s -> "Some ..."
      in
      "Promise(" ^ ns ^ "," ^ is ^ "," ^ vs ^ ")"
    | Accept (n,i, v) ->
      let ns = Sn.string_of n
      and is = Sn.string_of i in
      Printf.sprintf "Accept(%s,%s, ...)" ns is
    | Accepted (n,i) ->
      let ns = Sn.string_of n
      and is = Sn.string_of i in
      Printf.sprintf "Accepted(%s,%s)" ns is
    | Nak (n,(n',i')) ->
      Printf.sprintf "Nak (%s,(%s,%s))"
	(Sn.string_of n) (Sn.string_of n') (Sn.string_of i')

  let generic_of = function
    | Prepare (n,i) ->
      let buf = Buffer.create 20 in
      Sn.sn_to buf n;
      Sn.sn_to buf i;
      Message.create "prepare" (Buffer.contents buf)
    | Promise (n,i, vo) ->
      let buf = Buffer.create 20 in
      Sn.sn_to buf n;
      Sn.sn_to buf i;
      Llio.option_to Value.value_to buf vo;
      Message.create "promise" (Buffer.contents buf)
    | Accept(n,i , v) ->
      let buf = Buffer.create 20 in
      Sn.sn_to buf n;
      Sn.sn_to buf i;
      Value.value_to buf v;
      Message.create "accept" (Buffer.contents buf)
    | Accepted(n,i) ->
      let buf = Buffer.create 20 in
      Sn.sn_to buf n;
      Sn.sn_to buf i;
      Message.create "accepted" (Buffer.contents buf)
    | Nak (n,(n',i')) ->
      let buf = Buffer.create 20 in
      Sn.sn_to buf n;
      Sn.sn_to buf n';
      Sn.sn_to buf i';
      Message.create "nak" (Buffer.contents buf)

  let of_generic m =
    let kind, payload = Message.kind_of m, Message.payload_of m in
      match kind with
	| "prepare" -> 
	  let n, pos1 = Sn.sn_from payload 0 in
	  let i, _    = Sn.sn_from payload pos1 in
	  Prepare (n,i)
	| "nak"     ->
	  let n, pos1 = Sn.sn_from payload 0 in
	  let n', pos2 = Sn.sn_from payload pos1 in
	  let i', _ = Sn.sn_from payload pos2 in
	  Nak (n,(n',i'))
	| "promise" ->
	  let n, pos1 = Sn.sn_from payload 0 in
	  let i, pos2 = Sn.sn_from payload pos1 in
	  let v, pos3 = Llio.option_from Value.value_from payload pos2
	  in Promise (n,i, v)
	| "accept"  ->
	  let n, pos1 = Sn.sn_from payload 0 in
	  let i, pos2 = Sn.sn_from payload pos1 in
	  let s, pos2 = Value.value_from payload pos2 in
          Accept(n, i,s)
	| "accepted" ->
	  let n, pos1 = Sn.sn_from payload 0 in
	  let i, _ = Sn.sn_from payload pos1 in
          Accepted(n,i)
	| s -> failwith (s ^":not implemented")
end
