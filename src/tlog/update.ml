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
open Interval
open Routing

module Update = struct
  type t =
    | Set of string * string
    | Delete of string
    | MasterSet of string * int64
    | TestAndSet of string * string option * string option
    | Sequence of t list
    | SetInterval of Interval.t
    | SetRouting of Routing.t
    | SetRoutingDelta of (string * string * string)
    | Nop
    | Assert of string * string option
    | Assert_exists of string
    | UserFunction of string * string option
    | AdminSet of string * string option
    | SyncedSequence of t list 
    | DeletePrefix of string

  let make_master_set me maybe_lease =
    match maybe_lease with
      | None -> MasterSet (me,0L)
      | Some lease -> MasterSet (me,lease)

  let _size_of = function
    | None -> 0
    | Some w -> String.length w

  let update2s ?(values=false) u = 
    let maybe s = if values then s else "..." 
    and maybe_o = function 
      | None -> "None"
      | Some s -> if values then s else "..." 
    in 
    let rec _inner = function
      | Set (k,v)                 -> Printf.sprintf "Set            ;%S;%i;%S" k (String.length v) (maybe v)
      | Delete k                  -> Printf.sprintf "Delete         ;%S" k
      | MasterSet (m,i)           -> Printf.sprintf "MasterSet      ;%S;%Ld" m i
      | TestAndSet (k, _, wo) -> 
        let ws = _size_of wo in      Printf.sprintf "TestAndSet     ;%S;%i;%S" k ws (maybe_o wo)
      | Sequence updates ->
        let buf = Buffer.create (64 * List.length updates) in
        let () = Buffer.add_string buf "Sequence([" in
        let rec loop = function
	      | [] -> Buffer.add_string buf "])"
	      | u::us ->
	        let () = Buffer.add_string buf (_inner u) in
	        let () = if (us <> []) then Buffer.add_string buf "; "
	        in
	        loop us
        in
        let () = loop updates in
        Buffer.contents buf
      | SetInterval range ->      Printf.sprintf "SetInterval     ;%s" (Interval.to_string range)
      | SetRouting routing ->     Printf.sprintf "SetRouting      ;%s" (Routing.to_s routing)
      | SetRoutingDelta (left,sep,right) -> Printf.sprintf "SetRoutingDelta %s < '%s' <= %s" left sep right
      | Nop -> "NOP"
      | Assert (key,vo)        -> Printf.sprintf "Assert          ;%S;%i" key (_size_of vo)
      | Assert_exists (key)    -> Printf.sprintf "Assert_exists   ;%S"    key
      | UserFunction (name,param) ->
        let ps = _size_of param in
        Printf.sprintf "UserFunction;%s;%i" name ps
      | AdminSet (key,vo)      -> Printf.sprintf "AdminSet        ;%S;%i;%S" key (_size_of vo) (maybe_o vo)
      | SyncedSequence updates -> Printf.sprintf "SyncedSequence  ;..."
      | DeletePrefix prefix    -> Printf.sprintf "DeletePrefix    ;%S" prefix
    in
    _inner u
  
  let rec to_buffer b t =
    let _us_to b us = 
      Llio.int_to b (List.length us);
      let f = to_buffer b in
      List.iter f us
    in
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
        _us_to b us
      | Nop -> 
	    Llio.int_to b 6
      | UserFunction (name, param) ->
	    Llio.int_to b 7;
	    Llio.string_to b name;
	    Llio.string_option_to b param
      | Assert (k,vo) ->
	    Llio.int_to b 8;
	    Llio.string_to b k;
	    Llio.string_option_to b vo
      | AdminSet(k,vo) ->
        Llio.int_to b 9;
        Llio.string_to b k;
        Llio.string_option_to b vo;
      | SetInterval interval ->
	    Llio.int_to b 10;
	    Interval.interval_to b interval
      | SetRouting r ->
	    Llio.int_to b 11;
	    Routing.routing_to b r
      | SetRoutingDelta (l,s,r) ->
        Llio.int_to b 12;
        Llio.string_to b l;
        Llio.string_to b s;
        Llio.string_to b r
      | SyncedSequence us ->
        Llio.int_to b 13;
        _us_to b us
      | DeletePrefix prefix ->
        Llio.int_to b 14;
        Llio.string_to b prefix
      | Assert_exists (k) ->
	    Llio.int_to b 15;
	    Llio.string_to b k


  let rec from_buffer b pos =
    let kind, pos1 = Llio.int_from b pos in
    let _us_from b pos = 
      let n,pos2 = Llio.int_from b pos in
      let rec loop i acc pos=
        if i = n 
        then 
          (List.rev acc), pos
        else
          let u, next_pos = from_buffer b pos in
          loop (i+1) (u::acc) next_pos in
      loop 0 [] pos2
    in
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
        let us, pos2 = _us_from b pos1 in
        Sequence us, pos2
      | 6 -> Nop, pos1
      | 7 ->
	    let n,  pos2 = Llio.string_from b pos1 in
	    let po, pos3 = Llio.string_option_from b pos2 in
	    let r = UserFunction(n,po) in
	    r, pos3
      | 8 ->
	    let k, pos2 = Llio.string_from b pos1 in
	    let vo,pos3 = Llio.string_option_from b pos2 in
	    Assert (k,vo) , pos3
      | 9 ->
        let k,pos2 = Llio.string_from b pos1 in
        let vo, pos3 = Llio.string_option_from b pos2 in
        AdminSet(k,vo), pos3
      | 10 -> 
	    let interval,pos2 = Interval.interval_from b pos1 in
	    SetInterval interval, pos2
      | 11 ->
	    let r,pos2 = Routing.routing_from b pos1 in
	    SetRouting r, pos2
      | 12 ->
        let l, pos2 = Llio.string_from b pos1 in
        let s, pos3 = Llio.string_from b pos2 in
        let r, pos4 = Llio.string_from b pos3 in
        SetRoutingDelta (l, s, r), pos4
      | 13 ->
        let us, pos2 = _us_from b pos1 in
        SyncedSequence us, pos2
      | 14 ->
        let p, pos2 = Llio.string_from b pos1 in
        DeletePrefix p, pos2
      | 15 ->
	    let k, pos2 = Llio.string_from b pos1 in
	    Assert_exists (k) , pos2
      | _ -> failwith (Printf.sprintf "%i:not an update" kind)

  let is_synced = function
    | SyncedSequence _ -> true
    | _ -> false

end
