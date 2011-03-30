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

module Statistics = struct
  type t ={ 
    start: float (* start of operations (timestamp) *);
    mutable last:  float (* last operation (timestamp)      *);
    mutable avg_set_size:float (* size of values *);
    mutable avg_get_size:float;
    mutable n_sets:int;
    mutable n_gets:int;
    mutable n_deletes:int;
    mutable n_multigets:int;
    mutable n_sequences:int;
    mutable node_is:(string , Sn.t) Hashtbl.t;
  }
 
  let get_witnessed t =
      t.node_is
 
  let create () = 
    {start = Unix.gettimeofday();
     last  = Unix.gettimeofday();
     avg_set_size=0.0;
     avg_get_size=0.0;
     n_sets = 0;
     n_gets = 0;
     n_deletes = 0;
     n_multigets = 0;
     n_sequences = 0;
     node_is = Hashtbl.create 5;
    }

  let _clock t = t.last <- Unix.gettimeofday()

  let new_set t (key:string) (value:string) = 
    _clock t;
    let n' = t.n_sets + 1 in
    let nf' = float n' in
    t.n_sets <- n';
    let size = float(String.length value) in
    t.avg_set_size <- t.avg_set_size +.  ((size -. t.avg_set_size) /. nf')

  let new_get t (key:string) (value:string) = 
    _clock t;
    let n' = t.n_gets + 1 in
    let nf' = float n' in
    t.n_gets <- n';
    let size = float(String.length value) in
    t.avg_get_size <- t.avg_get_size +. ((size -. t.avg_get_size) /. nf')

  let new_delete t =
    _clock t;
    t.n_deletes <- t.n_deletes + 1

  let new_sequence t = 
    _clock t;
    t.n_sequences <- t.n_sequences + 1

  let new_multiget t = 
    _clock t;
    t.n_multigets <- t.n_multigets + 1

  let witness t name i =
    Hashtbl.replace t.node_is name i

  let last_witnessed t name = 
    if Hashtbl.mem t.node_is name 
    then Hashtbl.find t.node_is name
    else Sn.of_int (-1000)
      
  let to_buffer b t =
    Llio.float_to b t.start;
    Llio.float_to b t.last;
    Llio.float_to b t.avg_set_size;
    Llio.float_to b t.avg_get_size;
    Llio.int_to   b t.n_sets;
    Llio.int_to   b t.n_gets;
    Llio.int_to   b t.n_deletes;
    Llio.int_to   b t.n_multigets;
    Llio.int_to   b t.n_sequences;
    Llio.int_to   b (Hashtbl.length t.node_is);
    Hashtbl.iter 
      (fun n i -> 
	Llio.string_to b n;
	Sn.sn_to b i;
      )  t.node_is

      

  let from_buffer buffer pos =
    let start,pos2         = Llio.float_from buffer pos   in
    let last, pos3         = Llio.float_from buffer pos2  in
    let avg_set_size,pos4  = Llio.float_from buffer pos3  in
    let avg_get_size,pos5  = Llio.float_from buffer pos4  in
    let n_sets, pos6       = Llio.int_from   buffer pos5  in
    let n_gets, pos7       = Llio.int_from   buffer pos6  in
    let n_deletes, pos8    = Llio.int_from   buffer pos7  in
    let n_multigets, pos9  = Llio.int_from   buffer pos8  in
    let n_sequences, pos10 = Llio.int_from   buffer pos9  in
    let n_pairs, pos11     = Llio.int_from   buffer pos10 in

    let node_is = Hashtbl.create 5 in
    let rec build j pos=
      if j = 0 then () 
      else
	let name,p = Llio.string_from buffer pos in
	let i,pos' = Sn.sn_from buffer p in
	let () = Hashtbl.replace node_is name i in
	build (j-1) pos' 
    in
    let () = build n_pairs pos11
    in
    let s = {start = start;
	     last = last;
	     avg_set_size = avg_set_size;
	     avg_get_size = avg_get_size;
	     n_sets = n_sets;
	     n_gets = n_gets;
	     n_deletes = n_deletes;
	     n_multigets = n_multigets;
	     n_sequences  = n_sequences;
	     node_is = node_is;
	    } 
    in
    s, pos10

  let string_of t =
    let template = 
      "{start: %f, " ^^
	"last: %f, " ^^
	"avg_set_size: %f, " ^^
	"avg_get_size: %f, " ^^
	"n_sets: %i, " ^^
	"n_gets: %i, " ^^
	"n_deletes: %i, " ^^
	"n_multigets: %i, " ^^
	"n_sequences: %i, "  ^^
	"node_is: %s" ^^
	"}"
    in
    let node_iss = Buffer.create 100 in
    let () = Hashtbl.fold (fun n i () ->
      Buffer.add_string node_iss (Printf.sprintf "(%s,%s)" n (Sn.string_of i)))
      t.node_is ()
    in
    Printf.sprintf template 
      t.start 
      t.last 
      t.avg_set_size 
      t.avg_get_size
      t.n_sets
      t.n_gets
      t.n_deletes
      t.n_multigets
      t.n_sequences
      (Buffer.contents node_iss)
end
