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

type x_stats ={
    mutable n : int;
    mutable min:float;
    mutable max:float;
    mutable m2: float;
    mutable avg:float;
    mutable var:float;
  }

let x_stats_to_string (stats:x_stats) :string =
    let to_str_init_max = function
      | x when x = max_float -> "n/a"
      | x -> string_of_float x
    in
    let to_str_init_zero = function
      | 0.0 -> "n/a"
      | x -> string_of_float x
    in
    Printf.sprintf "(n:%i min: %s, max: %s, avg: %s, dev: %s)"
      stats.n
      (to_str_init_max stats.min)
      (to_str_init_zero stats.max)
      (to_str_init_zero stats.avg)
      (to_str_init_zero (sqrt stats.var))

let x_stats_from (buffer:string) (offset:int) : (x_stats * int) =
  let n,  o1 = Llio.int_from buffer offset in
  let min,o2 = Llio.float_from buffer o1 in
  let max,o3 = Llio.float_from buffer o2 in
  let m2 ,o4 = Llio.float_from buffer o3 in
  let avg,o5 = Llio.float_from buffer o4 in
  let var,o6 = Llio.float_from buffer o5 in
  ( {n;min;max;m2;avg;var;}, o6)

let x_stats_to_value_list (stats:x_stats) (list_name:string) : Llio.namedValue =
    let l = [
      Llio.NAMED_INT ("n", stats.n);
      Llio.NAMED_FLOAT ("min", stats.min);
      Llio.NAMED_FLOAT ("max", stats.max);
      Llio.NAMED_FLOAT ("m2", stats.m2);
      Llio.NAMED_FLOAT ("avg", stats.avg);
      Llio.NAMED_FLOAT ("var", stats.var)
    ] in
    Llio.NAMED_VALUELIST (list_name, l)

let create_x_stats () = {
    n = 0;
    min = max_float;
    max = 0.0;
    m2 = 0.0;
    avg = 0.0;
    var = 0.0;
}

let update_x_stats (t:x_stats) x =
  let n = t.n in
  let n' = n + 1 in
  t.n <- n';
  let nf' = float n' in
  if x < t.min then t.min <- x;
  if x > t.max then t.max <- x;
  let delta = x -. t.avg in
  let mean = t.avg in
  let mean' = mean +. (delta/. nf') in
  t.avg <- mean';
  if n' > 1 then
    begin
      let m2' = t.m2 +. delta *. (x -. mean') in
      t.m2 <- m2'
    end;
  let nf = float n in (* old size ! *)
  t.var <- t.m2 /. nf;

module Statistics = struct

  type t ={
    mutable start: float (* start of operations (timestamp) *);
    mutable last:  float (* last operation (timestamp)      *);

    mutable avg_set_size:           float (* size of values *);
    mutable avg_get_size:           float;
    mutable avg_range_size:         float;
    mutable avg_range_entries_size: float;
    mutable avg_rev_range_entries_size :float;
    mutable avg_prefix_size:float;
    mutable avg_del_prefix_size:float;
    mutable harvest_stats: x_stats;
    mutable set_time_stats:           x_stats;
    mutable get_time_stats:           x_stats;
    mutable del_time_stats:           x_stats;
    mutable seq_time_stats:           x_stats;
    mutable mget_time_stats:          x_stats;
    mutable mget_option_time_stats:   x_stats;
    mutable tas_time_stats:           x_stats;
    mutable range_time_stats:         x_stats;
    mutable range_entries_time_stats: x_stats;
    mutable rev_range_entries_time_stats: x_stats;
    mutable prefix_time_stats:        x_stats;
    mutable delete_prefix_time_stats: x_stats;
    mutable op_time_stats:            x_stats;
    mutable mem_allocated:            float;
    mutable mem_maxrss:                int;
    mutable mem_minor_collections :    int;
    mutable mem_major_collections :    int;
    mutable mem_compactions :          int;
    mutable node_is:          (string , Sn.t) Hashtbl.t;
  }

  let get_witnessed t =
      t.node_is

  let create () =
    {start = Unix.gettimeofday();
     last  = Unix.gettimeofday();
     avg_set_size             = 0.0;
     avg_get_size             = 0.0;
     avg_range_size           = 0.0;
     avg_range_entries_size   = 0.0;
     avg_rev_range_entries_size = 0.0;
     avg_prefix_size          = 0.0;
     avg_del_prefix_size      = 0.0;
     set_time_stats           = create_x_stats();
     get_time_stats           = create_x_stats();
     del_time_stats           = create_x_stats();
     seq_time_stats           = create_x_stats();
     mget_time_stats          = create_x_stats();
     mget_option_time_stats   = create_x_stats();
     tas_time_stats           = create_x_stats();
     range_time_stats         = create_x_stats();
     range_entries_time_stats = create_x_stats(); 
     rev_range_entries_time_stats     = create_x_stats();
     prefix_time_stats        = create_x_stats();
     delete_prefix_time_stats = create_x_stats();
     op_time_stats            = create_x_stats();
     mem_allocated            = 0.0;
     mem_maxrss= 0;
     mem_minor_collections= 0;
     mem_major_collections= 0;
     mem_compactions= 0;
     node_is = Hashtbl.create 5;
     harvest_stats = create_x_stats ();
    }

  let clear_most t =
    begin
      t.start <- Unix.gettimeofday();
      t.last  <- Unix.gettimeofday();
      t.avg_set_size           <- 0.0;
      t.avg_get_size           <- 0.0;
      t.avg_range_size         <- 0.0;
      t.avg_range_entries_size <- 0.0;
      t.avg_prefix_size        <- 0.0;
      t.avg_del_prefix_size    <- 0.0;
      t.set_time_stats           <- create_x_stats();
      t.get_time_stats           <- create_x_stats();
      t.del_time_stats           <- create_x_stats();
      t.seq_time_stats           <- create_x_stats();
      t.mget_time_stats          <- create_x_stats();
      t.tas_time_stats           <- create_x_stats();
      t.range_time_stats         <- create_x_stats();
      t.range_entries_time_stats <- create_x_stats();
      t.rev_range_entries_time_stats <- create_x_stats();
      t.prefix_time_stats        <- create_x_stats();
      t.delete_prefix_time_stats <- create_x_stats();
      t.op_time_stats            <- create_x_stats();
      t.harvest_stats            <- create_x_stats ();
    end

  let _clock t start =
    t.last <- Unix.gettimeofday();
    t.last -. start

  let new_op t start =
    let x = _clock t start in
    update_x_stats t.op_time_stats x ;
    x


  let new_set t (_key:string) (value:string) (start:float)=
    let x = new_op t start in
    update_x_stats t.set_time_stats x;
    let size = float(String.length value) in
    let n' = t.set_time_stats.n in
    let nf' = float n' in
    t.avg_set_size <- t.avg_set_size +.  ((size -. t.avg_set_size) /. nf')

  let new_harvest t n = update_x_stats t.harvest_stats (float n)



  let new_get t (_key:string) (value:string) (start:float) =
    let x = new_op t start in
    update_x_stats t.get_time_stats x;
    let size = float(String.length value) in
    let n' = t.get_time_stats.n in
    let nf' = float n' in
    t.avg_get_size <- t.avg_get_size +. ((size -. t.avg_get_size) /. nf')

  let new_delete t (start:float)=
    let x = new_op t start in
    update_x_stats t.del_time_stats x

  let new_sequence t (start:float)=
    let x = new_op t start in
    update_x_stats t.seq_time_stats x

  let new_multiget t (start:float)=
    let x = new_op t start in
    update_x_stats t.mget_time_stats x

  let new_multiget_option t (start:float) =
    let x = new_op t start in
    update_x_stats t.mget_option_time_stats x

  let new_testandset t (start:float)=
    let x = new_op t start in
    update_x_stats t.tas_time_stats x

  let new_prefix_keys t (start:float) n_keys =
    let x = new_op t start in
    update_x_stats t.prefix_time_stats x;
    let n = t.prefix_time_stats.n in
    let nf = float n in
    t.avg_prefix_size <- t.avg_prefix_size +. ((float n_keys -. t.avg_prefix_size) /. nf)

  let new_range t (start:float) n_keys =
    let x = new_op t start in
    update_x_stats t.range_time_stats x;
    let n = t.range_time_stats.n in
    let nf = float n in
    t.avg_range_size <- t.avg_range_size +. ((float n_keys -. t.avg_range_size) /. nf)

  let new_range_entries t (start:float) n_keys = 
    let x = new_op t start in
    update_x_stats t.range_entries_time_stats x;
    let n = t.range_entries_time_stats.n in
    let nf = float n in
    t.avg_range_entries_size <- t.avg_range_entries_size +. ((float n_keys -. t.avg_range_entries_size) /. nf)
 
  let new_rev_range_entries t (start:float) n_keys = 
    let x = new_op t start in
    update_x_stats t.rev_range_entries_time_stats x;
    let n = t.rev_range_entries_time_stats.n in
    let nf = float n in
    t.avg_rev_range_entries_size <- t.avg_rev_range_entries_size +. ((float n_keys -. t.avg_rev_range_entries_size) /. nf)

  let new_delete_prefix t (start:float) n_keys =
    let x = new_op t start in
    update_x_stats t.delete_prefix_time_stats x;
    let n = t.delete_prefix_time_stats.n in
    let nf = float n in
    t.avg_del_prefix_size <- t.avg_del_prefix_size +. ((float n_keys -. t.avg_del_prefix_size) /. nf)

  let witness t name i =
    Hashtbl.replace t.node_is name i

  let last_witnessed t name =
    if Hashtbl.mem t.node_is name
    then Hashtbl.find t.node_is name
    else Sn.of_int (-1000)

  let to_buffer b t =

    let node_is =
      Hashtbl.fold
        (fun n i l -> (Llio.NAMED_INT64 (n, i)) :: l )
        t.node_is []
    in

    let value_list = [
      Llio.NAMED_FLOAT ("start", t.start);
      Llio.NAMED_FLOAT ("last", t.last);
      Llio.NAMED_FLOAT ("avg_get_size", t.avg_set_size);
      Llio.NAMED_FLOAT ("avg_set_size", t.avg_get_size);
      Llio.NAMED_FLOAT ("avg_range_size", t.avg_range_size);
      Llio.NAMED_FLOAT ("avg_range_entries_size", t.avg_range_entries_size);
      Llio.NAMED_FLOAT ("avg_rev_range_entries_size", t.avg_rev_range_entries_size);
      Llio.NAMED_FLOAT ("avg_prefix_size", t.avg_prefix_size);
      Llio.NAMED_FLOAT ("avg_del_prefix_size", t.avg_del_prefix_size);
      x_stats_to_value_list t.harvest_stats "harvest_stats";
      x_stats_to_value_list t.set_time_stats "set_info";
      x_stats_to_value_list t.get_time_stats "get_info";
      x_stats_to_value_list t.del_time_stats "del_info";
      x_stats_to_value_list t.seq_time_stats "seq_info";
      x_stats_to_value_list t.mget_time_stats "mget_info";
      x_stats_to_value_list t.mget_option_time_stats "mget_option_info";
      x_stats_to_value_list t.tas_time_stats "tas_info";

      x_stats_to_value_list t.range_time_stats "range_info";
      x_stats_to_value_list t.range_entries_time_stats "range_entries_info";
      x_stats_to_value_list t.rev_range_entries_time_stats "rev_range_entries_info";
      x_stats_to_value_list t.prefix_time_stats "prefix_info";
      x_stats_to_value_list t.delete_prefix_time_stats "delete_prefix_info";

      x_stats_to_value_list t.op_time_stats "op_info";
      Llio.NAMED_FLOAT ("mem_allocated", t.mem_allocated);
      Llio.NAMED_INT ("mem_maxrss", t.mem_maxrss);
      Llio.NAMED_INT ("mem_minor_collections", t.mem_minor_collections);
      Llio.NAMED_INT ("mem_major_collections", t.mem_major_collections);
      Llio.NAMED_INT ("mem_compactions", t.mem_compactions);
      Llio.NAMED_VALUELIST ("node_is", node_is);
    ] in

    Llio.named_field_to b (Llio.NAMED_VALUELIST ("arakoon_stats", value_list))


  let from_buffer buffer pos =
    let n_value_list,pos = Llio.named_field_from buffer pos in

    let extract_next (l :Llio.namedValue list) : (Llio.namedValue * Llio.namedValue list) =
      match l with
        | [] -> failwith "Not enough elements in named value list"
        | hd :: tl ->
          hd, tl
    in

    let extract_list = function
      | Llio.NAMED_VALUELIST (_,l) -> l
      | _ -> failwith "Wrong value type (expected list)"
    in
    let extract_float (value:Llio.namedValue) : float =
      begin
      match value with
        | Llio.NAMED_FLOAT (_,f) -> f
        | _ -> failwith "Wrong value type (expected float)"
      end
    in
    let extract_int = function
      | Llio.NAMED_INT (_,i) -> i
      | _ -> failwith "Wrong value type (expected int)"
    in
    let extract_x_stats (value:Llio.namedValue) : x_stats =
      begin
      match value with
	    | Llio.NAMED_VALUELIST (_,l) ->
            let v, l = extract_next l in
            let n    = extract_int v in
            let v, l = extract_next l in
	        let min = extract_float v in
            let v, l = extract_next l in
            let max = extract_float v in
            let v,l = extract_next  l in
            let m2  = extract_float v in
            let v, l = extract_next l in
            let avg = extract_float v in
            let v, _l = extract_next l in
            let var = extract_float v in
	        {n; min; max; m2; avg; var;}
	    | _ -> failwith "Wrong value type (expected list)"
      end
    in

    let v_list = extract_list n_value_list in
    let value, v_list = extract_next v_list in
    let start = extract_float value in
    let value, v_list = extract_next v_list in
    let last = extract_float value in
    let value, v_list = extract_next v_list in
    let avg_set_size = extract_float value in

    let value, v_list = extract_next v_list in
    let avg_get_size = extract_float value in

    let value, v_list = extract_next v_list in
    let avg_range_size = extract_float value in
    
    let value, v_list = extract_next v_list in
    let avg_range_entries_size = extract_float value in

    let value, v_list = extract_next v_list in
    let avg_rev_range_entries_size =extract_float value in


    let value, v_list = extract_next v_list in
    let avg_prefix_size = extract_float value in

    let value, v_list = extract_next v_list in
    let avg_del_prefix_size = extract_float value in

    let value, v_list = extract_next v_list in
    let harvest_stats = extract_x_stats value in


    let value, v_list = extract_next v_list in
    let set_stats   = extract_x_stats value in

    let value, v_list = extract_next v_list in
    let get_stats   = extract_x_stats value in

    let value, v_list = extract_next v_list in
    let del_stats   = extract_x_stats value in

    let value, v_list = extract_next v_list in
    let seq_stats   = extract_x_stats value in

    let value, v_list = extract_next v_list in
    let mget_stats  = extract_x_stats value in

    let value, v_list = extract_next v_list in
    let mget_option_stats = extract_x_stats value in

    let value, v_list = extract_next v_list in
    let tas_stats   = extract_x_stats value in


    let value, v_list = extract_next v_list in
    let range_stats = extract_x_stats value in

    let value, v_list = extract_next v_list in
    let range_entries_stats = extract_x_stats value in

    let value, v_list = extract_next v_list in
    let rev_range_entries_stats = extract_x_stats value in

    let value, v_list = extract_next v_list in
    let prefix_stats = extract_x_stats value in

    let value, v_list = extract_next v_list in
    let delete_prefix_stats = extract_x_stats value in

    let value, v_list = extract_next v_list in
    let op_stats    = extract_x_stats value in

    let value, v_list = extract_next v_list in
    let mem_allocated = extract_float value in

    let value, v_list = extract_next v_list in
    let mem_maxrss  = extract_int value in

    let value, v_list = extract_next v_list in
    let mem_minor_collections  = extract_int value in

    let value, v_list = extract_next v_list in
    let mem_major_collections  = extract_int value in

    let value, v_list = extract_next v_list in
    let mem_compactions  = extract_int value in

    let value, _v_list = extract_next v_list in
    let node_list = extract_list value in

    let node_is = Hashtbl.create 5 in
    let insert_node value =
      match value with
        | Llio.NAMED_INT64(n,i) -> Hashtbl.replace node_is n i
        | _ -> failwith "Wrong value type (expected int64)."
    in
    List.iter insert_node node_list;
    let t =  {
      start = start;
      last = last;
      avg_set_size = avg_set_size;
      avg_get_size = avg_get_size;
      avg_range_size = avg_range_size;
      avg_range_entries_size = avg_range_entries_size;
      avg_rev_range_entries_size = avg_rev_range_entries_size;
      avg_prefix_size = avg_prefix_size;
      avg_del_prefix_size = avg_del_prefix_size;
      harvest_stats = harvest_stats;
      set_time_stats = set_stats;
      get_time_stats = get_stats;
      del_time_stats = del_stats;
      seq_time_stats = seq_stats;
      mget_time_stats = mget_stats;
      mget_option_time_stats = mget_option_stats;
      tas_time_stats = tas_stats;
      range_time_stats = range_stats;
      range_entries_time_stats = range_entries_stats;
      rev_range_entries_time_stats = rev_range_entries_stats;
      prefix_time_stats = prefix_stats;
      delete_prefix_time_stats = delete_prefix_stats;
      op_time_stats = op_stats;
      mem_allocated;
      mem_maxrss;
      mem_minor_collections;
      mem_major_collections;
      mem_compactions;
      node_is = node_is;
    } in
    t, pos

  let string_of t =
    let template =
      "{start: %f, " ^^
	"last: %f,\n" ^^
        "avg_set_size: %f,\n" ^^
        "avg_get_size: %f,\n" ^^
        "avg_range_size: %f,\n" ^^
        "avg_range_entries_size: %f,\n" ^^
        "avg_rev_range_entries_size: %f,\n" ^^
        "avg_prefix_size: %f,\n" ^^
        "avg_del_prefix_size: %f,\n" ^^
        "harvest_stats: %s,\n" ^^
        "set_info: %s,\n" ^^
        "get_info: %s,\n" ^^
        "del_info: %s,\n" ^^
	"mget_info: %s,\n" ^^
        "mget_option_info: %s\n" ^^
	"seq_info: %s,\n" ^^
        "tas_info: %s,\n" ^^
        "range_info: %s,\n" ^^
        "range_entries_info: %s,\n" ^^
        "rev_range_entries_info: %s,\n" ^^
        "prefix_info: %s,\n" ^^
        "delete_prefix_info: %s,\n" ^^
        "ops_info: %s,\n" ^^
        "mem_allocated: %f KB,\n" ^^
        "mem_maxrss: %i KB,\n" ^^
        "mem_minor_collections: %i,\n" ^^
        "mem_major_collections: %i,\n" ^^
        "mem_compactions: %i,\n" ^^
        "node_is: %s" ^^
	"}\n"
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
      t.avg_range_size
      t.avg_range_entries_size
      t.avg_rev_range_entries_size
      t.avg_prefix_size
      t.avg_del_prefix_size
      (x_stats_to_string t.harvest_stats)
      (x_stats_to_string t.set_time_stats)
      (x_stats_to_string t.get_time_stats)
      (x_stats_to_string t.del_time_stats)
      (x_stats_to_string t.mget_time_stats)
      (x_stats_to_string t.mget_option_time_stats)
      (x_stats_to_string t.seq_time_stats)
      (x_stats_to_string t.tas_time_stats)
      (x_stats_to_string t.range_time_stats)
      (x_stats_to_string t.range_entries_time_stats)
      (x_stats_to_string t.rev_range_entries_time_stats)
      (x_stats_to_string t.prefix_time_stats)
      (x_stats_to_string t.delete_prefix_time_stats)
      (x_stats_to_string t.op_time_stats)
      t.mem_allocated
      t.mem_maxrss
      t.mem_minor_collections
      t.mem_major_collections
      t.mem_compactions
      (Buffer.contents node_iss)
end
