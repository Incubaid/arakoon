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

open Interval
open Routing

module Update = struct
  type t =
    | Set of string * string
    | Delete of string
    | MasterSet of string * float
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
    | Replace of string * string option


  let make_master_set me maybe_lease =
    match maybe_lease with
      | None -> MasterSet (me,0.0)
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
      | MasterSet (m,i)           -> Printf.sprintf "MasterSet      ;%S;%f" m i
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
      | SyncedSequence _us     -> Printf.sprintf "SyncedSequence  ;..."
      | DeletePrefix prefix    -> Printf.sprintf "DeletePrefix    ;%S" prefix
      | Replace(k,vo) ->
         Printf.sprintf "Replace            ;%S;%i" k (_size_of vo)
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
      | MasterSet (m,_) ->
        Llio.int_to    b 4;
        Llio.string_to b m;
        Llio.int64_to b 0L
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
      | Replace (k,vo) ->
         Llio.int_to b 16;
         Llio.string_to b k;
         Llio.string_option_to b vo

  let rec from_buffer b =
    let kind = Llio.int_from b in
    let _us_from b =
      let n = Llio.int_from b in
      let rec loop i acc =
        if i = n
        then
          List.rev acc
        else
          let u  = from_buffer b in
          loop (i+1) (u::acc) in
      loop 0 []
    in
    match kind with
      | 1 ->
        let k = Llio.string_from b in
        let v = Llio.string_from b in
        Set(k,v)
      | 2 ->
        let k = Llio.string_from b in
        Delete k
      | 3 ->
        let k = Llio.string_from b in
        let e = Llio.string_option_from b in
        let w = Llio.string_option_from b in
        TestAndSet(k,e,w)
      | 4 ->
        let m = Llio.string_from b in
        let _ = Llio.int64_from b  in
        MasterSet (m,0.0)
      | 5 ->
        let us = _us_from b in
        Sequence us
      | 6 -> Nop
      | 7 ->
        let n  = Llio.string_from b in
        let po = Llio.string_option_from b in
        UserFunction(n,po)
      | 8 ->
        let k  = Llio.string_from b in
        let vo = Llio.string_option_from b in
        Assert (k,vo)
      | 9 ->
        let k  = Llio.string_from b in
        let vo = Llio.string_option_from b in
        AdminSet(k,vo)
      | 10 ->
         let interval = Interval.interval_from b in
        SetInterval interval
      | 11 ->
        let r = Routing.routing_from b in
        SetRouting r
      | 12 ->
        let l = Llio.string_from b in
        let s = Llio.string_from b in
        let r = Llio.string_from b in
        SetRoutingDelta (l, s, r)
      | 13 ->
        let us = _us_from b in
        SyncedSequence us
      | 14 ->
        let p = Llio.string_from b in
        DeletePrefix p
      | 15 ->
        let k= Llio.string_from b in
        Assert_exists (k)
      | 16 ->
         let k = Llio.string_from b in
         let vo = Llio.string_option_from b in
         Replace(k,vo)
      | _ -> failwith (Printf.sprintf "%i:not an update" kind)

  let is_synced = function
    | SyncedSequence _ -> true
    | Set _
    | Delete _
    | MasterSet _
    | TestAndSet _
    | Sequence _
    | SetInterval _
    | SetRouting _
    | SetRoutingDelta _
    | Nop
    | Assert _
    | Assert_exists _
    | UserFunction _
    | AdminSet _
    | DeletePrefix _
    | Replace _ -> false

end
