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
    | Promise (n,i,vo) ->
      let ns = Sn.string_of n
      and is = Sn.string_of i
      and vs = match vo with
        | None -> "None"
        | Some v -> Printf.sprintf "Some %s" (Value.value2s v)
      in
      "Promise(" ^ ns ^ "," ^ is ^ "," ^ vs ^ ")"
    | Accept (n,i, v) ->
      let ns = Sn.string_of n
      and is = Sn.string_of i
      and vs = Value.value2s v in
      Printf.sprintf "Accept(%s,%s, %s)" ns is vs
    | Accepted (n,i) ->
      let ns = Sn.string_of n
      and is = Sn.string_of i in
      Printf.sprintf "Accepted(%s,%s)" ns is
    | Nak (n,(n',i')) ->
      Printf.sprintf "Nak (%s,(%s,%s))"
        (Sn.string_of n) (Sn.string_of n') (Sn.string_of i')

  let generic_of' buf = function
    | Prepare (n,i) ->
      Sn.sn_to buf n;
      Sn.sn_to buf i;
      Message.create "prepare" (Buffer.contents buf)
    | Promise (n,i, vo) ->
      Sn.sn_to buf n;
      Sn.sn_to buf i;
      Llio.option_to Value.value_to buf vo;
      Message.create "promise" (Buffer.contents buf)
    | Accept(n,i , v) ->
      Sn.sn_to buf n;
      Sn.sn_to buf i;
      Value.value_to buf v;
      Message.create "accept" (Buffer.contents buf)
    | Accepted(n,i) ->
      Sn.sn_to buf n;
      Sn.sn_to buf i;
      Message.create "accepted" (Buffer.contents buf)
    | Nak (n,(n',i')) ->
      Sn.sn_to buf n;
      Sn.sn_to buf n';
      Sn.sn_to buf i';
      Message.create "nak" (Buffer.contents buf)

  let generic_of m =
    let buf = Buffer.create 20 in
    generic_of' buf m


  let of_generic m =
    let kind, payload = Message.kind_of m, Message.payload_of m in
    let buf = Llio.make_buffer payload 0 in
    match kind with
      | "prepare" ->
        let n = Sn.sn_from buf in
        let i = Sn.sn_from buf in
        Prepare (n,i)
      | "nak"     ->
        let n = Sn.sn_from buf in
        let n' = Sn.sn_from buf in
       let i' = Sn.sn_from buf in
        Nak (n,(n',i'))
      | "promise" ->
        let n = Sn.sn_from buf in
        let i = Sn.sn_from buf in
        let v = Llio.option_from Value.value_from buf in
        Promise (n,i, v)
      | "accept"  ->
        let n = Sn.sn_from buf in
        let i = Sn.sn_from buf in
        let s = Value.value_from buf in
        Accept(n, i,s)
      | "accepted" ->
        let n = Sn.sn_from buf in
        let i = Sn.sn_from buf in
        Accepted(n,i)
      | s -> failwith (s ^":not implemented")
end
