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

module ClientCfg = struct
  type node_address = string list * int
  type t = (string, node_address) Hashtbl.t

  let cfg_to buf (t:t) =
    Llio.hashtbl_to Llio.string_to (Llio.pair_to Llio.string_list_to Llio.int_to) buf t

  let cfg_from buf  =
    let entry_from buf =
      let k   = Llio.string_from buf in
      let ips = Llio.string_list_from buf in
      let p   = Llio.int_from buf in
      let (na:node_address) = ips,p in
      (k,na)
    in
    Llio.hashtbl_from entry_from buf

  let to_string t =
    let buffer = Buffer.create 127 in
    Hashtbl.iter (fun s (ips,p) ->
        let ipss = Printf.sprintf "[%s]" (String.concat ";" ips) in
        Buffer.add_string buffer (Printf.sprintf "(%s,(%s,%i))" s ipss p)) t;
    Buffer.contents buffer

  let input_cfg ic =
    let key_from ic =
      Llio.input_string ic
    in
    let value_from ic =
      Llio.input_string_list ic >>= fun ips ->
      Llio.input_int ic >>= fun port ->
      Lwt.return (ips,port)

    in
    Llio.input_hashtbl key_from value_from ic

  let output_cfg oc cfg =
    let helper oc key value =
      Llio.output_string oc key >>= fun () ->
      let (ip,port) = value in
      Llio.output_string oc ip >>= fun () ->
      Llio.output_int oc port
    in
    Llio.output_hashtbl helper oc cfg


  let node_names (t:t) = Hashtbl.fold (fun k _v acc -> k::acc) t []
  let make () = Hashtbl.create 7
  let add (t:t) name sa = Hashtbl.add t name sa
  let get (t:t) name = Hashtbl.find t name


  let from_ini section inifile =
    let cfg = make () in
    let _ips node_name = Ini.get inifile node_name "ip" Ini.p_string_list Ini.required in
    let _get s n p = Ini.get inifile s n p Ini.required in
    let nodes      = _get section "cluster" Ini.p_string_list in
    let () = List.iter
               (fun n ->
                  let ips = _ips n in
                  let port = _get n "client_port" Ini.p_int in
                  add cfg n (ips,port)
               ) nodes
    in
    cfg

  let _from_txt section txt =
    let inifile = new Arakoon_inifiles.inifile txt in
    from_ini section inifile

end
