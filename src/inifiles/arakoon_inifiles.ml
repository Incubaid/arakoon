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

(* A small library to read and write .ini files

   Copyright (C) 2004 Eric Stokes, and The California State University
   at Northridge

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with this library; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*)

(*
   changes to original (Romain Slootmaekers):
   - removed pcre dependency
   - fixed little bug on parsing of empty lines with spaces
   - little change to fold signature to accomodate ocaml 4.0
   - removed IO from ini file parsing so it can be used with Lwt too
*)


exception Invalid_section of string
exception Invalid_element of string
exception Missing_section of string
exception Missing_element of string
exception Ini_parse_error of (int * string)

type attribute_specification = {
  atr_name: string;
  atr_required: bool;
  atr_default: (string list) option;
  atr_validator: Str.regexp option;
}

type section_specification = {
  sec_name: string;
  sec_required: bool;
  sec_attributes: attribute_specification list;
}

type specification = section_specification list

let comment = Str.regexp "^#.*$"

module Ordstr =
struct
  type t = string
  let compare (x:t) (y:t) =
    String.compare (String.lowercase_ascii x) (String.lowercase_ascii y)
end

module Strset = Set.Make(Ordstr)

let setOfList list =
  let rec dosetOfList list set =
    match list with
        a :: tail -> dosetOfList tail (Strset.add a set)
      | []  -> set
  in
  dosetOfList list Strset.empty

let rec filterfile ?(buf=Buffer.create 500) f fd =
  try
    let theline = input_line fd in
    if f theline then begin
      Buffer.add_string buf (theline ^ "\n");
      filterfile ~buf f fd
    end
    else
      filterfile ~buf f fd
  with End_of_file -> Buffer.contents buf

let parse_inifile txt tbl =
  let lxbuf =
    let lines = Str.split (Str.regexp "\n") txt in
    let lines' = List.filter (fun line -> not (Str.string_match comment line 0)) lines in
    let buf = Buffer.create (String.length txt) in
    let () =List.iter
      (fun line -> Buffer.add_string buf line;
                   Buffer.add_char buf '\n'
      ) lines' in
    let txt' = Buffer.contents buf in
    Lexing.from_string txt'
  in
  try
    let parsed_txt = Arakoon_parseini.inifile Arakoon_inilexer.lexini lxbuf in
    List.iter
      (fun (section, values) ->
         Hashtbl.add tbl section
           (List.fold_left
              (fun tbl (key, value) -> Hashtbl.add tbl key value;tbl)
              (Hashtbl.create 10)
              values))
      parsed_txt
  with Parsing.Parse_error | Failure "lexing: empty token" ->
    raise (Ini_parse_error (lxbuf.Lexing.lex_curr_p.Lexing.pos_lnum, txt))

let write_inifile oc tbl =
  let open Lwt.Infix in
  let sections = Hashtbl.fold (fun k s acc -> (k,s)::acc) tbl [] |> List.rev in
  Lwt_list.iter_s
    (fun (k,section) ->
     Lwt_io.fprintlf oc "[%s]" k >>= fun () ->
     let kvs = Hashtbl.fold (fun k v acc -> (k,v) :: acc) section [] |> List.rev in
     Lwt_list.iter_s
       (fun (k,v) ->
        Lwt_io.fprintlf oc "%s = %s\n" k v
       )
       kvs
     >>= fun () ->
     Lwt_io.write oc "\n"
    ) sections

let ini_to_string tbl =
  let buf = Buffer.create 20 in
  Hashtbl.iter
    (fun k v ->
       Buffer.add_string buf "[";
       Buffer.add_string buf k;
       Buffer.add_string buf "]\n";
       (Hashtbl.iter
          (fun k v ->
             Buffer.add_string buf k;
             Buffer.add_string buf " = ";
             Buffer.add_string buf v;
             Buffer.add_string buf "\n") v);
       Buffer.add_string buf "\n")
    tbl;
  Buffer.contents buf

class inifile ?(spec=[]) txt =
  object (self)
    val data = Hashtbl.create 50

    initializer
      let () = parse_inifile txt data in
      self#validate

    method private validate =
      match spec with
          [] -> ()
        | spec ->
          List.iter
            (fun {sec_name=name;sec_required=required;
                  sec_attributes=attrs} ->
              try
                let sec =
                  try Hashtbl.find data name
                  with Not_found -> raise (Missing_section name)
                in
                List.iter
                  (fun {atr_name=name;atr_required=req;
                        atr_default=def;atr_validator=validator} ->
                    try
                      let attr_val =
                        try Hashtbl.find sec name
                        with Not_found -> raise (Missing_element name)
                      in
                      (match validator with
                           Some rex ->
                           if not (Str.string_match rex attr_val 0 )
                           then
                             raise
                               (Invalid_element
                                  (name ^ ": validation failed"))
                         | None -> ())
                    with Missing_element elt ->
                      if req then raise (Missing_element elt)
                      else match def with
                          Some def -> List.iter (Hashtbl.add sec name) def
                        | None -> ())
                  attrs
              with Missing_section s ->
                if required then raise (Missing_section s))
            spec

    method getval sec elt =
      try Hashtbl.find
            (try (Hashtbl.find data sec)
            with Not_found -> raise (Invalid_section sec))
            elt
      with Not_found -> raise (Invalid_element elt)

    method getaval sec elt =
      try Hashtbl.find_all
            (try (Hashtbl.find data sec)
            with Not_found -> raise (Invalid_section sec))
            elt
      with Not_found -> raise (Invalid_element elt)

    method setval sec elt v =
      (Hashtbl.add
         (try Hashtbl.find data sec
         with Not_found ->
           let h = Hashtbl.create 10 in
           Hashtbl.add data sec h;h)
         elt v);
      try self#validate
      with exn -> Hashtbl.remove data elt;raise exn

    method delval sec elt =
      let valu =
        try
          Some
            (Hashtbl.find
               (try Hashtbl.find data sec
               with Not_found -> raise (Invalid_section sec))
               elt)
        with Not_found -> None
      in
      match valu with
          Some v ->
          ((Hashtbl.remove
              (try Hashtbl.find data sec
              with Not_found -> raise (Invalid_section sec))
              elt);
           try self#validate
           with exn ->
             (Hashtbl.add
                (try Hashtbl.find data sec
                with Not_found -> raise (Invalid_section sec))
                elt v);
             raise exn)
        | None -> ()


    method sects =
      (Hashtbl.fold
         (fun k _v keys -> k :: keys)
         data [])

    method iter func sec =
      (Hashtbl.iter func
         (try Hashtbl.find data sec
         with Not_found -> raise (Invalid_section sec)))

    method attrs sec =
      (Strset.elements
         (setOfList
            (Hashtbl.fold
               (fun k _v attrs -> k :: attrs)
               (try Hashtbl.find data sec
               with Not_found -> raise (Invalid_section sec))
               [])))

    method save file =
      Lwt_io.with_file
        Lwt_io.Output file
        (fun oc -> write_inifile oc data)

    method to_string =
      ini_to_string data
  end

let readdir path =
  let dir = Unix.handle_unix_error Unix.opendir path in
  let rec do_read dir =
    try
      let current = Unix.readdir dir in
      current :: (do_read dir)
    with End_of_file -> []
  in
  let lst = do_read dir in
  Unix.closedir dir;
  lst

let fold ?(spec=[]) func path initial =
  let is_ini_file path =
      try
        if (Unix.stat path).Unix.st_kind = Unix.S_REG
        then Filename.check_suffix path ".ini"
        else false
      with Unix.Unix_error (_,_,_) -> false in
  (List.fold_left
     func
     initial
     (List.rev_map
        (new inifile ~spec)
        (List.filter
           is_ini_file
           (List.rev_map
              (fun p -> (path ^ "/" ^ p))
              (readdir path)))))
