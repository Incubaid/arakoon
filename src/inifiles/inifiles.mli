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


(** This library reads ini format property files *)

(** raised if you ask for a section which doesn't exist *)
exception Invalid_section of string

(** raised if you ask for an element (attribute) which doesn't exist,
    or the element fails to match the validation regex *)
exception Invalid_element of string

(** raised if a required section is not specified in the config file *)
exception Missing_section of string

(** raised if a required element is not specified in the config file *)
exception Missing_element of string

(** raised if there is a parse error in the ini file it will contain the
    line number and the name of the file in which the error happened *)
exception Ini_parse_error of (int * string)

(** The type of an attribute/element specification *)
type attribute_specification = {
  atr_name: string;
  atr_required: bool;
  atr_default: (string list) option;
  atr_validator: Str.regexp option;
}

(** The type of a section specification *)
type section_specification = {
  sec_name: string;
  sec_required: bool;
  sec_attributes: attribute_specification list;
}

(** The type of a secification *)
type specification = section_specification list

(** parse text into inifile object. *)
class inifile : ?spec:specification -> string ->
  object
    (** get a value from the config object raise Invalid_section, or
        invalid_element on error.  getval section element *)
    method getval : string -> string -> string

    (** get a value from the config object. return a list of all the
        objects bindings. If the key is listed on more than one line it
        will get n bindings, where n is the number of lines it is
        mentioned on.  raise Invalid_section, or invalid_element on error.
        getaval section element *)
    method getaval : string -> string -> string list

    (** set a value in the config create a new section, and or element
        if necessary.  setval section element *)
    method setval : string -> string -> string -> unit

    (** delete the topmost binding (the one returned by getval) from the
        section sec. Possibly exposing another binding.  raise
        Invalid_section on error.  delval sec elt *)
    method delval : string -> string -> unit

    (** save the changes you have made
        optionally save to a different file *)
    method save : Lwt_io.file_name -> unit Lwt.t

    (** get a string representation of the inifile *)
    method to_string : string

    (** iterates across a section. passes all key valu pairs to f
        exactly once.*)
    method iter : (string -> string -> unit) -> string -> unit

    (** returns a list of all sections in the file *)
    method sects : string list

    (** return all the attibutes of a section *)
    method attrs : string -> string list
  end

(** Executes a fold left across a directory of ini files
    (skips files which do not end with .ini). fold f path a
    computes (f ... (f (f file1 a) file2) fileN) *)
val fold: ?spec:section_specification list -> ('a -> inifile -> 'a) -> string -> 'a -> 'a
