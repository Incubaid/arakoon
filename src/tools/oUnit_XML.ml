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

open Unix
open OUnit

type unit_result ={
  mutable result_list : (float*test_result) list;
  mutable total_time : float;
  mutable total_success : int;
  mutable total_failures : int;
  mutable total_errors : int;
  mutable total_disabled : int;

  mutable tmp_start : float;
  mutable tmp_result : test_result;
}

let rec string_of_path_dot = function
  | [] -> ""
  | (ListItem value)::[] -> ""
  | (Label value)::[] -> value
  | (Label value)::(ListItem value2)::[] -> value
  | (ListItem value)::tl -> string_of_path_dot tl
  | (Label value)::tl -> value ^ "." ^ (string_of_path_dot tl)

let timed_result_fun unit_result = function
  | EStart path -> let _ = Printf.printf "running test %s\n" (string_of_path_dot path) in (unit_result.tmp_start <- Unix.gettimeofday())
  | EResult result -> unit_result.tmp_result <- result
  | EEnd path ->
    let time_diff = Unix.gettimeofday() -. unit_result.tmp_start in
    unit_result.result_list <- (time_diff , unit_result.tmp_result)::(unit_result.result_list);
    unit_result.total_time <- unit_result.total_time +. time_diff;
    match unit_result.tmp_result with
      | RSuccess path -> unit_result.total_success <- unit_result.total_success + 1
      | RFailure (path, result) -> unit_result.total_failures <- unit_result.total_failures + 1
      | RError (path, result) -> unit_result.total_errors <- unit_result.total_errors + 1
      | RSkip (path, result) | RTodo (path, result)
        -> unit_result.total_disabled <- unit_result.total_disabled + 1;;

(* let result = { result_list = [];
   total_time = 0.0;
   total_success = 0;
   total_failures = 0;
   total_errors = 0;
   total_disabled = 0;
   tmp_start = 0.0;
   tmp_result = RSuccess [] };; *)

let perform_timed_tests unit_result = perform_test (timed_result_fun unit_result)

let print_xml unit_result filename =

  let string_map (f:char -> 'a) (s:string) =
    let rec string_map2 (f:char -> 'a) (s:string) (n:int) (size:int) (acc:'a list) =
      if n = size then acc
      else
        let x = f (s.[n]) in
        string_map2 f s (n+1) size (x::acc)
    in
    string_map2 f s 0 (String.length s) [] in

  let xml_escape s =
    let list = string_map
                 (function
                   | '"' -> "&quot;"
                   | '&' -> "&amp;"
                   | '\'' -> "&apos;"
                   | '<' -> "&lt;"
                   | '>' -> "&gt;"
                   | c ->
                     let i = int_of_char c in
                     if i < 32 then (Printf.sprintf "&#x%04x;" i)
                     else (Printf.sprintf "%c" c)
                 ) s in
    String.concat "" (List.rev list) in

  let split_path = function
    | [] -> failwith "Testpath should at least contain 2 levels"
    | hd::[] -> failwith "Testpath should at least contain 2 levels"
    | hd::tl -> ((string_of_node hd),(string_of_path_dot (List.rev tl))) in

  let make_error result_type result =
    let e = xml_escape result in
    "<" ^ result_type ^ " message=\"" ^ e ^ "\"></" ^ result_type ^ ">" in

  let split_test_result = function
    | RSuccess path -> ((split_path path), "run", "")
    | RFailure (path, result) -> ((split_path path), "run", (make_error "failure" result))
    | RError (path, result) -> ((split_path path), "run", (make_error "error" result))
    | RSkip (path, result) -> ((split_path path), "notrun", "")
    | RTodo (path, result) -> ((split_path path), "notrun", "") in

  let xml_print_node output_channel elem =
    let time, result = elem in
    let ((name, path), result, message) = split_test_result result in
    match message with
      | "" -> Printf.fprintf output_channel
                "\t<testcase classname=\"%s\" name=\"%s\" status=\"%s\" time=\"%f\"/>\n" path name result time
      | _ -> let _ = Printf.fprintf output_channel
                       "\t<testcase classname=\"%s\" name=\"%s\" status=\"%s\" time=\"%f\">\n" path name result time in
        let _ = Printf.fprintf output_channel "\t\t%s\n" message in
        Printf.fprintf output_channel "\t</testcase>\n" in

  let xml_print_nodes output_channel = List.iter (xml_print_node output_channel) in

  let xml_print_head result oc =
    let total_tests = result.total_success + result.total_disabled + result.total_errors + result.total_failures in
    Printf.fprintf oc
      "<testsuite disabled=\"%i\" errors=\"%i\" failures=\"%i\" name=\"unittest.TestSuite\" tests=\"%i\" time=\"%f\">\n"
      result.total_disabled result.total_errors result.total_failures total_tests result.total_time in

  let xml_print_tail oc = Printf.fprintf oc "</testsuite>\n" in

  let oc = open_out filename in
  xml_print_head unit_result oc;
  xml_print_nodes oc unit_result.result_list;
  xml_print_tail oc;
  close_out oc
