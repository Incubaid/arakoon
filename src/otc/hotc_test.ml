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

open OUnit
open Hotc
open Otc
open Prefix_otc
open Extra
open Lwt
open Logging

let eq_string s1 s2 = eq_string "TEST" s1 s2

let test_overal db =
  Hotc.transaction db
    (fun db ->
      let () = Bdb.put db "hello" "world" in
      let x = Bdb.get db "hello" in
      Lwt.return x
    ) >>= fun res ->
  let () = eq_string "world" res in
  Lwt.return ()

let test_with_cursor db =
  Hotc.transaction db
    (fun db' ->
      let () = Bdb.put db' "hello" "world" in
      Hotc.with_cursor db'
	(fun _ cursor ->
	  let () = Bdb.first db' cursor in
	  let x = Bdb.value db' cursor in
	  Lwt.return x
	)
    ) >>= fun res ->
  let () = eq_string "world" res in
  Lwt.return ()

let test_prefix db =
  Hotc.transaction db
    (fun db' ->
      Prefix_otc.put db' "VOL" "hello" "world" >>= fun () ->
      let x = Bdb.get db' "VOLhello" in
      Prefix_otc.get db' "VOL" "hello" >>= fun y ->
      Lwt.return (x,y)
    ) >>= fun (res1, res2) ->
  let () = eq_string "world" res1 in
  let () = eq_string "world" res2 in
  Lwt.return ()

let test_prefix_fold db =
  Hotc.transaction db
    (fun db' ->
      Prefix_otc.put db' "VOL" "hello" "world" >>= fun () ->
      Prefix_otc.put db' "VOL" "hi" "mars"
    ) >>= fun () ->
  Hotc.transaction db
    (fun db' -> Prefix_otc.iter (log "%s %s") db' "VOL")


let test_transaction db =
  let key = "test_transaction:1"
  and bad_key = "test_transaction:does_not_exist"
  in
  Lwt.catch
    (fun () ->
      Hotc.transaction db
	(fun db ->
	  Bdb.put db key "one";
	  Bdb.out db bad_key;
	  Lwt.return ()
	)
    )
    (function
      | Not_found ->
	Lwt_log.debug "yes, this key was not found" >>= fun () ->
	Lwt.return ()
      | x -> Lwt.fail x
    )
  >>= fun () ->
Lwt.catch
  (fun () ->
    Hotc.transaction db
      (fun db ->
	let v = Bdb.get db key in
	Lwt_io.printf "value=%s\n" v >>= fun () ->
	OUnit.assert_failure "this is not a transaction"
      )
  )
  (function
    | Not_found -> Lwt.return ()
    | x -> Lwt.fail x
  )




let test_batch db =
  Hotc.transaction db
    (fun db' ->
       Prefix_otc.put db' "VOL" "ha" "lo" >>= fun () ->
       Prefix_otc.put db' "VOL" "he" "pluto" >>= fun () ->
       Prefix_otc.put db' "VOL" "hello" "world" >>= fun () ->
       Prefix_otc.put db' "VOL" "hi" "mars"
    ) >>= fun () ->
  Hotc.batch db 2 "VOL" None >>= fun batch ->
  match batch with
    | [(k1,s1);(k2,s2)] ->
      begin
      Hotc.batch db 2 "VOL" (Some k2) >>= fun batch2 ->
      match batch2 with
	| [(k3,s3);(k4,s4)] ->
	  eq_string "ha" k1;
	  eq_string "lo" s1;
	  eq_string "he" k2;
	  eq_string "pluto" s2;
	  eq_string "hello" k3;
	  eq_string "world" s3;
	  eq_string "hi" k4;
	  eq_string "mars" s4;
	  Hotc.batch db 2 "VOL" (Some k4) >>= fun batch3 ->
	  assert_equal batch3 [];
	  Lwt.return ()
	| _ -> Lwt.fail (Failure "2:got something else")
      end
    | _ -> Lwt.fail (Failure "1:got something else")

let eq_string str i1 i2 =
  let msg = Printf.sprintf "%s expected:\"%s\" actual:\"%s\"" str (String.escaped i1) (String.escaped i2) in
  OUnit.assert_equal ~msg i1 i2

let eq_list eq_ind str l1 l2 =
  let len1 = List.length l1 in
  let len2 = List.length l2 in
  if len1 <> len2 then
    OUnit.assert_failure (Printf.sprintf "%s: lists not equal in length l1:%d l2:%d" str len1 len2)
  else
    let cl = List.combine l1 l2 in
    let (_:int) = List.fold_left
      (fun i (x1,x2) ->
        let () = eq_ind (Printf.sprintf "ele:%d %s" i str) x1 x2 in
        i+1) 0 cl in
    ()

let eq_tuple eq1 eq2 str v1 v2 =
  let (v11, v12) = v1 in
  let (v21, v22) = v2 in
  let () = eq1 ("t1:"^str) v11 v21 in
  let () = eq2 ("t2:"^str) v12 v22 in
  ()

let show_l l =
  let l2 = List.map (fun (k,v) -> Printf.sprintf "('%s','%s')" k v) l in
  "[" ^ (String.concat ";" l2) ^ "]"

let test_next_prefix =
  let get = function | Some x -> x | _ -> raise Exit in
  let some_next_prefix s = get (next_prefix s) in
  let assert_next prefix next = eq_string "" (some_next_prefix prefix) next in
  assert_next "aa" "ab";
  assert_next "a\255\255" "b\000\000";
  let next = next_prefix "\255\255" in
  if (next <> None) then failwith "next prefix not correct"

let test_rev_range_entries db =
  let eq = eq_list (eq_tuple eq_string eq_string) in
  Hotc.transaction db
    (fun db ->
      Prefix_otc.put db "VAN" "il" "le" >>= fun () ->
      Prefix_otc.put db "VOL" "ha" "lo" >>= fun () ->
      Prefix_otc.put db "VOL" "he" "pluto" >>= fun () ->
      Prefix_otc.put db "VOL" "hello" "world" >>= fun () ->
      Prefix_otc.put db "VOL" "hi" "mars" >>= fun () ->
      Prefix_otc.put db "Z"   "h" "f"
    ) >>= fun () ->
  let bdb = Hotc.get_bdb db in
  (* as we give NO start entry this will start at the LAST entry in the prefix... *)
  let l1 = Hotc.rev_range_entries "VOL" bdb None true None true 3 in
  let () = Printf.printf "l1: %s\n" (show_l l1) in
  let () = eq "l1" [("he","pluto");("hello","world");("hi","mars")] l1 in
  let l2 = Hotc.rev_range_entries "VOL" bdb (Some "hi") true None true 3 in
  let () = Printf.printf "l2: %s\n" (show_l l2) in
  (* note that they are returned IN order *)
  let () = eq "l2" [("he","pluto");("hello","world");("hi", "mars")] l2 in
  (* don't include first *)
  let l3 = Hotc.rev_range_entries "VOL" bdb (Some "hi") false None true 3 in
  let () = Printf.printf "l3: %s\n" (show_l l3) in
  let () = eq "l3" [("ha","lo");("he","pluto");("hello","world")] l3 in
  let l4 = Hotc.rev_range_entries "VOL" bdb (Some "hi") false (Some "he") true 3 in
  let () = Printf.printf "l4: %s\n" (show_l l4) in
  let () = eq "l4" [("he","pluto");("hello","world")] l4 in
  let l5 = Hotc.rev_range_entries "VOL" bdb (Some "hi") false (Some "he") false 3 in
  let () = Printf.printf "l5: %s\n" (show_l l5) in
  let () = eq "l5" [("hello","world")] l5 in
  let l6 = Hotc.rev_range_entries "VOL" bdb (Some "he") false None false 4 in
  let () = Printf.printf "l6: %s\n" (show_l l6) in
  let () = eq "l6" [("ha","lo")] l6 in
  let l7 = Hotc.rev_range_entries "VOL" bdb (Some "ha") false None false 4 in
  let () = Printf.printf "l7: %s\n" (show_l l7) in
  let () = eq "l7" [] l7 in
  let l8 = Hotc.rev_range_entries "VOL" bdb (Some "he") true (Some "h") false 4 in
  let () = Printf.printf "l8: %s\n" (show_l l8) in
  let () = eq "l8" [("ha","lo");("he","pluto")] l8 in
  let l9 = Hotc.rev_range_entries "VOL" bdb (Some "hd") false None true 3 in
  let () = Printf.printf "l9: %s\n" (show_l l9) in
  let () = eq "l9" [("ha","lo")] l9 in
  let l10 = Hotc.rev_range_entries "VOL" bdb (Some "hu") false None true 3 in
  let () = Printf.printf "l10: %s\n" (show_l l10) in
  let () = eq "l10" [("he","pluto");("hello","world");("hi","mars")] l10 in
  Lwt.return ()

let test_rev_range_entries2 db =
  let eq = eq_list (eq_tuple eq_string eq_string) in
  Hotc.transaction db
    (fun db ->
      Prefix_otc.put db "@" "nsA_foo" "foox" >>= fun () ->
      Prefix_otc.put db "@" "nsI_foo" "foo0" >>= fun () ->
      Prefix_otc.put db "@" "nsN_VOL" "foo1" >>= fun () ->
      Prefix_otc.put db "@" "nsN_VOL2" "foo2"
    ) >>= fun () ->
  let bdb = Hotc.get_bdb db in
  (* rev_range_entries @ @nsN_VOL2 true @nsN_ false *)
  let l1 = Hotc.rev_range_entries "@" bdb (Some "nsN_VOL2") true (Some "nsN_") false 3 in
  let () = Printf.printf "l1: %s\n" (show_l l1) in
  let () = eq "l1" [("nsN_VOL","foo1");("nsN_VOL2", "foo2")] l1 in
  Lwt.return ()

let test_rev_range_entries3 db = 
  let eq = eq_list (eq_tuple eq_string eq_string) in
  let _pf = "@" in
  Hotc.transaction db
    (fun bdb -> 
      let put k v = Bdb.put bdb (_pf ^ k) v in
      put "faa" "boo";
      put "foo" "bar";
      Lwt.return () 
    ) >>= fun () ->

  let first = Some "z" 
  and finc = false
  and last = None
  and linc = false
  and max = -1 in
  let bdb = Hotc.get_bdb db in
  let l1 = Hotc.rev_range_entries _pf bdb first finc last linc max in
  let () = Printf.printf "l1: %s\n" (show_l l1) in
  let () = eq "l1" [("faa", "boo");("foo","bar")] l1 in
  Lwt.return ()


let test_delete_prefix db = 
  let bdb = Hotc.get_bdb db in
  let rec fill i = 
    if i = 100 
    then ()
    else
      let () = Bdb.put bdb (Printf.sprintf "my_prefix_%i"  i) "value does not matter" in
      fill (i+1)
  in
  let () = fill 0 in
  let g0 = Hotc.delete_prefix bdb "x" in
  let my_test a b = OUnit.assert_equal ~printer:string_of_int a b in
  my_test 0 g0;
  let g11 = Hotc.delete_prefix bdb "my_prefix_2" in
  my_test 11 g11;
  let g89 = Hotc.delete_prefix bdb "my_prefix_" in
  my_test 89 g89;
  Lwt.return ()

let setup () = Hotc.create "/tmp/foo.tc"

let teardown db =
  Hotc.delete db >>= fun () ->
  let () = Unix.unlink "/tmp/foo.tc" in 
  Lwt.return ()

let suite =
  let wrap f = lwt_bracket setup f teardown in
  "Hotc" >:::
    [
      "overal" >:: wrap test_overal;
      "with_cursor" >:: wrap test_with_cursor;
      "prefix" >:: wrap test_prefix;
      "prefix_fold" >:: wrap test_prefix_fold;
      "batch" >:: wrap test_batch;
      "transaction" >:: wrap test_transaction;
      "rev_range_entries" >:: wrap test_rev_range_entries;
      "rev_range_entries2" >:: wrap test_rev_range_entries2;
      "rev_range_entries3" >:: wrap test_rev_range_entries3;
      "delete_prefix" >:: wrap test_delete_prefix;
    ]
