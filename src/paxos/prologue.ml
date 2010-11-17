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

(*

decide_master still here to peek because of WIP

let rec decide_master ?(n=Sn.start) constants (me2, others, store, tlog_coll) (take,current_i) =
  let me = constants.me in
  log ~me "MASTER NOT YET KNOWN" >>= fun () ->
  let me_master = Update.make_update_value (Update.MasterSet me) in
  Lwt.catch (fun () ->
    let fail_on_nak = true in
    Multi_paxos.suggest ~fail_on_nak constants me_master (n,current_i)
    >>= fun (v',n,i) ->
    if v' <> me_master then
      begin
	log ~me "I AM SLAVE (%s,%s)" (Sn.string_of n) (Sn.string_of i) >>= fun () ->
	extended_slave constants (me2, others, store, tlog_coll) (take, i)
      end
    else
      begin
	log ~me "I AM MASTER (%s,%s)" (Sn.string_of n) (Sn.string_of i) >>= fun () ->
	let new_i = Sn.succ i in
	let q = LWTQ.create () in
	stable_master q constants take (v',n, new_i)
      end
  ) (function
    | Multi_paxos.Got_higher_prepare n'
    | Multi_paxos.Fail_with_nak n' ->
      begin
	log ~me "I FALL BACK TO SLAVE (%s,%s)" (Sn.string_of n) (Sn.string_of current_i) >>= fun () ->
	extended_slave constants (me2, others, store, tlog_coll) (take, current_i)
      end
    | x ->
      let n' = Sn.add n (Sn.from_int (1 + Random.int ((List.length others) + 1))) in
      log ~me "RETRY to become master... %s -> %s" (Sn.string_of n) (Sn.string_of n') >>= fun () ->
      let n = n' in
      decide_master ~n constants (me2, others, store, tlog_coll) (take,current_i)
  )

*)
