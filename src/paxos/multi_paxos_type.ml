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


type finished_fun = Store.update_result -> unit Lwt.t
type master_option = finished_fun option

type transitions =
  (* dummy to always have a previous transition *)
  | Start_transition

  (* forced master only *)
  | Forced_master_suggest of (Sn.t * Sn.t)

  (* election only *)
  | Election_suggest of (Sn.t * Sn.t)

  (* slave or pending slave *)
  | Slave_fake_prepare of Sn.t
  | Slave_waiting_for_prepare of Mp_msg.MPMessage.n
  | Slave_steady_state of (Mp_msg.MPMessage.n * Mp_msg.MPMessage.n * Value.t)
  | Slave_wait_for_accept of (Mp_msg.MPMessage.n * Mp_msg.MPMessage.n * Value.t option)
  | Slave_discovered_other_master of (Messaging.id * Mp_msg.MPMessage.n * Mp_msg.MPMessage.n * Mp_msg.MPMessage.n)

  | Promises_check_done of (Mp_msg.MPMessage.n * Mp_msg.MPMessage.n * (int * Messaging.id list) * Value.t * Value.t list * (string * Mp_msg.MPMessage.n) option)
  | Wait_for_promises of (Mp_msg.MPMessage.n * Mp_msg.MPMessage.n * (int * Messaging.id list) * Value.t * Value.t list * (string * Mp_msg.MPMessage.n) option)
  | Accepteds_check_done of (master_option * Mp_msg.MPMessage.n * Mp_msg.MPMessage.n * (int * Messaging.id list) * Value.t)
  | Wait_for_accepteds of (master_option * Mp_msg.MPMessage.n * Mp_msg.MPMessage.n * (int * Messaging.id list) * Value.t)

  (* active master only *)
  | Master_consensus of (master_option * Value.t * Mp_msg.MPMessage.n * Mp_msg.MPMessage.n)
  | Stable_master of (Value.t * Mp_msg.MPMessage.n * Mp_msg.MPMessage.n)
  | Master_dictate of (master_option * Value.t * Mp_msg.MPMessage.n * Mp_msg.MPMessage.n)

(* utility functions *)
let show_transition = function
  | Start_transition -> "Start_transition"
  | Forced_master_suggest _ -> "Forced_master_suggest"
  | Election_suggest _ -> "Election_suggest"
  | Slave_fake_prepare _ -> "Slave_fake_prepare"
  | Slave_waiting_for_prepare _ -> "Slave_waiting_for_prepare"
  | Slave_steady_state _ -> "Slave_steady_state"
  | Slave_wait_for_accept _ -> "Slave_wait_for_accept"
  | Slave_discovered_other_master _ -> "Slave_discovered_other_master"
  | Wait_for_promises _ -> "Wait_for_promises"
  | Promises_check_done _ -> "Promises_check_done"
  | Wait_for_accepteds _ -> "Wait_for_accepteds"
  | Accepteds_check_done _ -> "Accepteds_check_done"
  | Master_consensus _ -> "Master_concensus"
  | Stable_master _ -> "Stable_master"
  | Master_dictate _ -> "Master_dictate"
