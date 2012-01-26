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

type resource = (* natural order, ie: CPU = 0 etc. *)
  | CPU 
  | SIZE 
  | EATA 
  | STACK 
  | CORE 
  | SSS 
  | PROC 
  | NOFILE 
  | MEMLOCK 
  | AS 
  | LOCKS 
  | SIGPENDING 
  | MSGQUEUE 
  | NICE 
  | RTPRIO 
  | NLIMITS 

type soft_or_hard =
  | Soft
  | Hard

external get_rlimit: resource -> soft_or_hard -> int = "arakoon_get_rlimit"

external get_maxrss: unit -> int  = "arakoon_get_maxrss"
