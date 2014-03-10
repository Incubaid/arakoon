(*
This file is part of Arakoon, a distributed key-value store. Copyright
(C) 2010-2014 Incubaid BVBA

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

open Lwt

module Section =
struct
  let make = Lwt_log.Section.make
  let set_level = Lwt_log.Section.set_level
  let level = Lwt_log.Section.level
  let main = Lwt_log.Section.main
end

type level =
    Lwt_log.level =
  | Debug
  | Info
  | Notice
  | Warning
  | Error
  | Fatal

let log ?exn section level msg =
  if level < Lwt_log.Section.level section
  then
    Crash_logger.add_to_crash_log section level [Crash_logger.Immediate (msg, exn)]
  else
    Lwt_log.log ?exn ~section ~level msg

let ign_log ?exn section level msg =
  Lwt.ignore_result (log ?exn section level msg)

let log_ ?exn section level dmsg =
  if level < Lwt_log.Section.level section
  then
    Crash_logger.add_to_crash_log section level [Crash_logger.Delayed (dmsg, exn)]
  else
    Lwt_log.log ?exn ~section ~level (dmsg ())

let ign_log_ ?exn section level dmsg =
  Lwt.ignore_result (log_ ?exn section level dmsg)
