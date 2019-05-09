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
open Tlogcommon

class tlogWriter oc lastI =

  object

    val mutable lastWrittenI = lastI;

    method log_value i value =
      if isValidSuccessor i lastWrittenI  then
        begin
          write_entry oc i value >>= fun total_size ->
          Lwt_io.flush oc >>= fun () ->
          let () = lastWrittenI <- i in
          Lwt.return total_size
        end
      else
        Llio.lwt_failfmt "invalid successor %s" (Sn.string_of i)


  end
