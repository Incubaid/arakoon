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



module Mode = struct
  type t = NotQuiesced
         | ReadOnly
         | Writable

  let to_string = function
    | NotQuiesced -> "NotQuiesced"
    | ReadOnly -> "ReadOnly"
    | Writable -> "Writable"

  let is_quiesced = function
    | NotQuiesced -> false
    | ReadOnly | Writable -> true
end

module Result = struct
  type t = OK
         | FailMaster
         | Fail

  let to_string = function
    | OK -> "OK"
    | FailMaster -> "FailMaster"
    | Fail -> "Fail"
end
