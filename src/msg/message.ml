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



module Message = struct

  type t = {kind:string; payload:string} (* primitive, bug suffices *)


  let create kind payload =
    {kind = kind; payload = payload}

  let kind_of t = t.kind

  let payload_of t = t.payload

  let string_of t=
    if String.length t.payload >= 128
    then
      Printf.sprintf "{kind=%s;payload=%S ...}" t.kind (String.sub t.payload 0 128)
    else
      Printf.sprintf "{kind=%s;payload=%S}" t.kind t.payload

  let to_buffer t buffer =
    Llio.string_to buffer t.kind;
    Llio.string_to buffer t.payload

  let from_buffer buffer =
    let k  = Llio.string_from buffer in
    let p  = Llio.string_from buffer
    in {kind=k;payload=p}
end
