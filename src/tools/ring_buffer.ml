(*
 * Copyright (c) 2014, Incubaid BVBA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *)

(** A simple ring-buffer implementation *)

(** Create a new ring-buffer for a given element type *)
module Make(E : sig
    (** Ringbuffer element type *)
    type t

    (** Zero-value, used to initialize the backing array

    Note: Entries equal to this value are passed through during a fold like
    any other. *)
    val zero : t
end) : sig
    type t

    val create : size:int -> t
    val insert : E.t -> t -> unit
    val fold : f:('a -> E.t -> 'a) -> acc:'a -> t -> 'a
    val to_string : f:(E.t -> string) -> t -> string
end = struct
    (** Ring-buffer type for elements of type {!E.t} *)
    type t = { buffer : E.t array
             ; mutable pos : int
             }

    (** Create a new ring-buffer of given size

    @param size Size of the ring-buffer *)
    let create ~size =
        let buffer = Array.make size E.zero in
        { buffer; pos = 0; }

    (** Insert an element in the ring-buffer *)
    let insert e t =
        let cp = t.pos in
        Array.set t.buffer cp e;
        let np = cp + 1 in
        t.pos <- np mod (Array.length t.buffer)

    (** Left-fold over the content of the ring-buffer, starting at the oldest value

    Note: {!E.zero} elements are not skipped. *)
    let fold ~f ~acc t =
        let size = Array.length t.buffer
        and pos = t.pos in

        let rec loop acc = function
          | 0 -> acc
          | n ->
              let p = (pos + abs (n - size)) mod size in
              let e = Array.get t.buffer p in
              let acc' = f acc e in
              loop acc' (n - 1)
        in

        loop acc size

    (** Generate a string representation of a ring-buffer, for debugging purposes

    @param f Function used to convert an {!E.t} to a string representation *)
    let to_string ~f t =
        let l = Array.to_list t.buffer in
        let ls = List.map f l in
        let b = String.concat "; " ls in
        Printf.sprintf "{ pos = %d; buffer = [| %s |] }" t.pos b
end

module Test = struct
    let run () =
        let module M = Make(struct
            type t = int
            let zero = 0
        end) in

        let t = M.create ~size:5 in
        print_endline (M.to_string ~f:string_of_int t);

        let sum t = M.fold ~f:(fun a b -> Printf.printf "%d %d\n" a b;  a + b) ~acc:0 t in

        List.iter (fun x ->
            M.insert x t;
            print_endline (M.to_string ~f:string_of_int t);
            Printf.printf "Sum: %d\n" (sum t))
            [1; 2; 3; 4; 5; 6; 7; 8; 9; 10]
end
