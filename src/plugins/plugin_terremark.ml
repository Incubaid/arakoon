open Otc

(* Fairly literal translation of the Haskell implementation *)
let rec range (i:int) (j:int) = if i > j then [] else i :: (range (i + 1) j)
let charFromTo (f:char) (l:char) = List.map Char.chr
    (range (Char.code f) (Char.code l))

let find_idx (e:char) (l:char list) =
    let rec find_it elt acc = function
        | hd :: tl when elt = hd -> acc
        | hd :: tl -> find_it elt (acc + 1) tl
        | _ -> raise Not_found
    in find_it e 0 l

let c2s = Char.escaped

let (suffixes:char list) = charFromTo '0' '9' @ charFromTo 'A' 'Z' @
    charFromTo 'a' 'z'

let (firstSuffixChar:char) = List.hd suffixes
let (lastSuffixChar:char) = List.hd (List.rev suffixes)

let tailKeyName (k:string) = k ^ "_tail"
let keyPrefix (k:string) = k ^ "__"
let firstKeyName (k:string) = keyPrefix k ^ c2s firstSuffixChar

let rightNeighbour (c:char) = List.nth suffixes (find_idx c suffixes + 1)

let incrementKey (k:string) =
    let init (s:string) = String.sub s 0 (String.length s - 1) in
    let last (s:string) = String.get s (String.length s - 1) in
    let incr (c:char) = if c = lastSuffixChar
        then c2s c ^ c2s firstSuffixChar
        else Char.escaped (rightNeighbour c) in
    init k ^ incr (last k)


(* Database interface *)
let get db k =
    try let s = Bdb.get db (Local_store._p k) in Some s
    with Not_found -> None
let set = Local_store._set
let testAndSet = Local_store._test_and_set
let delete = Local_store._delete
let rangeEntries db f fi l li n =
    let ka = Bdb.range db (Local_store._f f) fi (Local_store._l l) li n in
    let kl = Array.to_list ka in
    List.fold_left
        (fun ret_list k ->
            let l = String.length k in
            ((String.sub k 1 (l - 1)), Bdb.get db k) :: ret_list) [] kl

let pop db q = match q with
      None -> raise (Failure "Invalid argument None")
    | Some q' ->
        let kp = Some (keyPrefix q') in
        let entries = rangeEntries db kp true None true 1 in
        match entries with
              [] -> None
            | [(key, value)] ->
                let () = delete db key in
                let tailKey = get db (tailKeyName q') in
                let () = match tailKey with
                      None -> ()
                    | Some _ -> set db (tailKeyName q') (firstKeyName q')
                in Some value
            | _ -> raise (Failure "Invalid number of values returned")

let push' db q v =
    let oldTail = get db (tailKeyName q) in
    let newTail = match oldTail with
          None -> firstKeyName q
        | Some oldTail' -> incrementKey oldTail'
    in
    let r = testAndSet db newTail None (Some v) in
    let () = match r with
          None -> set db (tailKeyName q) newTail
        | Some _ -> raise (Failure ("Key " ^ newTail ^ " already exists"))
    in None

let push db a = match a with
      None -> raise (Failure "Invalid argument None")
    | Some s ->
        let q, pos = Llio.string_from s 0 in
        let v, _ = Llio.string_from s pos in
        push' db q v

open Registry
let () = Registry.register "terremark.queue.pop_1" pop
let () = Registry.register "terremark.queue.push_1" push
