type url =
  File of string
  | Etcd of (((string * int) list) * string)

let to_string = function
  | File f -> f
  | _ -> "??"

let make string =
  let protocol, rest =
    try
      Scanf.sscanf string "%s@://%s" (fun p r -> p,r)
    with End_of_file ->
      "file", string
  in
  match protocol with
  | "file" ->
      let canonical =
        if rest.[0] = '/'
        then rest
        else Filename.concat (Unix.getcwd()) rest
      in
      File canonical
  | "etcd" ->
     let host,port,path =
       Scanf.sscanf rest "%s@:%i/%s" (fun h i p -> h,i,p)
     in
     let peers = [host,port]
     in
     Etcd (peers, path)
  | _ -> failwith (Printf.sprintf "unknown protocol:%s" protocol)
