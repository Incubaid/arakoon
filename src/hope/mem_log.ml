open Mp
open Core

module MemLog = struct

  type t = {
    mutable last_i  : tick option;
    mutable entries : (tick*update) list;
  }
  
  let log_update t i u =
    let valid_request () =
      t.last_i  <- Some i ;
      t.entries <- (i,u) :: t.entries
    in
    begin
      match t.last_i with
        | None -> valid_request() 
        | Some li when li = i -> valid_request() 
        | Some li when (next_tick li) = i -> valid_request() 
        | Some li -> 
          let msg = Printf.sprintf "Invalid log request for update (li: %s) (i:%s)"
            (tick2s li) (tick2s i)
          in failwith msg 
    end
      
  let create () =  { entries = [] ; last_i = None }
 
end