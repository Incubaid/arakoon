open Core
module ONE = struct
  type state_name = 
    | S_MASTER
    | S_PUSHING
    | S_FINISH

  type tick = TICK of int
  let (<:) (TICK i0) (TICK i1) = i0 < i1

  let start_tick = TICK 0
  let next_tick (TICK i) = TICK (i+1)

  type state = tick * state_name

  let tick_of (c,_) = c
  let sn_of (_,sn) = sn
  type id = int

  let tick2s (TICK c) = Printf.sprintf "%04i" c

  let id2s id = string_of_int id

  let state_name2s = function
    | S_MASTER  -> "S_MASTER"
    | S_PUSHING -> "S_PUSHING"
    | S_FINISH  -> "S_FINISH"

  let state2s (c,sn) = Printf.sprintf "(%s,%s)" (tick2s c) (state_name2s sn) 

  let update2s= function
    | SET (k,v) -> Printf.sprintf "SET(%s,%s)" k v
    | DELETE k  -> Printf.sprintf "DELETE %s" k

  let result2s = function| UNIT -> "UNIT"

  type value = 
    | V_D
    | V_C of (id * update)


  let value2s = function
    | V_D -> "V_D"
    | V_C (id,u) -> Printf.sprintf "V_C (%s %s)" (id2s id) (update2s u)

  type msg = 
    | M_FINISH
    | M_TIMEOUT of state 
    | M_CLIENT
    | M_CHOICE of value
    | M_PUSHED of value

  let msg2s = function
    | M_FINISH -> "M_FINISH"
    | M_TIMEOUT (c0,sn0) -> Printf.sprintf "M_TIMEOUT(%s,%s)" (tick2s c0) (state_name2s sn0)
    | M_CLIENT -> "M_CLIENT"
    | M_CHOICE v -> Printf.sprintf "M_CHOICE (%s)" (value2s v)
    | M_PUSHED v -> Printf.sprintf "M_PUSHED (%s)" (value2s v)

  type action = 
    | A_NOP
    | A_CHOOSE 
    | A_PUSH of value
    | A_STORE_RETURN of value
    | A_DIE

  let action2s = function
    | A_NOP -> "A_NOP"
    | A_DIE -> "A_DIE"
    | A_CHOOSE -> Printf.sprintf "A_CHOOSE"
    | A_PUSH  v  -> Printf.sprintf "A_PUSH (%s)" (value2s v)
    | A_STORE_RETURN v  -> Printf.sprintf "A_STORE_RETURN (%s)" (value2s v)

  
  let start = (TICK 0, S_MASTER)

  let step m s = 
    let c,sn = s in
    let c' = next_tick c in
    let push v =    (A_PUSH  v), (c', S_PUSHING) in
    let store idv =   (A_STORE_RETURN idv), (c', S_MASTER) in
    let choose () =   A_CHOOSE, (c', S_MASTER) in
    let timeout_master (c0,sn0) = 
      if c0 <: c 
      then A_NOP, (c, S_MASTER) 
      else push V_D
    in
    match m,sn with
    | M_TIMEOUT t , S_MASTER  -> timeout_master t
    | M_CLIENT           , S_MASTER  -> choose ()
    | M_CHOICE v         , S_MASTER  -> push v
    | M_PUSHED v         , S_PUSHING -> store v
    | M_FINISH , _ -> 
      let s' = (c', S_FINISH) 
      and a = A_DIE in
      a, s' 
    | _, _ -> failwith (Printf.sprintf "unhandled %s in %s" (msg2s m) (state2s s))

end

module ONE_SM = (ONE:SM) (* TODO *)
