module Node : sig
    type t

    val make : string -> t
    val to_string : t -> string
end = struct
    type t = string

    let make = Std.id
    let to_string = Std.id
end

module Message : sig
    type t
end = struct
    type t
end

module Config : sig
    type t
end = struct
    type t
end

module FSM(T : sig type t end) = struct
    module F = struct
        type 'a t = Log of ((unit -> string) * (unit -> 'a))
                  | Send of (Node.t * Message.t * (unit -> 'a))
                  | Broadcast of (Message.t * (unit -> 'a))
                  | GetState of (T.t -> 'a)
                  | SetState of (T.t * (unit -> 'a))
                  | GetConfig of (Config.t -> 'a)

        let map f = function
          | Log (m, c) -> Log (m, fun () -> f (c ()))
          | Send (n, m, c) -> Send (n, m, fun () -> f (c ()))
          | Broadcast (m, c) -> Broadcast (m, fun () -> f (c ()))
          | GetState c -> GetState (fun t -> f (c t))
          | SetState (s, c) -> SetState (s, fun () -> f (c ()))
          | GetConfig c -> GetConfig (fun t -> f (c t))
    end

    module M = Free.Make(F)
    module U = Free.Utils(M)

    include U

    let iter = M.iter
    module IterM = M.IterM

    let log m = M.lift (F.Log (m, Std.id))
    let send n m = M.lift (F.Send (n, m, Std.id))
    let broadcast m = M.lift (F.Broadcast (m, Std.id))
    let get_state () = M.lift (F.GetState Std.id)
    let set_state s = M.lift (F.SetState (s, Std.id))
    let get_config () = M.lift (F.GetConfig Std.id)

    let modify f = get_state () >>= fun s -> set_state (f s)
end

module Slave = struct
    type t

    module FSM = FSM(struct type z = t type t = z end)

    module M = FSM.IterM(struct
        include Lwt

        let pure = return
        let ap f a = f >>= fun f' -> a >>= fun a' -> pure (f' a')
    end)
    let run cfg =
        let handle = function
          | FSM.F.GetConfig c -> c cfg
        in
        M.iterM handle

    let a c = run c FSM.(get_config () >>= fun _ -> FSM.pure 1)
end
