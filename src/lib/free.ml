(* TODO Finally read http://homepages.cwi.nl/~ploeg/papers/zseq.pdf and fix the
 * algorithmic complexity of what's constructed *)
module type Functor = sig
    type 'a t

    val map : ('a -> 'b) -> 'a t -> 'b t
end

module type Applicative = sig
    include Functor

    val pure : 'a -> 'a t
    val ap : ('a -> 'b) t -> 'a t -> 'b t
end

module type Monad = sig
    include Applicative

    val bind : 'a t -> ('a -> 'b t) -> 'b t
end

module Make(F : Functor) : sig
    include Monad

    val lift : 'a F.t -> 'a t
    val iter : ('a F.t -> 'a) -> 'a t -> 'a

    module IterM(M : Monad) : sig
        val iterM : ('a M.t F.t -> 'a M.t) -> 'a t -> 'a M.t
    end
end = struct
    type 'a t = Pure of 'a | Free of ('a t) F.t

    let map f =
        let rec loop = function
          | Pure a -> Pure (f a)
          | Free fa -> Free (F.map loop fa)
        in
        loop

    let pure a = Pure a
    let ap =
        let rec loop f a = match f with
          | Pure f' -> begin match a with
              | Pure a' -> Pure (f' a')
              | Free a' -> Free (F.map (map f') a')
          end
          | Free f' -> Free (F.map (fun f'' -> loop f'' a) f')
        in
        loop

    let bind =
        let rec loop a f = match a with
          | Pure a' -> f a'
          | Free a' -> Free (F.map (fun a'' -> loop a'' f) a')
        in
        loop

    let iter handle =
        let rec loop = function
          | Pure a -> a
          | Free f -> handle (F.map loop f)
        in
        loop

    let lift f = Free (F.map pure f)

    module IterM(M : Monad) = struct
        let iterM handle =
            let rec loop = function
              | Pure a -> M.pure a
              | Free f -> handle (F.map loop f)
            in
            loop
    end
end

module Utils(M : Monad) : sig
    include Monad with type 'a t := 'a M.t

    val (<$>) : ('a -> 'b) -> 'a M.t -> 'b M.t
    val (<*>) : ('a -> 'b) M.t -> 'a M.t -> 'b M.t
    val (>>=) : 'a M.t -> ('a -> 'b M.t) -> 'b M.t

    val sequence : 'a M.t list -> 'a list M.t
    val filterM : ('a -> bool M.t) -> 'a list -> 'a list M.t
    val foldM : ('a -> 'b -> 'a M.t) -> 'a -> 'b list -> 'a M.t
    val forM : 'a list -> ('a -> 'b M.t) -> 'b list M.t

    val unless : bool -> unit M.t -> unit M.t
    val when_ : bool -> unit M.t -> unit M.t
end = struct
    include M

    let (<$>) = map
    let (<*>) = ap
    let (>>=) = bind

    let filterM p =
        let rec loop = function
          | [] -> pure []
          | (x :: xs) -> begin
              p x >>= fun k ->
              loop xs >>= fun r ->
              pure (if k then x :: r else r)
          end
        in
        loop

    let foldM f =
        let rec loop a = function
          | [] -> pure a
          | (x :: xs) -> begin
              f a x >>= fun a' ->
              loop a' xs
          end
        in
        loop

    let unless b f =
        if b then pure () else f

    let when_ b f =
        if b then f else pure ()

    let sequence ls =
        let helper t0 t1 =
            t0 >>= fun x ->
            t1 >>= fun xs ->
            pure (x :: xs)
        in
        List.fold_right helper ls (pure [])

    let forM l f = sequence (List.map f l)
end
