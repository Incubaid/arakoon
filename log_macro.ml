open Camlp4.PreCast


let rec apply e = function
  | [] -> e
  | x :: l -> let _loc = Ast.loc_of_expr x in apply <:expr< $e$ $x$ >> l

let split e =
  let rec aux acc = function
    | <:expr@_loc< log $arg$ >> ->
        `Log(arg :: acc)
    | <:expr@_loc< log_f $arg$ >> ->
        `Log_f(arg :: acc)
    | <:expr@loc< $a$ $b$ >> -> begin
        match b with
          | b ->
              aux (b :: acc) a
      end
    | _ ->
        `No_match
  in
  aux [] e

let make_loc _loc =
  <:expr<
    ($str:Loc.file_name _loc$,
     $int:string_of_int (Loc.start_line _loc)$,
     $int:string_of_int (Loc.start_off _loc - Loc.start_bol _loc)$)
  >>

let map =
object
  inherit Ast.map as super

  method expr e =
    let _loc = Ast.loc_of_expr e in
    match split e with
      | `Log(args) ->
          let args = List.map super#expr args in
          <:expr<
            begin
              if Lwt_log.Section.level section <= Lwt_log.Debug
              then $apply <:expr< log >> args$
              else Lwt.return ()
            end >>
      | `Log_f(args) ->
          let args = List.map super#expr args in
          <:expr<
            begin
              if Lwt_log.Section.level section <= Lwt_log.Debug
              then $apply <:expr< log_f >> args$
              else Lwt.return ()
            end >>
      | `No_match ->
          super#expr e
end

let () =
  AstFilters.register_str_item_filter map#str_item;
  AstFilters.register_topphrase_filter map#str_item;
