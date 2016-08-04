open Camlp4.PreCast

(*

This file defines several macros related to the Logger module.

It is important to keep in mind that macros can't always be used like functions, therefor it's useful to know how these macros are expanded.

The general structure is the following (in which level can be any of Debug|Info|Notice|Warning|Error|Fatal):
Logger.level     ?exn section msg
Logger.level_    ?exn msg
Logger.level_f   ?exn section format_string and args
Logger.level_f_  ?exn format_string and args
Logger.ign_level    ?exn section msg
Logger.ign_level_   ?exn msg
Logger.ign_level_f  ?exn section format_string and args
Logger.ign_level_f_ ?exn format_string and args

The macros ending with _ grab a variable named section from the enviroment.
The macros starting with ign_ return unit instead of unit Lwt.t.
Macros containing _f wrap the 'format_string and args' part into a closure which can at a later time be evaluated to calculate a string.

Some concrete examples:

Logger.debug section "log this"
=> Logger.log section Logger.Debug "log this"

Logger.debug_ "log this"
=> Logger.log section Logger.Debug "log this"

Logger.debug_f section "format%s string" "bla"
=> Logger.log_ section Logger.Debug (fun () -> Printf.sprintf "format%s string" "bla")

Logger.debug_f_ "format%s string" "bla"
=> Logger.log_ section Logger.Debug (fun () -> Printf.sprintf "format%s string" "bla")


!!!! Important note !!!

Most of the time it's good when the arguments for a format string are captured in a closure, because when the arguments themselves are expensive to compute this cost might be avoided.
The catch however is that when the computation of the arguments depends on a variable that might be changed at a later time or for example a call to Unix.gettimeofday this will also be evaluated at a later time, possibly resulting in another log message than intended.

Bad examples (don't do this):
Logger.debug_f_ "Event bla happened at time %f" (Unix.gettimeofday ())
Logger.debug_f_ "bla bla %s" (string_of !counter)

*)

let rec apply e = function
  | [] -> e
  | x :: l -> let _loc = Ast.loc_of_expr x in apply <:expr< $e$ $x$ >> l

let split e =
  let rec aux acc = function
    | <:expr@_loc< Logger.log_f $section$ $level$ >> ->
      `Log_f(section, level, acc)
    | <:expr@_loc< Logger.ign_log_f $section$ $level$ >> ->
       `Ign_log_f(section, level, acc)
    | <:expr@_loc< Logger.$lid:func$ ~exn $arg$ >> ->
      let ign =
        String.length func >= 4 && "ign_" = String.sub func 0 4 in
      let func =
        if ign
        then
          String.sub func 4 (String.length func - 4)
        else
          func in
      let implicit_section = func.[String.length func - 1] = '_' in
      let pos = String.length func - 1 - (if implicit_section then 1 else 0) in
      let is_f = func.[pos] = 'f' in
      if not is_f
      then
        let level = String.sub func 0 (pos + 1) in
        if level = "log"
        then
          `No_match
        else
          let level' = String.capitalize_ascii level in
          if implicit_section
          then
            `Log_e_l_ (arg::acc, level', ign)
          else
            `Log_e_l(acc, level', arg, ign)
      else
        let level = String.sub func 0 (pos - 1) in
        let level' = String.capitalize_ascii level in
        if implicit_section
        then
          `Log_e_f_l_ (arg::acc, level', ign)
        else
          `Log_e_f_l (acc, level', arg, ign)
    | <:expr@_loc< Logger.$lid:func$ $arg$ >> ->
      let ign =
        String.length func >= 4 && "ign_" = String.sub func 0 4 in
      let func =
        if ign
        then
          String.sub func 4 (String.length func - 4)
        else
          func in
      let implicit_section = func.[String.length func - 1] = '_' in
      let pos = String.length func - 1 - (if implicit_section then 1 else 0) in
      let is_f = func.[pos] = 'f' in
      if not is_f
      then
        let level = String.sub func 0 (pos + 1) in
        if level = "log"
        then
          `No_match
        else
          let level' = String.capitalize_ascii level in
          if implicit_section
          then
            `Log_l_(arg::acc, level', ign)
          else
            `Log_l (acc, level', arg, ign)
      else
        let level = String.sub func 0 (pos - 1) in
        let level' = String.capitalize_ascii level in
        if implicit_section
        then
          `Log_f_l_ (arg::acc, level', ign)
        else
          `Log_f_l (acc, level', arg, ign)
    | <:expr@_loc< $a$ $b$ >> -> begin
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

    method! expr e =
      let _loc = Ast.loc_of_expr e in
      match split e with
        | `Log_f(section, level, args) ->
          let args = List.map super#expr args in
          <:expr<
            Logger.log_
            $section$
            $level$
            (fun () -> $apply <:expr< Printf.sprintf >> args$)
>>
        | `Ign_log_f(section, level, args) ->
          let args = List.map super#expr args in
          <:expr<
            Logger.ign_log_
            $section$
            $level$
            (fun () -> $apply <:expr< Printf.sprintf >> args$)
>>
        | `Log_l_(args, level, ign) ->
           let args = List.map super#expr args in
           if ign
           then
             apply
               <:expr<
                Logger.ign_log
                section
                Lwt_log.$uid:level$
                >> args
           else
             apply
               <:expr<
                Logger.log
                section
                Lwt_log.$uid:level$
                >> args
        | `Log_e_l_(args, level, ign) ->
           let args = List.map super#expr args in
           if ign
           then
             apply
               <:expr<
                Logger.ign_log
                ~exn
                section
                Lwt_log.$uid:level$
                >> args
           else
             apply
               <:expr<
                Logger.log
                ~exn
                section
                Lwt_log.$uid:level$
                >> args
        | `Log_l(args, level, section, ign) ->
           let args = List.map super#expr args in
           if ign
           then
             apply
               <:expr<
                Logger.ign_log
                $section$
                Lwt_log.$uid:level$
                >> args
           else
             apply
               <:expr<
                Logger.log
                $section$
                Lwt_log.$uid:level$
                >> args
        | `Log_e_l(args, level, section, ign) ->
           let args = List.map super#expr args in
           if ign
           then
             apply <:expr<
                    Logger.ign_log
                    ~exn
                    $section$
                    Lwt_log.$uid:level$
                    >> args
           else
             apply <:expr<
                    Logger.log
                    ~exn
                    $section$
                    Lwt_log.$uid:level$
                    >> args
        | `Log_f_l_(args, level, ign) ->
           let args = List.map super#expr args in
           if ign
           then
             <:expr<
              Logger.ign_log_
              section
              Lwt_log.$uid:level$
              (fun () -> $apply <:expr< Printf.sprintf >> args$)
>>
           else
             <:expr<
              Logger.log_
              section
              Lwt_log.$uid:level$
              (fun () -> $apply <:expr< Printf.sprintf >> args$)
>>
        | `Log_e_f_l_(args, level, ign) ->
           let args = List.map super#expr args in
           if ign
           then
             <:expr<
              Logger.ign_log_
              ~exn
              section
              Lwt_log.$uid:level$
              (fun () -> $apply <:expr< Printf.sprintf >> args$)
                                                         >>
           else
             <:expr<
              Logger.log_
              ~exn
              section
              Lwt_log.$uid:level$
              (fun () -> $apply <:expr< Printf.sprintf >> args$)
                                                         >>
        | `Log_f_l(args, level, section, ign) ->
           let args = List.map super#expr args in
           if ign
           then
             <:expr<
              Logger.ign_log_
              $section$
              Lwt_log.$uid:level$
              (fun () -> $apply <:expr< Printf.sprintf >> args$)
                                                         >>
           else
             <:expr<
              Logger.log_
              $section$
              Lwt_log.$uid:level$
              (fun () -> $apply <:expr< Printf.sprintf >> args$)
                                                         >>
        | `Log_e_f_l(args, level, section, ign) ->
           let args = List.map super#expr args in
           if ign
           then
             <:expr<
              Logger.ign_log_
              ~exn
              $section$
              Lwt_log.$uid:level$
              (fun () -> $apply <:expr< Printf.sprintf >> args$)
                                                         >>
           else
             <:expr<
              Logger.log_
              ~exn
              $section$
              Lwt_log.$uid:level$
              (fun () -> $apply <:expr< Printf.sprintf >> args$)
                                                         >>
| `No_match ->
super#expr e
end

let () =
  AstFilters.register_str_item_filter map#str_item;
  AstFilters.register_topphrase_filter map#str_item;
