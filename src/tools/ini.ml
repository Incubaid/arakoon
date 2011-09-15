let p_string s = Scanf.sscanf s "%s" (fun s -> s)

let p_int s = Scanf.sscanf s "%i" (fun i -> i)

let p_string_list s = Str.split (Str.regexp "[, \t]+") s

let p_bool s = Scanf.sscanf s "%b" (fun b -> b)

let required section name = failwith  (Printf.sprintf "missing: %s %s" section name)

let default x = fun _ _ -> x

let get inifile section name s2a not_found =
    try
      let v_s = inifile # getval section name in
      s2a v_s
    with (Inifiles.Invalid_element _) -> not_found section name
