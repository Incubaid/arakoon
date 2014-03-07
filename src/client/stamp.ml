
type t = int64

let stamp_to buffer (t:t) = Llio.int64_to buffer t

let input_stamp ic = Llio.input_int64 ic

let to_s = Int64.to_string

let (<=) i j = Int64.compare i j <= 0
