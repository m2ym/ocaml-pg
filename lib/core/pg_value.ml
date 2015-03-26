module Text = struct
  type t = string

  let of_int16 = Pervasives.string_of_int
  let of_int32 = Int32.to_string
  let of_int64 = Int64.to_string
  let of_int x =
    try Pervasives.string_of_int x
    with _ ->
      if Sys.word_size <= 32
      then of_int32 (Int32.of_int x)
      else of_int64 (Int64.of_int x)
  let of_float = string_of_float
  let of_string x = x

  let to_int16 = Pervasives.int_of_string
  let to_int32 = Int32.of_string
  let to_int64 = Int64.of_string
  let to_float = float_of_string
  let to_string t = t
end

type t = [
  | `Int of int
  | `Int16 of int
  | `Int32 of Int32.t
  | `Int64 of Int64.t
]

let text_encode = function
  | `Int x -> Text.of_int x
  | `Int16 x -> Text.of_int16 x
  | `Int32 x -> Text.of_int32 x
  | `Int64 x -> Text.of_int64 x
  | `String x -> Text.of_string x

let text_decode v ftype =
  let open Postgresql in
  match ftype with
  | INT2 -> `Int (Text.to_int16 v)
  | INT4 -> `Int32 (Text.to_int32 v)
  | INT8 -> `Int64 (Text.to_int64 v)
  | FLOAT4 -> `Float (Text.to_float v)
  | FLOAT8 -> `Float (Text.to_float v)
  | TEXT -> `String (Text.to_string v)
  | _ -> failwith "unsupported field type"
