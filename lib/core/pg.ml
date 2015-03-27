open Postgresql

exception Error of string

module Value = struct
  type t = [
    | `Null
    | `Bool of bool
    | `Int of int
    | `Int16 of int
    | `Int32 of Int32.t
    | `Int64 of Int64.t
    | `Float of float
    | `String of string
    | `Json of Yojson.Basic.json
  ]

  let pp ppf =
    let open Format in
    function
    | `Null -> pp_print_string ppf "null"
    | `Bool x -> pp_print_bool ppf x
    | `Int x -> pp_print_int ppf x
    | `Int16 x -> pp_print_int ppf x
    | `Int32 x -> fprintf ppf "%ld" x
    | `Int64 x -> fprintf ppf "%Ld" x
    | `Float x -> pp_print_float ppf x
    | `String x -> fprintf ppf "%S" x
    | `Json x -> pp_print_string ppf (Yojson.Basic.pretty_to_string x)

  let to_string t = Format.asprintf "%a" pp t

  module Text = struct
    let null = "NULL"

    let of_bool = string_of_bool
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
    let of_json x = Yojson.Basic.to_string x

    let to_bool = bool_of_string
    let to_int16 = Pervasives.int_of_string
    let to_int32 = Int32.of_string
    let to_int64 = Int64.of_string
    let to_float = float_of_string
    let to_string s = s
    let to_json s = Yojson.Basic.from_string s

    let encode = function
      | `Null -> null
      | `Bool x -> of_bool x
      | `Int x -> of_int x
      | `Int16 x -> of_int16 x
      | `Int32 x -> of_int32 x
      | `Int64 x -> of_int64 x
      | `Float x -> of_float x
      | `String x -> of_string x
      | `Json x -> of_json x

    let decode s ftype : t =
      let open Postgresql in
      match ftype with
      | BOOL -> `Bool (to_bool s)
      | INT2 -> `Int (to_int16 s)
      | INT4 -> `Int32 (to_int32 s)
      | INT8 -> `Int64 (to_int64 s)
      | FLOAT4 -> `Float (to_float s)
      | FLOAT8 -> `Float (to_float s)
      | CHAR -> `String (to_string s)
      | VARCHAR -> `String (to_string s)
      | TEXT -> `String (to_string s)
      | JSON -> `Json (to_json s)
      | _ -> failwith (Printf.sprintf "unsupported field type: %s" (Postgresql.string_of_ftype ftype))
  end
end

class result res =
  let ftypes = Array.init res#nfields res#ftype in
object (self)
  method check =
    if not (res#status = Command_ok || res#status = Tuples_ok) then
      raise (Error res#error)

  method to_array =
    Array.map
      (fun tuple ->
         Array.mapi
           (fun i s -> Value.Text.decode s ftypes.(i))
           tuple)
      res#get_all

  method to_list =
    List.map
      (fun tuple ->
         List.mapi
           (fun i s -> Value.Text.decode s ftypes.(i))
           tuple)
      res#get_all_lst

  method pp ppf =
    let nfields = res#nfields in
    let widths = Array.make nfields 0 in
    let header =
      Array.to_list
        (Array.init nfields
           (fun i ->
              let s = res#fname i in
              widths.(i) <- String.length s;
              s))
    in
    let body =
      List.map 
        (fun tuple ->
           List.mapi
             (fun i value ->
                let s = Value.to_string value in
                let len = String.length s in
                if widths.(i) < len then
                  widths.(i) <- len;
                s)
             tuple)
        self#to_list
    in
    let spaces = String.make 1000 ' ' in
    let pad i s = s ^ String.sub spaces 0 (max 0 (widths.(i) - String.length s)) in
    let row tuple = String.concat " | " (List.mapi pad tuple) in
    Format.fprintf ppf "%s@." (row header);
    Format.fprintf ppf "%s@." (String.make (String.length (row header)) '-');
    List.iter
      (fun tuple -> Format.fprintf ppf "%s@." (row tuple))
      body

  method to_string = Format.asprintf "%t" self#pp
end

module Result = struct
  type t = result

  let to_array t = t#to_array
  let to_list t = t#to_list

  let pp ppf t = t#pp ppf
  let to_string t = t#to_string
end

module type IO = sig
  type 'a t
  type channel

  val return : 'a -> 'a t
  val bind : 'a t -> ('a -> 'b t) -> 'b t
  val fail : exn -> 'a t
  val catch : (unit -> 'a t) -> (exn -> 'a t) -> 'a t

  val channel : Unix.file_descr -> channel
  val poll : [ `Read | `Write ] -> channel -> (unit -> 'a t) -> 'a t
end

module type Pg = sig
  type t
  type 'a monad
  type isolation = [ `Serializable | `Repeatable_read | `Read_committed | `Read_uncommitted ]
  type access = [ `Read_write | `Read_only ]

  val connect :
    ?host : string ->
    ?hostaddr : string ->
    ?port : string ->
    ?dbname : string ->
    ?user : string ->
    ?password : string ->
    ?options : string ->
    ?tty : string ->
    ?requiressl : string ->
    ?conninfo : string ->
    unit ->
    t monad

  val close : t -> unit monad

  val connection : t -> Postgresql.connection
  val status : t -> Postgresql.connection_status

  val exec : t -> ?check_result : bool -> ?params : Value.t list -> string -> Result.t monad

  val begin_work : ?isolation : isolation -> ?access : access -> ?deferrable : bool -> t -> unit monad
  val commit : t -> unit monad
  val rollback : t -> unit monad
  val transact : t -> ?isolation : isolation -> ?access : access -> ?deferrable : bool -> (t -> 'a monad) -> 'a monad
end

module Make (IO : IO) = struct
  type t = {
    conn : connection;
    sock : IO.channel
  }
  type 'a monad = 'a IO.t
  type isolation = [ `Serializable | `Repeatable_read | `Read_committed | `Read_uncommitted ]
  type access = [ `Read_write | `Read_only ]

  let return = IO.return
  let (>>=) = IO.bind
  let fail = IO.fail

  let try_with f =
    try return (f ()) with exn -> fail exn

  let connect
      ?host ?hostaddr ?port ?dbname ?user ?password
      ?options ?tty ?requiressl ?conninfo
      () =
    try
      let conn =
        new connection
          ?host ?hostaddr ?port ?dbname ?user ?password
          ?options ?tty ?requiressl ?conninfo ~startonly:true ()
      in
      let sock = IO.channel (Obj.magic conn#socket) in
      let t = { conn; sock } in
      let rec work = function
        | Polling_failed ->
          fail (Error "polling failed")
        | Polling_reading ->
          cont `Read
        | Polling_writing ->
          cont `Write
        | Polling_ok ->
          if conn#status = Ok then begin
            conn#set_nonblocking true;
            return t
          end else
            fail (Error "connection failed")
      and cont ev =
        IO.poll ev t.sock (fun () -> try_with (fun () -> conn#connect_poll) >>= work)
      in cont `Write
    with exn -> fail exn

  let close t = try_with (fun () -> t.conn#finish)

  let connection t = t.conn

  let status t = t.conn#status

  let rec get_result t =
    try
      t.conn#consume_input;
      if t.conn#is_busy
      then IO.poll `Read t.sock (fun () -> get_result t)
      else return t.conn#get_result
    with exn ->
      fail exn

  let get_results t =
    let rec loop acc =
      get_result t >>= function
      | None -> return (List.rev acc)
      | Some result -> loop (result :: acc)
    in loop []

  let get_single_result t =
    get_results t >>= fun results ->
    match results with
    | [result] -> return result
    | _ -> fail (Error "invalid number of results")

  let exec t ?(check_result = true) ?(params = []) query =
    let params = Array.of_list (List.map Value.Text.encode params) in
    try_with (fun () -> t.conn#send_query ~params query) >>= fun () ->
    get_single_result t >>= fun res ->
    let res = new result res in
    if check_result then res#check;
    return res

  let begin_work ?isolation ?access ?deferrable t =
    let isolation_str =
      match isolation with
      | None -> ""
      | Some x -> " isolation level " ^ (
          match x with
          | `Serializable -> "serializable"
          | `Repeatable_read -> "repeatable read"
          | `Read_committed -> "read committed"
          | `Read_uncommitted -> "read uncommitted"
        )
    and access_str = match access with
      | None -> ""
      | Some `Read_write -> " read write"
      | Some `Read_only -> " read only"
    and deferrable_str = match deferrable with
      | None -> ""
      | Some x -> (match x with true -> "" | false -> " not") ^ " deferrable" in
    exec t ("begin work" ^ isolation_str ^ access_str ^ deferrable_str) >>= fun _ ->
    return ()

  let commit t =
    exec t "commit" >>= fun _ -> return ()

  let rollback t =
    exec t "rollback" >>= fun _ -> return ()

  let transact t ?isolation ?access ?deferrable f =
    begin_work ?isolation ?access ?deferrable t >>= fun () ->
    IO.catch
      (fun () ->
         f t >>= fun res ->
         commit t >>= fun () ->
         return res)
      (fun exn ->
         rollback t >>= fun () ->
         fail exn)
end

module Simple_io = struct
  type 'a t = 'a
  type channel = Unix.file_descr

  let return x = x
  let bind x f = f x
  let fail = raise
  let catch f g = try f () with exn -> g exn

  let channel fd = fd
  let poll ev fd f =
    match ev with
    | `Read -> ignore (Unix.select [fd] [] [] (-1.0)); f ()
    | `Write -> ignore (Unix.select [] [fd] [] (-1.0)); f ()
end

module M = Make (Simple_io)

include M
