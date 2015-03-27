open Postgresql
open CalendarLib

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

    | `Bool_array of bool option array
    | `Int32_array of Int32.t option array
    | `Int64_array of Int64.t option array
    | `Float_array of float option array
    | `String_array of string option array

    | `Date of Date.t
    | `Time of Time.t
    | `Timestamp of Calendar.t
    | `Timestamptz of Calendar.t * Time_Zone.t
    | `Interval of Calendar.Period.t

    | `Json of Yojson.Basic.json
  ]

  module Text = struct
    (* TODO remove PGOCaml depepdency *)
    module C = PGOCaml

    let encode = function
      | `Null -> "NULL"
      | `Bool x -> C.string_of_bool x
      | `Int x -> C.string_of_int x
      | `Int16 x -> C.string_of_int16 x
      | `Int32 x -> C.string_of_int32 x
      | `Int64 x -> C.string_of_int64 x
      | `Float x -> C.string_of_float x
      | `String x -> C.string_of_string x

      | `Bool_array x -> C.string_of_bool_array (Array.to_list x)
      | `Int32_array x -> C.string_of_int32_array (Array.to_list x)
      | `Int64_array x -> C.string_of_int64_array (Array.to_list x)
      | `Float_array x -> C.string_of_float_array (Array.to_list x)
      | `String_array x -> C.string_of_string_array (Array.to_list x)

      | `Date x -> C.string_of_date x
      | `Time x -> C.string_of_time x
      | `Timestamp x -> C.string_of_timestamp x
      | `Timestamptz x -> C.string_of_timestamptz x
      | `Interval x -> C.string_of_interval x

      | `Json x -> Yojson.Basic.to_string x

    let decode s oid : t =
      let open Postgresql in
      match oid with
      | 16 ->
        (* BOOL *)
        `Bool (C.bool_of_string s)
      | 21 ->
        (* INT2 *)
        `Int (C.int16_of_string s)
      | 23 ->
        (* INT4 *)
        `Int32 (C.int32_of_string s)
      | 20 ->
        (* INT8 *)
        `Int64 (C.int64_of_string s)
      | 700 ->
        (* FLOAT4 *)
        `Float (C.float_of_string s)
      | 701 ->
        (* FLOAT8 *)
        `Float (C.float_of_string s)
      | 1042 ->
        (* BPCHAR *)
        `String (C.string_of_string s)
      | 1043 ->
        (* VARCHAR *)
        `String (C.string_of_string s)
      | 25 ->
        (* TEXT *)
        `String (C.string_of_string s)
      | 1000 ->
        (* BOOLARRAY *)
        `Bool_array (Array.of_list (C.bool_array_of_string s))
      | 1007 ->
        (* INT4ARRAY *)
        `Int32_array (Array.of_list  (C.int32_array_of_string s))
      | 1016 ->
        (* INT8ARRAY *)
        `Int64_array (Array.of_list (C.int64_array_of_string s))
      | 1021 ->
        (* FLOAT4ARRAY *)
        `Float_array (Array.of_list (C.float_array_of_string s))
      | 1022 ->
        (* FLOAT8ARRAY *)
        `Float_array (Array.of_list (C.float_array_of_string s))
      | 1009 ->
        (* TEXTARRAY *)
        `String_array (Array.of_list (C.string_array_of_string s))
      | 1015 ->
        (* VARCHARARRAY *)
        `String_array (Array.of_list (C.string_array_of_string s))

      | 1082 ->
        (* DATE *)
        `Date (C.date_of_string s)
      | 1083 ->
        (* TIME *)
        `Time (C.time_of_string s)
      | 1114 ->
        (* TIMESTAMP *)
        `Timestamp (C.timestamp_of_string s)
      | 1184 ->
        (* TIMESTAMPTZ *)
        `Timestamptz (C.timestamptz_of_string s)
      | 1186 ->
        (* INTERVAL *)
        `Interval (C.interval_of_string s)

      | 114 ->
        (* JSON *)
        `Json (Yojson.Basic.from_string s)
      | _ -> failwith (Printf.sprintf "unsupported field type oid: %d" oid)
  end

  let to_string t = Text.encode t

  let pp ppf t = Format.pp_print_string ppf (to_string t)
end

class result res =
  let foids = Array.init res#nfields res#ftype_oid in
object (self)
  method check =
    if not (res#status = Command_ok || res#status = Tuples_ok) then
      raise (Error res#error)

  method to_array =
    Array.map
      (fun tuple ->
         Array.mapi
           (fun i s -> Value.Text.decode s foids.(i))
           tuple)
      res#get_all

  method to_list =
    List.map
      (fun tuple ->
         List.mapi
           (fun i s -> Value.Text.decode s foids.(i))
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
