open Postgresql

module type IO = sig
  type 'a t
  type channel

  val return : 'a -> 'a t
  val bind : 'a t -> ('a -> 'b t) -> 'b t
  val fail : exn -> 'a t

  val channel : Unix.file_descr -> channel
  val poll : [`Read | `Write] -> channel -> (unit -> 'a t) -> 'a t
end

module Make (IO : IO) = struct
  type t = {
    conn : connection;
    sock : IO.channel
  }

  exception Error of string

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

  let validate_result result =
    if result#status = Command_ok || result#status = Tuples_ok
    then return ()
    else fail (Error result#error)

  let exec t ?(params = []) query =
    let params = Array.of_list (List.map Pg_value.text_encode params) in
    try_with (fun () -> t.conn#send_query ~params query) >>= fun () ->
    get_single_result t >>= fun result ->
    validate_result result >>= fun () ->
    let ftypes = Array.init result#nfields (fun i -> result#ftype i) in
    return
      (List.map
         (fun tuple ->
            List.mapi
              (fun i value -> Pg_value.text_decode value ftypes.(i))
              tuple)
         result#get_all_lst)
end

module Simple_io = struct
  type 'a t = 'a
  type channel = Unix.file_descr

  let return x = x
  let bind x f = f x
  let fail = raise

  let channel fd = fd
  let poll ev fd f =
    match ev with
    | `Read -> ignore (Unix.select [fd] [] [] (-1.0)); f ()
    | `Write -> ignore (Unix.select [] [fd] [] (-1.0)); f ()
end

module M = Make (Simple_io)

include M
