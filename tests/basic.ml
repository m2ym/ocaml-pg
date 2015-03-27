module Make = functor (IO : Pg.IO) -> struct
  module P = Pg.Make (IO)

  let run () =
    let (>>=) = IO.bind
    and (return) = IO.return in

    P.connect ~conninfo:Sys.argv.(1) () >>= fun conn ->
    let exec ?params query = P.exec conn ?params query
    and show t = t >>= fun res -> Format.printf "%t@." res#pp; return () in
    let test t expected = t >>= fun res -> assert (res#to_list = expected); return () in

    (* null *)
    test (exec {|SELECT NULL::BOOLEAN|}) [[`Null]] >>= fun () ->

    (* transaction *)
    exec {|CREATE TEMPORARY TABLE temp (id SERIAL PRIMARY KEY, x INTEGER NOT NULL, y TEXT NOT NULL)|} >>= fun _ ->
    exec {|INSERT INTO temp (x, y) VALUES ($1, $2)|} ~params:[`Int 42; `String "Hello"] >>= fun _ ->
    show (exec {|SELECT * FROM temp|}) >>= fun () ->
    test (exec {|SELECT x, y FROM temp|}) [[`Int32 42l; `String "Hello"]] >>= fun () ->
    IO.catch
      (fun () ->
         P.transact conn
           (fun _ ->
              exec {|INSERT INTO temp (x, y) VALUES ($1, $2)|} ~params:[`Int 42; `String "Hello"] >>= fun _ ->
              exec {|INSERT INTO temp (x, y) VALUES ($1, $2)|} ~params:[`String "Hello"; `String "World"] >>= fun _ ->
              return ()))
      (fun _ -> return ()) >>= fun () ->
    test (exec {|SELECT x, y FROM temp|}) [[`Int32 42l; `String "Hello"]] >>= fun () ->
    exec {|DROP TABLE temp|} >>= fun _ ->

    (* json *)
    exec {|CREATE TEMPORARY TABLE temp (json JSON)|} >>= fun _ ->
    let json = `Assoc ["x", `Int 32;
                       "y", `List [`String "Hello"]] in
    exec {|INSERT INTO temp (json) VALUES ($1)|} ~params:[`Json json] >>= fun _ ->
    show (exec {|SELECT * FROM temp|}) >>= fun _ ->
    test (exec {|SELECT json FROM temp|}) [[`Json json]] >>= fun _ ->
    exec {|DROP TABLE temp|} >>= fun _ ->

    (* large data *)
    exec {|CREATE TEMPORARY TABLE temp (id INTEGER, a SERIAL, b SERIAL, c SERIAL, d TEXT)|} >>= fun _ ->
    exec {|INSERT INTO temp (id, d) SELECT id, $1 FROM generate_series(1, 100000) id|} ~params:[`String (String.make 1000 'x')] >>= fun _ ->
    exec {|SELECT * FROM temp|} >>= fun _ ->
    exec {|DROP TABLE temp|} >>= fun _ ->

    P.close conn
end

let () =
  let module M = Make (Pg.Simple_io) in
  M.run ()

let () =
  let module M = Make (Pg_lwt.Lwt_io) in
  Lwt_main.run (M.run ())
