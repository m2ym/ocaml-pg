let conn = Pg.connect ~conninfo:Sys.argv.(1) ()

let run ?params stmt = Pg.exec conn ?params stmt |> ignore

let query ?params query = Pg.exec conn ?params query

let show res = Format.printf "%t@." res#pp

let () =
  run {|CREATE TEMPORARY TABLE foo (id SERIAL PRIMARY KEY, x INTEGER NOT NULL, y TEXT NOT NULL)|};
  run {|INSERT INTO foo (x, y) VALUES ($1, $2)|} ~params:[`Int 42; `String "Hello"];
  show @@ query {|SELECT * FROM foo|};
  assert ((query {|SELECT x, y FROM foo|})#to_list = [[`Int32 42l; `String "Hello"]]);
  (try
     Pg.transact conn
       (fun _ ->
          run {|INSERT INTO foo (x, y) VALUES ($1, $2)|} ~params:[`Int 42; `String "Hello"];
          run {|INSERT INTO foo (x, y) VALUES ($1, $2)|} ~params:[`String "Hello"; `String "World"])
   with _ -> ());
  assert ((query {|SELECT x, y FROM foo|})#to_list = [[`Int32 42l; `String "Hello"]]);
  run {|DROP TABLE foo|}

let () =
  run {|CREATE TEMPORARY TABLE foo (json JSON)|};
  let json = `Assoc ["x", `Int 32;
                     "y", `List [`String "Hello"]] in
  run {|INSERT INTO foo (json) VALUES ($1)|} ~params:[`Json json];
  show @@ query {|SELECT * FROM foo|};
  assert ((query {|SELECT json FROM foo|})#to_list = [[`Json json]]);
  run {|DROP TABLE foo|}

let () = Pg.close conn
