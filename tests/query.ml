let () =
  let conn = Pg.connect ~conninfo:Sys.argv.(1) () in
  let tuples = Pg.exec conn ~params:[`Int 3] "SELECT $1::integer" in
  Format.printf "%d\n" (List.length tuples);
  Pg.close conn
