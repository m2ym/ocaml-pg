module Lwt_io = struct
  type 'a t = 'a Lwt.t
  type channel = Lwt_unix.file_descr

  let return = Lwt.return
  let bind = Lwt.bind
  let fail = Lwt.fail
  let catch = Lwt.catch

  let channel fd = Lwt_unix.of_unix_file_descr fd
  let poll ev fd f =
    let ev = match ev with `Read -> Lwt_unix.Read | `Write -> Lwt_unix.Write in
    Lwt.bind (Lwt_unix.wrap_syscall ev fd f) (fun x -> x)
end

module M = Pg.Make (Lwt_io)

include M
