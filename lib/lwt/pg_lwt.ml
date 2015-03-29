module Lwt_io = struct
  type 'a t = 'a Lwt.t
  type channel = Lwt_unix.file_descr

  let return = Lwt.return
  let bind = Lwt.bind
  let fail = Lwt.fail
  let catch = Lwt.catch

  let channel fd = Lwt_unix.of_unix_file_descr ~blocking:false ~set_flags:false fd
  let poll ev ch =
    let ev = match ev with `Read -> Lwt_unix.Read | `Write -> Lwt_unix.Write in
    Lwt_unix.check_descriptor ch;
    Lwt_unix.register_action ev ch (fun x -> x)
end

module M = Pg.Make (Lwt_io)

include M
