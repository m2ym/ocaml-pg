exception Error of string

module Value : sig
  type t = [
    | `Null
    | `Bool of bool
    | `Int of int
    | `Int16 of int
    | `Int32 of Int32.t
    | `Int64 of Int64.t
    | `Float of float
    | `String of string
    | `Bytea of string

    | `Bool_array of bool option list
    | `Int32_array of Int32.t option list
    | `Int64_array of Int64.t option list
    | `Float_array of float option list
    | `String_array of string option list

    | `Date of CalendarLib.Date.t
    | `Time of CalendarLib.Time.t
    | `Timestamp of CalendarLib.Calendar.t
    | `Timestamptz of CalendarLib.Calendar.t * CalendarLib.Time_Zone.t
    | `Interval of CalendarLib.Calendar.Period.t

    | `Json of Yojson.Safe.json
  ]

  val pp : Format.formatter -> t -> unit
  val to_string : t -> string

  module Text : sig
    val encode : t -> string
    val decode : string -> Postgresql.oid -> t
  end
end

class result :
  Postgresql.result ->
object
  method check : unit

  method to_array : Value.t array array
  method to_list : Value.t list list

  method pp : Format.formatter -> unit
  method to_string : string
end

module Result : sig
  type t = result

  val to_array : t -> Value.t array array
  val to_list : t -> Value.t list list

  val pp : Format.formatter -> t -> unit
  val to_string : t -> string
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

module Make : functor (IO : IO) ->
  Pg with type 'a monad = 'a IO.t

module Simple_io : IO

include Pg with type 'a monad = 'a
