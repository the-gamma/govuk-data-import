module GovUk.Cloud

open System
open MBrace
open MBrace.Core
open MBrace.Runtime

// ------------------------------------------------------------------------------------------------
// Assorted MBrace helpers - logging and retrying 
// ------------------------------------------------------------------------------------------------

let printLogs (task:CloudProcess<_>) = 
  for log in task.GetLogs() do
    printfn "[%s] %s" (log.DateTime.ToString("T")) log.Message

let (|TimeoutException|_|) (e:exn) =
  match e with 
  | :? System.TimeoutException -> Some()
  | :? System.Net.WebException as we when (we.InnerException :? System.TimeoutException) -> Some()
  | :? System.Data.SqlClient.SqlException as e when e.Number = -2 (* Timeout expired *) -> Some()
  | _ -> None

let retryOnTimeout initSleep initRetries f = local {
  let mutable retries = initRetries
  let mutable sleep = initSleep
  let mutable result = None
  let reset () = retries <- initRetries; sleep <- initSleep
  while result.IsNone do 
    try 
      let! r = f reset
      result <- Some r
    with e ->
      match e with
      | TimeoutException when retries > 0 ->
          do! Cloud.Logf "Timeout exception - remaining attempts %d, sleeping %dms" retries sleep
          do! Cloud.Sleep(sleep)
          sleep <- sleep * 2
          retries <- retries - 1
      | TimeoutException ->
          do! Cloud.Logf "Timeout exception - too many attempts - terminating"
          return raise (TimeoutException("Timeout exception - too many attempts"))
      | _ -> raise e 
  return result.Value }

