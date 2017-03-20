module GovUk.Database

open GovUk
open GovUk.Config
open System
open System.IO
open System.Text
open System.Data
open System.Data.SqlClient
open Microsoft.FSharp.Reflection

// ------------------------------------------------------------------------------------------------
// Helpers for storing data to database - given F# record, generate SQL CREATE table scripts
// ------------------------------------------------------------------------------------------------

let tableName name = 
  let chars = 
    [| for c in name do 
        if Char.IsUpper c then yield '-'
        yield Char.ToLower(c) |]
  System.String(chars).Trim('-')
  
let rec getTables prefix typ = seq {
  for fld in FSharpType.GetRecordFields(typ) do
    if FSharpType.IsRecord(fld.PropertyType) then yield! getTables prefix fld.PropertyType
  yield prefix + "-" + tableName typ.Name }
  
let rec scriptTable (sb:StringBuilder) prefix typ =   
  let fields =
    [ for fld in FSharpType.GetRecordFields(typ) ->
        let name, typ = 
          if fld.PropertyType = typeof<string> then fld.Name, "nvarchar(1000)"
          elif fld.PropertyType = typeof<int> then fld.Name, "int"
          elif fld.PropertyType = typeof<float> then fld.Name, "float"
          elif fld.PropertyType = typeof<DateTimeOffset> then fld.Name, "datetimeoffset"
          elif fld.PropertyType = typeof<Guid> then fld.Name, "uniqueidentifier"
          elif FSharpType.IsRecord fld.PropertyType then 
            let idProp = FSharpType.GetRecordFields(fld.PropertyType) |> Seq.find (fun p -> p.Name = "ID")
            let idTyp = 
              if idProp.PropertyType = typeof<int> then "int"
              elif idProp.PropertyType = typeof<Guid> then "uniqueidentifier"
              elif idProp.PropertyType = typeof<string> then "nvarchar(1000)"
              else failwith "Unsupported ID type"
            fld.Name + "ID", idTyp
          else failwithf "Unsupported type: %s" fld.PropertyType.Name
        if FSharpType.IsRecord fld.PropertyType then
          scriptTable sb prefix fld.PropertyType
        if name = "ID" then sprintf "[ID] %s PRIMARY KEY NOT NULL" typ
        else sprintf "[%s] %s NOT NULL" name typ ]
  let fields = String.concat ",\n  " fields
  sprintf "CREATE TABLE dbo.[%s-%s] (\n  %s\n)\n\n" 
    prefix (tableName typ.Name) fields |> sb.Append |> ignore

let executeCommand sql = 
  use conn = new SqlConnection(Keys.TheGammaSqlConnection)
  conn.Open()
  use cmd = new SqlCommand(sql, conn)
  cmd.ExecuteNonQuery() |> ignore

let executeReader sql parse = 
  use conn = new SqlConnection(Keys.TheGammaSqlConnection)
  conn.Open()
  use cmd = new SqlCommand(sql, conn)
  use rdr = cmd.ExecuteReader() 
  let res = ResizeArray<_>()
  while rdr.Read() do res.Add(parse rdr)
  res

let executeScalarCommand sql = 
  use conn = new SqlConnection(Keys.TheGammaSqlConnection)
  conn.Open()
  use cmd = new SqlCommand(sql, conn)
  cmd.ExecuteScalar()

let initializeStorage<'T> prefix = 
  let sb = StringBuilder()
  scriptTable sb prefix (typeof<'T>) 
  executeCommand (sb.ToString())
  executeCommand 
    ( "CREATE EXTERNAL DATA SOURCE TheGammaStorage "  +
      sprintf "WITH (TYPE = BLOB_STORAGE, LOCATION = '%s')" storageAccount)

let cleanupStorage<'T> prefix =
  try executeCommand "DROP EXTERNAL DATA SOURCE TheGammaStorage" 
  with e -> printfn "Could not delete TheGammaStorage: %s"  e.Message
  for tab in getTables prefix typeof<'T> do
    try executeCommand (sprintf "DROP TABLE [%s]" tab)
    with e -> printfn "Could not delete table %s: %s" tab e.Message
      
