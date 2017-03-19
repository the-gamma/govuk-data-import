﻿module GovUk.Import

open System
open System.Data.SqlClient
open System.Collections.Generic
open Microsoft.FSharp.Reflection

open GovUk
open GovUk.Storage
open GovUk.Database

// ------------------------------------------------------------------------------------------------
// Inserting records to SQL using BULK INSERT via temporary blob
// ------------------------------------------------------------------------------------------------

type InsertContext = 
  { Nested : Dictionary<string, System.Type * Dictionary<string, obj>>
    Inserted : Dictionary<string, HashSet<string>> }
  static member Create() = 
    { Nested = new Dictionary<_, _>() 
      Inserted = new Dictionary<_, _>() }

let insertRecords prefix (typ:System.Type) (records:seq<obj>) =
  let tableName = prefix + "-" + tableName typ.Name
  let blob = writeRecordsToBlob "temp" (tableName + ".txt") typ records
  let insert = 
    ( sprintf "BULK INSERT [%s] FROM '%s/temp/%s.txt' " tableName Config.container tableName ) +
    ( sprintf "WITH (DATA_SOURCE = 'TheGammaStorage', FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', TABLOCK)" )
  executeCommand insert
  blob.Delete()

let addNestedValue ctx (typ:System.Type) id value = 
  if not (ctx.Nested.ContainsKey typ.Name) then ctx.Nested.Add(typ.Name, (typ, new Dictionary<_, _>()))
  let _, vals = ctx.Nested.[typ.Name]
  if not (vals.ContainsKey(id)) then vals.Add(id, value)

let rec collectNested ctx (typ:System.Type) record = 
  for fld in FSharpType.GetRecordFields(typ) do
    if FSharpType.IsRecord(fld.PropertyType) then
      let idProp = FSharpType.GetRecordFields(fld.PropertyType) |> Seq.find (fun fld -> fld.Name = "ID")
      let value = fld.GetValue(record)
      let id = idProp.GetValue(value).ToString()
      addNestedValue ctx fld.PropertyType id value
      collectNested ctx fld.PropertyType value |> ignore
  ctx

let insertNestedRecords prefix ctx = 
  for KeyValue(typName, (typ, values)) in ctx.Nested do
    // If we do not have set of inserted IDs for this record yet, 
    // read the list of ids from the database (this should be smallish)
    if not (ctx.Inserted.ContainsKey(typName)) then
      let cmd = sprintf "SELECT [ID] FROM [%s-%s]" prefix (tableName typ.Name)
      let idProp = FSharpType.GetRecordFields(typ) |> Seq.find (fun fld -> fld.Name = "ID")
      let parse = 
        if idProp.PropertyType = typeof<int> then (fun (rdr:SqlDataReader) -> rdr.GetInt32(0).ToString())
        elif idProp.PropertyType = typeof<Guid> then (fun (rdr:SqlDataReader) -> rdr.GetGuid(0).ToString())
        else failwith "Unsupported type of ID"
      let ids = executeReader cmd parse
      ctx.Inserted.Add(typName, HashSet(ids))

    let inserted = ctx.Inserted.[typName]
    let toInsert = values |> Seq.choose (fun (KeyValue(id, value)) -> 
      if inserted.Contains id then None else Some(id, value)) |> Array.ofSeq
    if toInsert.Length > 0 then
      insertRecords prefix typ (Seq.map snd toInsert)
      for id, _ in toInsert do ctx.Inserted.[typName].Add(id) |> ignore
  ctx

let insertRecordsWithNested ctx prefix (records:seq<'T>) =
  let boxedRecords = Seq.map box records
  insertRecords prefix (typeof<'T>) boxedRecords
  records 
  |> Seq.fold (fun ctx recd -> collectNested ctx (typeof<'T>) recd) ctx 
  |> insertNestedRecords prefix
        
