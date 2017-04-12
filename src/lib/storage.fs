module GovUk.Storage

open System
open System.IO
open System.Collections.Generic
open Microsoft.WindowsAzure.Storage
open FSharp.Data
open Microsoft.FSharp.Reflection

// ------------------------------------------------------------------------------------------------
// Helpers for writing data to storage & writing records to CSV-like temporary blob for BULK INSERT
// ------------------------------------------------------------------------------------------------

let getContainerReference () = 
  let account = CloudStorageAccount.Parse(Keys.TheGammaDataStorage)
  let cont = account.CreateCloudBlobClient().GetContainerReference(Config.container)
  if not (cont.Exists()) then failwithf "Container '%s' does not exist!" Config.container
  cont

let downloadFileToBlob url dir file = 
  let container = getContainerReference ()
  let blob = container.GetDirectoryReference(dir).GetBlockBlobReference(file)
  if not (blob.Exists()) then 
    let data = Http.RequestString(url,timeout=Config.requestTimeout)
    blob.UploadText(data)

let downloadBlobAsText dir file = 
  let container = getContainerReference ()
  let blob = container.GetDirectoryReference(dir).GetBlockBlobReference(file)
  if not (blob.Exists()) then failwithf "Blob '%s/%s' does not exist!" dir file
  blob.DownloadText()

let getBlob dir file = 
  let container = getContainerReference ()
  container.GetDirectoryReference(dir).GetBlockBlobReference(file)

let writeFileToBlob data dir file = 
  let container = getContainerReference ()
  let blob = container.GetDirectoryReference(dir).GetBlockBlobReference(file)
  if not (blob.Exists()) then 
    blob.UploadText(data)

let writeStreamToBlob stream dir file = 
  let container = getContainerReference ()
  let blob = container.GetDirectoryReference(dir).GetBlockBlobReference(file)
  if not (blob.Exists()) then 
    blob.UploadFromStream(stream)

let formatRecord typ = 
  let fields = 
    FSharpType.GetRecordFields(typ)
    |> Array.map (fun p -> 
      if not (FSharpType.IsRecord p.PropertyType) then (fun v -> p.GetValue(v).ToString())
      else 
        let idProp = FSharpType.GetRecordFields(p.PropertyType) |> Seq.find (fun p -> p.Name = "ID")
        (fun v -> idProp.GetValue(p.GetValue(v)).ToString() ) )
  fun (v:obj) ->
    fields |> Array.map (fun fld ->
      (fld v).Replace(',', ' ').Replace('\n', ' ')) |> String.concat ","

let writeRecordsToBlob dir file typ (records:seq<obj>) = 
  let container = getContainerReference ()
  let blob = container.GetDirectoryReference(dir).GetBlockBlobReference(file)
  if blob.Exists() then blob.Delete()
  let sb = System.Text.StringBuilder()
  let fm = formatRecord typ
  for recd in records do sb.Append(fm recd).Append('\n') |> ignore
  blob.UploadText(sb.ToString().Substring(0, sb.Length-1), System.Text.UTF8Encoding.UTF8)
  blob
