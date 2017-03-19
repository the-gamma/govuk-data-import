#r "System.Transactions.dll"
#r "../packages/FSharp.Data/lib/net40/FSharp.Data.dll"
#load "../packages/FSharp.Azure.StorageTypeProvider/StorageTypeProvider.fsx"
#load "mbrace.fsx"
#load "../config/config.fs"

open System
open System.IO
open System.Data
open System.Data.SqlClient

open MBrace.Core
open FSharp.Data
open Microsoft.WindowsAzure.Storage

// ------------------------------------------------------------------------------------------------
// Get Azure cluster connection
// ------------------------------------------------------------------------------------------------

let cluster = Config.GetCluster()

cluster.ShowWorkers()
cluster.ShowProcesses()



// ------------------------------------------------------------------------------------------------
// Save raw CSV data into blob storage
// ------------------------------------------------------------------------------------------------

let container = "govuk"

let getContinerReference () = 
  let account = CloudStorageAccount.Parse(Config.TheGammaDataStorage)
  let cont = account.CreateCloudBlobClient().GetContainerReference(container)
  if not (cont.Exists()) then failwithf "Container '%s' does not exist!" container
  cont

let downloadFileToBlob url dir file = 
  let container = getContinerReference ()
  let blob = container.GetDirectoryReference(dir).GetBlockBlobReference(file)
  if not (blob.Exists()) then 
    use resp = Http.RequestStream(url).ResponseStream
    blob.UploadFromStream(resp)

let openFileFromBlob dir file = 
  let container = getContinerReference ()
  let blob = container.GetDirectoryReference(dir).GetBlockBlobReference(file)
  if blob.Exists() then blob.OpenRead()
  else failwithf "File '%s/%s' does not exist!" dir file

let down = 
  [ for year in 1995 .. 2017 -> cloud {
      do! Cloud.Logf "Starting download for year '%d'" year
      let file = sprintf "pp-%d.csv" year
      let url = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/" + file
      downloadFileToBlob url "land-registry-monthly-price-paid-data" file 
      do! Cloud.Logf "Finished download for year '%d'" year } ]
  |> Cloud.Parallel 
  |> cluster.CreateProcess

for log in down.GetLogs() do
  printfn "[%s] %s" (log.DateTime.ToString("T")) log.Message

down.ShowInfo()
down.Result


// ------------------------------------------------------------------------------------------------
// Copy data from CSV files into SQL table 
// ------------------------------------------------------------------------------------------------

let columns = 
  [ "TransactionId", typeof<Guid>
    "Price", typeof<Int32>
    "DateOfTransfer", typeof<DateTime>
    "Postcode", typeof<String>
    "PropertyType", typeof<String>
    "NewBuild", typeof<String>
    "Duration", typeof<String>
    "PAON", typeof<String>
    "SAON", typeof<String>
    "Street", typeof<String>
    "Locality", typeof<String>
    "TownCity", typeof<String>
    "District", typeof<String>
    "County", typeof<String>
    "Status", typeof<String> ]

let createTable () = 
  let table = new DataTable("new-data")
  for name, typ in columns do 
    table.Columns.Add(new DataColumn(DataType=typ, ColumnName=name))
  table.PrimaryKey <- [| table.Columns.[0] |] 
  table 

let writeTable conn table = local {
  use bulkCopy = new SqlBulkCopy((conn:SqlConnection), BulkCopyTimeout = 10)
  bulkCopy.DestinationTableName <- "[land-registry-monthly-price-paid-data]"
  do! Cloud.Logf "Inserting data table..."
  bulkCopy.WriteToServer(table:DataTable)
  let count = new SqlCommand("SELECT COUNT(*) FROM [land-registry-monthly-price-paid-data]", conn)
  do! Cloud.Logf "Inserted. Total count: %d" (count.ExecuteScalar() :?> int) }

let processYear year = local {
  do! Cloud.Logf "Opening blob data: %d" year
  let str = openFileFromBlob "land-registry-monthly-price-paid-data" (sprintf "pp-%d.csv" year)
  
  use conn = new SqlConnection(Config.TheGammaSqlConnection)
  conn.Open()
  use sql = new SqlCommand(sprintf "SELECT COUNT(*) FROM [land-registry-monthly-price-paid-data] WHERE YEAR([DateOfTransfer]) = %d" year, conn)
  let skip = sql.ExecuteScalar() :?> int    
  do! Cloud.Logf "Skiping %d records for year %d" skip year

  let mutable table = createTable ()
  let mutable count = 0
  for row in CsvFile.Load(str, hasHeaders=false).Rows |> Seq.skip skip do
    count <- count + 1
    if count % 100 = 0 then
      table.AcceptChanges()
      do! writeTable conn table
      table <- createTable ()

    let newRow = table.NewRow()
    for i, (name, typ) in Seq.indexed columns do
      let value = 
        if typ = typeof<Guid> then box (Guid.Parse(row.[i]))
        elif typ = typeof<DateTime> then box (DateTime.Parse(row.[i]))
        elif typ = typeof<Int32> then box (Int32.Parse(row.[i]))
        else box row.[i]
      newRow.[name] <- value
    table.Rows.Add(newRow) |> ignore

  do! Cloud.Logf "Inserting remaining data..." 
  table.AcceptChanges()
  do! writeTable conn table
  do! Cloud.Logf "All completed!"  }

let task = 
  [ for y in 1996 .. 1996 -> cloud { 
      try
        do! processYear y
      with e ->
        do! Cloud.Logf "Something went wrong for year %d: %A" y e } ]
  |> Cloud.Parallel
  |> cluster.CreateProcess

for log in task.GetLogs() do
  printfn "[%s] %s" (log.DateTime.ToString("T")) log.Message

task.ShowInfo()


let counts = cloud {
  let res = ResizeArray<_>()
  use conn = new SqlConnection(Config.TheGammaSqlConnection)
  conn.Open()
  for year in 1995 .. 2017 do
    do! Cloud.Logf "Counting records in year: %d" year
    let str = openFileFromBlob "land-registry-monthly-price-paid-data" (sprintf "pp-%d.csv" year)
    let csvCount = CsvFile.Load(str, hasHeaders=false).Rows |> Seq.length 
    let sql = sprintf "SELECT COUNT(*) FROM [land-registry-monthly-price-paid-data] WHERE YEAR([DateOfTransfer]) = %d" year
    let cmd = new SqlCommand(sql, conn)
    let dbCount = cmd.ExecuteScalar() :?> int
    do! Cloud.Logf "CSV count: %d, DB count: %d" csvCount dbCount
    res.Add((year, csvCount, dbCount))
  return Array.ofSeq res } |> cluster.CreateProcess

for log in counts.GetLogs() do
  printfn "[%s] %s" (log.DateTime.ToString("T")) log.Message

counts.ShowInfo()






let stuff = 
  CsvFile.Parse(IO.File.ReadAllText(@"C:\Users\Tomas\Downloads\pp-1995.csv"), hasHeaders=false).Rows 
  |> Seq.skip 717399
  |> Seq.take 100
  |> Seq.toArray


stuff 
|> Seq.sortByDescending (fun o -> (sprintf "%A" o).Length)
|> Seq.iter (printfn "%A")