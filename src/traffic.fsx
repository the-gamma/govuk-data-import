#nowarn "58"
#r "System.Transactions.dll"
#r "../packages/FSharp.Data/lib/net40/FSharp.Data.dll"
#r "../packages/DotSpatial.Projections/lib/net40-Client/DotSpatial.Projections.dll"
#r "System.IO.Compression.dll"
#r "System.IO.Compression.FileSystem.dll"
#load "../packages/FSharp.Azure.StorageTypeProvider/StorageTypeProvider.fsx"
#load "mbrace.fsx"
#load "config/keys.fs" "lib/common.fs" "lib/config.fs" "lib/database.fs" 
  "lib/storage.fs" "lib/cloud.fs" "lib/import.fs"

open System
open System.IO.Compression
open System.Collections.Generic

open GovUk
open MBrace
open MBrace.Core
open MBrace.Runtime
open FSharp.Data
open DotSpatial.Projections

// ------------------------------------------------------------------------------------------------
// Domain model - also defines the structure of the database tables
// ------------------------------------------------------------------------------------------------

type Region = 
  { ID : int 
    Region : string }

type Location = 
  { ID : int 
    Location : string }

type Road =  
  { ID : int
    Latitude : float // NS
    Longitude : float // WE
    Road : string }

type DailyMeasurement =
  { ID : int
    Location : Location
    Region : Region
    Road : Road
    Day : int
    Month : int
    Year : int
    PedalCycles : int
    MotorVehicles : int }

// ------------------------------------------------------------------------------------------------
// Download raw data from dft.gov.uk and unzip it
// ------------------------------------------------------------------------------------------------

let downloadRawCounts () = cloud {
  do! Cloud.Logf "Starting download from dft.gov.uk"
  let req = Http.RequestStream("http://data.dft.gov.uk/gb-traffic-matrix/Raw_count_data_major_roads.zip")
  do! Cloud.Logf "Extracting compressed file"
  let zip = new ZipArchive(req.ResponseStream)
  let entity = zip.Entries |> Seq.head
  do! Cloud.Logf "Saving decompressed file"
  let csv = entity.Open()
  Storage.writeStreamToBlob csv "gb-road-traffic-counts" "raw.csv"
  do! Cloud.Logf "Raw data saved!" }


// ------------------------------------------------------------------------------------------------
// Calculate daily sums and
// ------------------------------------------------------------------------------------------------

type Traffic = 
  CsvProvider<(const(__SOURCE_DIRECTORY__ + "/samples/traffic.csv")), 
    CacheRows=false, AssumeMissingValues=true, PreferOptionals=true>

let makeGeoConverter () =
  let source = ProjectionInfo.FromProj4String("+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +ellps=airy +towgs84=446.448,-125.157,542.060,0.1502,0.2470,0.8421,-20.4894 +units=m +no_defs")
  let dest = KnownCoordinateSystems.Geographic.World.WGS1984
  fun e n ->
    let xy = [|e; n|]
    Reproject.ReprojectPoints(xy, [| 0.0 |], source, dest, 0, 1)
    xy.[0], xy.[1]

let insertMeasurements () = cloud {
  let geoConv = makeGeoConverter ()
  let blob = Storage.getBlob "gb-road-traffic-counts" "raw.csv"
  let csv = Traffic.Load(blob.OpenRead())

  do! Cloud.Logf "Reading and parsing measurements"  

  let regions = System.Collections.Generic.Dictionary<_, _>()
  let locations = System.Collections.Generic.Dictionary<_, _>()
  let getLocation loc = 
    if not (locations.ContainsKey(loc)) then 
      locations.Add(loc, { Location.ID = locations.Count; Location = loc })    
    locations.[loc]
  let getRegion reg = 
    if not (regions.ContainsKey(reg)) then 
      regions.Add(reg, { Region.ID = regions.Count; Region = reg })    
    regions.[reg]

  let measurements = 
    csv.Rows
    |> Seq.chunkWhile (fun r1 r2 -> r1.CP = r2.CP)
    |> Seq.mapi (fun idx chunk ->
      let r = chunk.[0]
      match r.``S Ref E``, r.``S Ref N``, r.``Region Name (GO)``, r.``ONS LA Name``, r.Road with
      | Some re, Some rn, Some reg, Some name, Some road ->   
          let road = 
            match defaultArg r.``A-Junction`` "", defaultArg r.``B-Junction`` "" with
            | "", "" -> road
            | (j, "") | ("", j) -> sprintf "%s (%s)" road j
            | aj, bj -> sprintf "%s (%s - %s)" road aj bj
          let long, lat = geoConv (float re) (float rn)
          let loc = getRegion reg, getLocation name, { Road.ID = idx; Road = road; Latitude = lat; Longitude = long }
          chunk 
          |> Seq.filter (fun r -> r.DCount.IsSome)
          |> Seq.groupBy (fun r -> r.DCount.Value)
          |> Seq.map (fun (d, g) -> loc, d, g)
      | _ -> Seq.empty )
    |> Seq.concat
    |> Array.ofSeq

  do! Cloud.Logf "Calculating daily measurements"  
  let daily = measurements |> Array.mapi (fun idx ((reg, loc, rd), date, group) ->
    { DailyMeasurement.ID = idx; Location = loc; Region = reg; Road = rd
      Day = date.Day; Month = date.Month; Year = date.Year
      PedalCycles = group |> Seq.sumBy (fun r -> defaultArg r.PC 0)
      MotorVehicles = group |> Seq.sumBy (fun r -> defaultArg r.AMV 0) })
  
  do! Cloud.Logf "Inserting daily measurements"  
  let ctx = Import.InsertContext.Create()
  let _ = Import.insertRecordsWithNested ctx "gb-road-traffic-counts" daily

  do! Cloud.Logf "Finished processing %d records" measurements.Length }


// ------------------------------------------------------------------------------------------------
// Actually run things!
// ------------------------------------------------------------------------------------------------

let cluster = Config.GetCluster()
cluster.ShowWorkers()
cluster.ShowProcesses()

// Download raw data from the server and unzip it into blob storage
let p1 = downloadRawCounts () |> cluster.CreateProcess
p1.Status
p1.Result
p1.ShowInfo()

Cloud.printLogs p1

// Parse the downloaded CSV and insert daily & monthly sums into database
Database.cleanupStorage "gb-road-traffic-counts" [typeof<DailyMeasurement>]
Database.initializeStorage "gb-road-traffic-counts" [typeof<DailyMeasurement>]
Database.initializeExternalBlob ()

let p2 = insertMeasurements () |> cluster.CreateProcess
p2.Status
p2.Result
p2.ShowInfo()

Cloud.printLogs p2


// Count how many records have we inserted already...
"SELECT Count(*) FROM [gb-road-traffic-counts-daily-measurement]" |> Database.executeScalarCommand
"SELECT Count(*) FROM [gb-road-traffic-counts-location]" |> Database.executeScalarCommand
"SELECT Count(*) FROM [gb-road-traffic-counts-road]" |> Database.executeScalarCommand
"SELECT Count(*) FROM [gb-road-traffic-counts-region]" |> Database.executeScalarCommand

// Create some indices over the table to make filtering by time, station & pollutant faster
"CREATE NONCLUSTERED INDEX IX_YearMonthDay ON [gb-road-traffic-counts-daily-measurement] (Year,Month,Day)" 
|> Database.executeCommandWithTimeout (60 * 15)
"CREATE NONCLUSTERED INDEX IX_LocationID ON [gb-road-traffic-counts-daily-measurement] (LocationID)" 
|> Database.executeCommandWithTimeout (60 * 15)
"CREATE NONCLUSTERED INDEX IX_RegionID ON [gb-road-traffic-counts-daily-measurement] (RegionID)" 
|> Database.executeCommandWithTimeout (60 * 15)
"CREATE NONCLUSTERED INDEX IX_RoadID ON [gb-road-traffic-counts-daily-measurement] (RoadID)" 
|> Database.executeCommandWithTimeout (60 * 15)

// This is how to drop some index in case we do not actually want it!
"DROP INDEX IX_YearMonthDay ON [gb-road-traffic-counts-daily-measurement]" 
|> Database.executeCommand