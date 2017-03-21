#r "System.Transactions.dll"
#r "../packages/FSharp.Data/lib/net40/FSharp.Data.dll"
#load "../packages/FSharp.Azure.StorageTypeProvider/StorageTypeProvider.fsx"
#load "mbrace.fsx"
#load "config/keys.fs" "lib/config.fs" "lib/database.fs" 
  "lib/storage.fs" "lib/cloud.fs" "lib/import.fs"

open System
open System.Collections.Generic

open GovUk
open MBrace
open MBrace.Core
open MBrace.Runtime
open FSharp.Data

// ------------------------------------------------------------------------------------------------
// Domain model - also defines the structure of the database tables
// ------------------------------------------------------------------------------------------------

type Pollutant =
  { ID : int
    Label : string
    Definition : string
    Notation : string }

type Station = 
  { ID : string
    Name : string
    Latitude : float // NS
    Longitude : float } // WE

type RawMeasurement = 
  { ID : Guid
    Pollutant : Pollutant
    Station : Station
    Time : DateTimeOffset
    Value : float }

type DailyMeasurement = 
  { ID : Guid
    Pollutant : Pollutant
    Station : Station
    Year : int
    Month : int
    Day : int
    Value : float }

type MonthlyMeasurement = 
  { ID : Guid
    Pollutant : Pollutant
    Station : Station
    Year : int
    Month : int
    Value : float }


// ------------------------------------------------------------------------------------------------
// Scripts for crawling Defra and storing data into Blob storage
// ------------------------------------------------------------------------------------------------

type Pollutants = CsvProvider<"http://dd.eionet.europa.eu/vocabulary/aq/pollutant/csv">
type Year = XmlProvider<"https://uk-air.defra.gov.uk/data/atom-dls/auto/2017/atom.en.xml">
type Data = XmlProvider<"http://uk-air.defra.gov.uk/data/atom-dls/observations/auto/GB_FixedObservations_2017_BIR1.xml">

let removePrefix prefix (s:string) = 
  if s.StartsWith(prefix) then s.Substring(prefix.Length) else s

let removeAfter substr (s:string) = 
  let i = s.LastIndexOf(substr:string)
  if i > 0 then s.Substring(0, i) else s

let removeBefore substr (s:string) = 
  let i = s.IndexOf(substr:string)
  if i > 0 then s.Substring(i+substr.Length) else s

// Download all known pollutants and build a lookup dictionary

let getPollutantsId (url:string) = 
  if url.StartsWith("http://dd.eionet.europa.eu/vocabulary/aq/pollutant/") then
    url.Substring("http://dd.eionet.europa.eu/vocabulary/aq/pollutant/".Length) |> int
  else failwithf "Invalid pollutant: %s" url

let getPollutants () = 
  let pollutantUrl = "http://dd.eionet.europa.eu/vocabulary/aq/pollutant/csv"
  let pollutantData = Http.RequestString(pollutantUrl, timeout=Config.requestTimeout)
  let pollutantsCsv = Pollutants.Parse(pollutantData)
  [ for row in pollutantsCsv.Rows -> 
      row.URI,
      { Pollutant.ID = getPollutantsId row.URI; Label = row.Label; 
        Definition = row.Definition; Notation = row.Notation } ] |> dict

// Download raw XML files and save them in Azre blobs for further processing

let getStations yearNumber = 
  let yearUrl = sprintf "https://uk-air.defra.gov.uk/data/atom-dls/auto/%d/atom.en.xml" yearNumber
  let yearData = Http.RequestString(yearUrl, timeout=Config.requestTimeout)
  Storage.writeFileToBlob yearData "defra-airquality" (string yearNumber + ".xml")
  let year = Year.Parse(yearData)
  let stations = 
    [ for entry in year.Entries ->
        try
          let name = entry.Summary.Value |> removePrefix "GB Fixed Observations for " |> removeAfter " ("
          let code = entry.Summary.Value |> removeBefore " (" |> removeAfter ") "
          let lat, log = 
            match entry.Polygon.Split(' ') |> List.ofArray with 
            | lat::log::_ -> (float lat, float log) 
            | _ -> failwith "Could not parse coordinates"
          Some { Station.ID = code; Longitude = log; Latitude = lat; Name = name }
        with _ -> None  ]
  let failures = stations |> Seq.filter Option.isNone |> Seq.length
  if failures > 5 then failwithf "Failed to parse stations: Too many errors for year %d" yearNumber
  else List.choose id stations

let downloadMesurementFiles years = 
  [ for year in years -> Cloud.retryOnTimeout (60*1000) 6 (fun resetTimeouts -> local {
      do! Cloud.Logf "(%d) starting...." year 
      if (Storage.getBlob "defra-airquality" (string year + ".downloaded")).Exists() then
        do! Cloud.Logf "(%d) skipping! '%d.downloaded' exists!" year year
      else
        do! Cloud.Logf "(%d) getting stations..." year 
        let stations = getStations year
        resetTimeouts ()
        for station in stations do
          let container = Storage.getContainerReference ()
          let blobName = sprintf "defra-airquality/%d/%s.xml" year (station.ID.ToLower())
          let blob = container.GetBlockBlobReference(blobName)
          if not (blob.Exists()) then 
            let dataUrl = sprintf "http://uk-air.defra.gov.uk/data/atom-dls/observations/auto/GB_FixedObservations_%d_%s.xml" year station.ID
            do! Cloud.Logf "(%d) downloading measurements for %s" year station.Name
            blob.UploadFromStream(Http.RequestStream(dataUrl,timeout=Config.requestTimeout).ResponseStream)
            do! Cloud.Logf "(%d) stored measurements for %s" year station.Name
          else 
            do! Cloud.Logf "(%d) skipping measurements for %s" year station.Name 
        Storage.writeFileToBlob "" "defra-airquality" (string year + ".completed")
        do! Cloud.Logf "(%d) downloaded all stations!" year }) ]
  |> Cloud.Parallel 
  |> Cloud.Ignore


// ------------------------------------------------------------------------------------------------
// Scripts to read downloaded XML files from storage and write them to SQL database
// ------------------------------------------------------------------------------------------------

let readMeasurements (pollutants:IDictionary<_, _>) year (station:Station) = 
  let blobName = sprintf "%d/%s.xml" year (station.ID.ToLower())
  let data = Data.Parse(Storage.downloadBlobAsText "defra-airquality" blobName)

  // Collect observations that have data & measure known property
  let observations = 
    data.FeatureMembers 
    |> Seq.choose (fun mem -> mem.OmObservation) 
    |> Seq.filter (fun obs -> obs.ObservedProperty.Href.IsSome)
        
  let rows = ResizeArray<RawMeasurement>()
  for obs in observations do
    try
      // Ensure the XML file has StartDate & Value in the usual places
      let fields = obs.Result.DataArray.ElementType.DataRecord.Fields
      if fields.[0].Name <> "StartTime" || fields.[4].Name <> "Value" then failwith "Unexpected fields!"
  
      // Get pollutant from pre-loaded lookup table
      let pollutant = pollutants.[obs.ObservedProperty.Href.Value]

      let vals = obs.Result.DataArray.Values
      let block = obs.Result.DataArray.Encoding.TextEncoding.BlockSeparator
      let tok = obs.Result.DataArray.Encoding.TextEncoding.TokenSeparator

      for block in vals.Split([| block |], StringSplitOptions.RemoveEmptyEntries) do
        let flds = block.Split([| tok |], StringSplitOptions.None) 
        { RawMeasurement.ID = Guid.NewGuid() 
          Time = DateTimeOffset.Parse(flds.[0])
          Pollutant = pollutant
          Station = station
          Value = float flds.[4] } |> rows.Add
    with e -> failwithf "Failed to process observation:\n%A\n\n%A" obs e
  rows.ToArray()    

let averageMeasurements f g (measurements:seq<RawMeasurement>) = 
  measurements 
  |> Seq.groupBy (fun m -> m.Pollutant.ID, f m.Time)
  |> Seq.map (fun ((_, k), group) ->   
      let first = Seq.head group
      g first.Station first.Pollutant k (group |> Seq.averageBy (fun m -> m.Value)))
  |> Array.ofSeq

let averageMonthly = 
  averageMeasurements (fun m -> m.Year, m.Month) (fun s p (y, m) v -> 
    { MonthlyMeasurement.ID = Guid.NewGuid(); Pollutant = p; Station = s; Year = y; Month = m; Value = v })

let averageDaily = 
  averageMeasurements (fun m -> m.Year, m.Month, m.Day) (fun s p (y, m, d) v -> 
    { DailyMeasurement.ID = Guid.NewGuid(); Pollutant = p; Station = s; Year = y; Month = m; Day = d; Value = v })
    
let storeMeasurements years = 
  [ for year in years -> Cloud.retryOnTimeout (60*1000) 0 (fun resetTimeouts -> local {
      let mutable ctx = Import.InsertContext.Create()
      let pollutants = getPollutants ()
      let inserted = Storage.getBlob "defra-airquality" (string year + ".inserted")
      if inserted.Exists() then
        do! Cloud.Logf "(%d) All data inserted already. Skipping." year
      else
        let dailyData = ResizeArray<_>()
        let monthlyData = ResizeArray<_>()
        for station in getStations year do
          let! raw = local {
            try return readMeasurements pollutants year station 
            with e -> 
              do! Cloud.Logf "(%d) Failed to read measurements for %s:\n  %A" year station.Name e
              return [||] }
          do! Cloud.Logf "(%d) Read %d records for %s" year raw.Length station.Name
          dailyData.AddRange(averageDaily raw)
          monthlyData.AddRange(averageMonthly raw)

        let ctx = Import.insertRecordsWithNested ctx "defra-airquality" dailyData 
        let ctx = Import.insertRecordsWithNested ctx "defra-airquality" monthlyData
        Storage.writeFileToBlob "" "defra-airquality" (string year + ".inserted")
        do! Cloud.Logf "(%d) Success. Inserted %d daily points and %d monthly points" year dailyData.Count monthlyData.Count
        resetTimeouts () }) ]
  |> Cloud.Parallel 
  |> Cloud.Ignore


// ------------------------------------------------------------------------------------------------
// Actually run things!
// ------------------------------------------------------------------------------------------------

// Download raw XML files and save them in Azre blobs for further processing
let cluster = Config.GetCluster()
cluster.ShowWorkers()
cluster.ShowProcesses()

let p1 = downloadMesurementFiles [1973 .. 2017] |> cluster.CreateProcess
p1.Status
p1.Result
p1.ShowInfo()

Cloud.printLogs p1

// Check progress and see which years have completed already...
for y in [1973 .. 2017] do
  let blob = Storage.getBlob "defra-airquality" (string y + ".downloaded") 
  printfn "%d: %A" y (blob.Exists())

// Read downloaded XML files from storage and write them to SQL database
Database.cleanupStorage "defra-airquality" [typeof<DailyMeasurement>; typeof<MonthlyMeasurement>]
Database.initializeStorage "defra-airquality" [typeof<DailyMeasurement>; typeof<MonthlyMeasurement>]

let p2 = storeMeasurements [2017 .. 2017] |> cluster.CreateProcess
p2.Status
p2.Result
p2.ShowInfo()

Cloud.printLogs p2

// Count how many records have we inserted already...
"SELECT Count(*) FROM [defra-airquality-daily-measurement]" |> Database.executeScalarCommand
"SELECT Count(*) FROM [defra-airquality-monthly-measurement]" |> Database.executeScalarCommand
"SELECT Count(*) FROM [defra-airquality-pollutant]" |> Database.executeScalarCommand
"SELECT Count(*) FROM [defra-airquality-station]" |> Database.executeScalarCommand

// Check progress and see which years have completed already...
for y in [1973 .. 2017] do
  let blob = Storage.getBlob "defra-airquality" (string y + ".inserted") 
  printfn "%d: %A" y (blob.Exists())

// Create some indices over the table to make filtering by time, station & pollutant faster
"CREATE NONCLUSTERED INDEX IX_YearMonthDay ON [defra-airquality-daily-measurement] (Year,Month,Day)" 
|> Database.executeCommandWithTimeout (60 * 15)
"CREATE NONCLUSTERED INDEX IX_StationID ON [defra-airquality-daily-measurement] (StationID)" 
|> Database.executeCommandWithTimeout (60 * 15)
"CREATE NONCLUSTERED INDEX IX_PollutantID ON [defra-airquality-daily-measurement] (PollutantID)" 
|> Database.executeCommandWithTimeout (60 * 15)

"CREATE NONCLUSTERED INDEX IX_YearMonth ON [defra-airquality-monthly-measurement] (Year,Month)" 
|> Database.executeCommandWithTimeout (60 * 15)
"CREATE NONCLUSTERED INDEX IX_StationID ON [defra-airquality-monthly-measurement] (StationID)" 
|> Database.executeCommandWithTimeout (60 * 15)
"CREATE NONCLUSTERED INDEX IX_PollutantID ON [defra-airquality-monthly-measurement] (PollutantID)" 
|> Database.executeCommandWithTimeout (60 * 15)

"DROP INDEX IX_measurement_??? ON [defra-airquality-daily-measurement]" 
|> Database.executeCommand

