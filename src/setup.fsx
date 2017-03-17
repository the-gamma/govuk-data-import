#load "mbrace.fsx"

open MBrace.Azure
open MBrace.Azure.Management
open MBrace.Core

// Deploy MBrace to Azure and check progress
let deployment = Config.ProvisionCluster()
deployment.ShowInfo()

// Get cluster and check that it works
let cluster = Config.GetCluster()
cluster.ShowWorkers()

// Add 1 + 1 in the cloud (whoa!)
let add = cloud { return 1 + 1 } |> cluster.CreateProcess
add.ShowInfo()
add.Result

// Configure cluster and delete it
Config.ResizeCluster 20
Config.DeleteCluster()
