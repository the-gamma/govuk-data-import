namespace global

open System

// ------------------------------------------------------------------------------------------------
// Extensions for F# core libraries
// ------------------------------------------------------------------------------------------------

module Seq = 
  let chunkWhile f input = seq {
    let chunk = ResizeArray<_>()
    for item in input do 
      if chunk.Count = 0 then chunk.Add(item)
      elif f chunk.[0] item then chunk.Add(item)
      else 
        yield chunk.ToArray()
        chunk.Clear() }
