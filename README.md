Data import scripts for data.gov.uk
===================================

This repository contains various scripts for importing data from various UK Open Government
Data sources. This generally copies raw data into Azure blob storage and then goes over it
again and inserts data into SQL database. To run the code, you need to configure a couple
of keys first (see the `src/config/NOTE.md` file).

You will find the following in the `src` folder:

 - `config` is a folder where you need to specify Azure connection strings etc.
 - `lib` contains various helpers for inserting data and generating tables
 - `setup.fsx` starts MBrace cluster, so that we can run things nicely
 - `defra.fsx` imports Air Quality data from [Defra](https://uk-air.defra.gov.uk/data/atom-dls/)
 - `houses.fsx` imports [HM Land Registry Price Paid Data](https://data.gov.uk/dataset/land-registry-monthly-price-paid-data)
