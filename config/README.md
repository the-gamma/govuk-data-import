1. Go to https://manage.windowsazure.com/publishsettings to download publish settings and
   save it in this folder as a file `azure.publishsettings`.

2. Create `config.fs` file with the following:

   ```
   module Config

   let TheGammaDataStorage = "DefaultEndpointsProtocol=https;AccountName=thegammadata;AccountKey=..."
   let TheGammaSqlConnection = "Server=tcp:thegamma-sql-data.database.windows.net,1433;" +
     "Initial Catalog=thegamma-sql-data;Persist Security Info=False;User ID=...;Password=...;" + "MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"
   ```
