#r "Microsoft.AnalysisServices"
#r "Microsoft.AnalysisServices.Tabular"
#r "Microsoft.Deployment.Common.ActionModel"
#r "Microsoft.Deployment.Common.Actions"
#r "Microsoft.Deployment.Common.Helpers"


using System;
using System.ComponentModel.Composition;
using System.IO;
using System.Threading.Tasks;

using Microsoft.AnalysisServices;
using Microsoft.AnalysisServices.Tabular;
using Microsoft.Deployment.Common.ActionModel;
using Microsoft.Deployment.Common.Actions;
using Microsoft.Deployment.Common.Helpers;

public static async Task Run()
{
    log.Info("C# HTTP trigger function processed a request.");
    // Configuration
    string AzureTokenAS = System.Configuration.ConfigurationManager.ConnectionStrings["AzureTokenAS"].ConnectionString;
    string ASServerUrl = System.Configuration.ConfigurationManager.ConnectionStrings["ASServerUrl"].ConnectionString;
    string ASDatabase = System.Configuration.ConfigurationManager.ConnectionStrings["ASDatabase"].ConnectionString;
    string ModelFilePath = System.Configuration.ConfigurationManager.ConnectionStrings["modelFilePath"].ConnectionString;
            


    //var azureToken = request.DataStore.GetJson("AzureTokenAS");
    //string serverUrl = request.DataStore.GetValue("ASServerUrl");
    //string asDatabase = request.DataStore.GetValue("ASDatabase");
    //string modelFile = request.DataStore.GetValue("modelFilePath");


    //get data store location and connection string 
    //get get key for the blob storage 
    string connectionString = ValidateConnectionToAS.GetASConnectionString(request, AzureTokenAS, ASServerUrl);


    string jsonContents = File.ReadAllText(request.Info.App.AppFilePath + "/" + modelFile);

    Server server = null;
    try
    {
        server = new Server();
        server.Connect(connectionString);

        // Delete existing
        Database db = server.Databases.FindByName(ASDatabase);
        db?.Drop();

        var dbModel = JsonSerializer.DeserializeDatabase(jsonContents);

        server.Databases.Add(dbModel);

        dbModel.Model.RequestRefresh(AnalysisServices.Tabular.RefreshType.Full);
        dbModel.Update(UpdateOptions.ExpandFull);

        server.Disconnect(true);

        return new ActionResponse(ActionStatus.Success);
    }
    catch (Exception e)
    {
        return new ActionResponse(ActionStatus.Failure, string.Empty, e, null);
    }
    finally
    {
        server?.Dispose();
    }
}