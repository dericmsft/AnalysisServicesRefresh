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

public static async Task<HttpResponseMessage> Run(HttpRequestMessage req, TraceWriter log)
{
{
    log.Info("C# HTTP trigger function processed a request.");
    // Configuration
    string appId = System.Configuration.ConfigurationManager.ConnectionStrings["appId"].ConnectionString;
    string appKey = System.Configuration.ConfigurationManager.ConnectionStrings["appKey"].ConnectionString;
    string tenantId = System.Configuration.ConfigurationManager.ConnectionStrings["tenantId"].ConnectionString;
    string asServer = System.Configuration.ConfigurationManager.ConnectionStrings["asServer"].ConnectionString;
    string ASDatabase = System.Configuration.ConfigurationManager.ConnectionStrings["databaseAS"].ConnectionString;
    string ModelFilePath = System.Configuration.ConfigurationManager.ConnectionStrings["modelFilePath"].ConnectionString;
            


    //var azureToken = request.DataStore.GetJson("AzureTokenAS");
    //string serverUrl = request.DataStore.GetValue("ASServerUrl");
    //string asDatabase = request.DataStore.GetValue("ASDatabase");
    //string modelFile = request.DataStore.GetValue("modelFilePath");

    Uri asServerUrl = new Uri(asServer);
    string resource = "https://" + asServerUrl.Host;
    AuthenticationContext context = new AuthenticationContext("https://login.windows.net/" + tenantId);
    ClientCredential credential = new ClientCredential(appId, appKey);
    var token = await context.AcquireTokenAsync(resource, credential);


    string password = token.AccessToken;
    string connectionString = $"Provider=MSOLAP;Data Source={asServer};Password={password};";


    string jsonContents = req.GetQueryNameValuePairs()
            .FirstOrDefault(q => string.Compare(q.Key, "jsonModel", true) == 0)
            .Value;

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

        return req.CreateResponse(HttpStatusCode.OK, "Done");
    }
    catch (Exception e)
    {
        HttpResponseMessage res = req.CreateErrorResponse(HttpStatusCode.InternalServerError, ex);
        return res;
    }
    finally
    {
        server?.Dispose();
    }
}