
#r "Microsoft.AnalysisServices.dll"
#r "Microsoft.AnalysisServices.Core.dll"
#r "Microsoft.AnalysisServices.Tabular.dll"
#r "System.Data"

using System.Data;
using System.Data.SqlClient;
using System.Net;
using RefreshType = Microsoft.AnalysisServices.Tabular.RefreshType;
using Server = Microsoft.AnalysisServices.Tabular.Server;

public static async Task Run (TimerInfo myTimer, TraceWriter log)
{
    log.Info("C# HTTP trigger function processed a request.");
    // Configuration
    string connStringAS = System.Configuration.ConfigurationManager.ConnectionStrings["connStringAS"].ConnectionString;
    string databaseAS = System.Configuration.ConfigurationManager.ConnectionStrings["databaseAS"].ConnectionString;

    string connStringSql = System.Configuration.ConfigurationManager.ConnectionStrings["connStringSql"].ConnectionString;
    string schema = System.Configuration.ConfigurationManager.ConnectionStrings["schema"].ConnectionString;


    // Check if process is not running 
    string status = ExecuteStoredProcedure(connStringSql, schema, "[sp_get_process_status_flag]");
    if (status.ToLower() == "running")
    {
        return;
    }

    status = ExecuteStoredProcedure(connStringSql, schema, "[sp_get_process_flag]");
    if (status.ToLower() == "0")
    {
        return;
    }

    //Start Process
    ExecuteStoredProcedure(connStringSql, schema, "[sp_start_process]");
    ExecuteStoredProcedure(connStringSql, schema, "[sp_set_process_flag]", "@status_flag", "0");

    string responseSuccess = "Success";

    try
    {
        Server server = new Server();
        server.Connect(connStringAS);
        var db = server.Databases.Find(databaseAS);
        db.Model.RequestRefresh(RefreshType.Full);
        db.Model.SaveChanges();
        server.Refresh(true);
        
    }
    catch (Exception ex)
    {
        responseSuccess = ex.Message;

    }

    ExecuteStoredProcedure(connStringSql, schema, "[sp_finish_process]", "@status_flag", responseSuccess);
    
}

public static string ExecuteStoredProcedure(string connectionString, string schema, string spName)
{
    using (SqlConnection conn = new SqlConnection(connectionString))
    {
        using (SqlCommand command = new SqlCommand(schema + "." + spName, conn) {CommandType = CommandType.StoredProcedure})
        {
            conn.Open();
            var data = command.ExecuteReader(CommandBehavior.SingleRow);
            bool dataRead = data.Read();
            if (dataRead)
            {
                return data[0].ToString();
            }

        }
    };
    return string.Empty;
}

public static string ExecuteStoredProcedure(string connectionString, string schema, string spName, string param, string paramValue)
{
    using (SqlConnection conn = new SqlConnection(connectionString))
    {
        using (SqlCommand command = new SqlCommand(schema + "." + spName, conn) { CommandType = CommandType.StoredProcedure })
        {
            conn.Open();
            command.Parameters.Add(new SqlParameter(param, paramValue));
            var data = command.ExecuteReader(CommandBehavior.SingleRow);
            bool dataRead = data.Read();
            if (dataRead)
            {
                return data[0].ToString();
            }
            
        }
    };
    return string.Empty;
}
