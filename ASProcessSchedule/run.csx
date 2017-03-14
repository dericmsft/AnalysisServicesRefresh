
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
    string responseSuccess = "Success";
    string id = "";
    try
    {
        id = ExecuteStoredProcedure(connStringSql, schema, "[sp_start_job]");
        if (string.IsNullOrEmpty(id) || id == "0")
        {
            return; 
        }

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

    Dictionary<string, string> param = new Dictionary<string, string>();
    param.Add("@jobid", id);
    param.Add("@jobMessage", responseSuccess);
    ExecuteStoredProcedure(connStringSql, schema, "[sp_finish_job]", param);
}

public static string ExecuteStoredProcedure(string connectionString, string schema, string spName)
{
    using (SqlConnection conn = new SqlConnection(connectionString))
    {
        using (SqlCommand command = new SqlCommand(schema + "." + spName, conn) {CommandType = CommandType.StoredProcedure})
        {
            conn.Open();
            //var data = command.ExecuteReader(CommandBehavior.SingleResult);
            //bool dataRead = data.Read();
            //if (dataRead)
            //{
            //    return data[0].ToString();
            //}
            SqlParameter retval = command.Parameters.Add("@returnval", SqlDbType.VarChar);
            retval.Direction = ParameterDirection.ReturnValue;
            command.ExecuteNonQuery(); // MISSING
            return command.Parameters["@returnval"].Value.ToString();

        }
    };
    return string.Empty;
}

public static string ExecuteStoredProcedure(string connectionString, string schema, string spName, Dictionary<string,string> param )
{
    using (SqlConnection conn = new SqlConnection(connectionString))
    {
        using (SqlCommand command = new SqlCommand(schema + "." + spName, conn) { CommandType = CommandType.StoredProcedure })
        {
            conn.Open();
            foreach (var keyValuePair in param)
            {
                command.Parameters.Add(new SqlParameter(keyValuePair.Key, keyValuePair.Value));
            }

            SqlParameter retval = command.Parameters.Add("@returnval", SqlDbType.VarChar);
            retval.Direction = ParameterDirection.ReturnValue;
            command.ExecuteNonQuery(); // MISSING
            return command.Parameters["@returnval"].Value.ToString();

            //var data = command.ExecuteReader(CommandBehavior.SingleResult);
            //bool dataRead = data.Read();
            //if (dataRead)
            //{
            //   return data[0].ToString();
            //}

        }
    };
    return string.Empty;
}
