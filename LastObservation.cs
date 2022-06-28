using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Text;
using log4net;
using log4net.Config;
using Newtonsoft.Json.Linq;

[assembly: CompilationRelaxations(8)]
[assembly: RuntimeCompatibility(WrapNonExceptionThrows = true)]
[assembly: Debuggable(DebuggableAttribute.DebuggingModes.IgnoreSymbolStoreSequencePoints)]
[assembly: AssemblyTitle("QMD.LastObservation")]
[assembly: AssemblyDescription("")]
[assembly: AssemblyConfiguration("")]
[assembly: AssemblyCompany("")]
[assembly: AssemblyProduct("QMD.LastObservation")]
[assembly: AssemblyCopyright("Copyright ©  2022")]
[assembly: AssemblyTrademark("")]
[assembly: XmlConfigurator]
[assembly: ComVisible(false)]
[assembly: Guid("9237bc88-54fb-42d0-9f30-36eaade5b4fb")]
[assembly: AssemblyFileVersion("1.0.0.0")]
[assembly: TargetFramework(".NETFramework,Version=v4.6.1", FrameworkDisplayName = ".NET Framework 4.6.1")]
[assembly: AssemblyVersion("1.0.0.0")]
namespace QMD.LastObservation;

internal class Program
{
	private static DateTime _requestTime;

	private static DateTime _responseTime;

	private static void Main(string[] args)
	{
		BasicConfigurator.Configure();
		ILog logger = LogManager.GetLogger(typeof(Program));
		try
		{
			string lastObservationData = GetLastObservationData();
			logger.Info("Web Service Result: " + lastObservationData);
			JArray array = JArray.Parse(lastObservationData);
			string text = Guid.NewGuid().ToString();
			logger.Info("Batch ID: " + text);
			logger.Info("RequestTime: " + _requestTime);
			logger.Info("ResponseTime: " + _responseTime);
			logger.Info("Response Message: " + lastObservationData.Substring(0, 100) + "...");
			using (SqlConnection sqlConnection = new SqlConnection(ConfigurationManager.ConnectionStrings["ODW_STG_ConnectionString"].ConnectionString))
			{
				sqlConnection.Open();
				logger.Info("Connection with SQL Server made successfully.");
				using (SqlCommand sqlCommand = new SqlCommand())
				{
					sqlCommand.Connection = sqlConnection;
					sqlCommand.CommandType = CommandType.StoredProcedure;
					sqlCommand.CommandText = "QMD.Insert_STG_LastObservation";
					sqlCommand.Parameters.Add("@RequestTime", SqlDbType.DateTime).Value = _requestTime;
					sqlCommand.Parameters.Add("@ResponsePayload", SqlDbType.NVarChar, -1).Value = lastObservationData;
					sqlCommand.Parameters.Add("@ResponseTime", SqlDbType.DateTime).Value = _responseTime;
					sqlCommand.Parameters.Add("@ServiceOperation", SqlDbType.VarChar, 50).Value = "QMD.LastObservation";
					sqlCommand.Parameters.Add("@Batch_Id", SqlDbType.NVarChar, 256).Value = text;
					sqlCommand.Parameters.Add("@QMDLastObservationType", SqlDbType.Structured).Value = GetLastObservationDetail(array);
					sqlCommand.ExecuteNonQuery();
					sqlCommand.Dispose();
				}
				sqlConnection.Close();
				sqlConnection.Dispose();
			}
			logger.Info("Succesfully inserted the Observation details in LastObservation staging table.");
			logger.Info(string.Concat("***********************************", DateTime.Now, "***********************************"));
		}
		catch (Exception ex)
		{
			logger.Error("Error Message: " + ex.Message + "\n Error Inner Exception: " + ex.InnerException);
			logger.Info(string.Concat("***********************************", DateTime.Now, "***********************************"));
		}
	}

	private static DataTable GetLastObservationDetail(JArray array)
	{
		Guid.NewGuid().ToString();
		DataTable dataTable = new DataTable();
		dataTable.Columns.Add("Id");
		dataTable.Columns.Add("Latitude");
		dataTable.Columns.Add("Longitude");
		dataTable.Columns.Add("Name_En");
		dataTable.Columns.Add("Name_Ar");
		dataTable.Columns.Add("Local_Time");
		dataTable.Columns.Add("Utc_Time");
		dataTable.Columns.Add("Air_Temp");
		dataTable.Columns.Add("RH");
		dataTable.Columns.Add("Wind_Speed");
		dataTable.Columns.Add("Wind_Dir");
		dataTable.Columns.Add("Wind_Gust");
		dataTable.Columns.Add("Pressure");
		dataTable.Columns.Add("Rainfall");
		dataTable.Columns.Add("Visibility");
		int num = 0;
		foreach (JToken item in array)
		{
			dataTable.Rows.Add(num, CheckDataTypefloat(item.SelectToken("Lat")), CheckDataTypefloat(item.SelectToken("Lon")), item.SelectToken("NameEn"), item.SelectToken("NameAr"), Convert.ToDateTime(item.SelectToken("LocalTime")), Convert.ToDateTime(item.SelectToken("UtcTime")), CheckDataTypefloat(item.SelectToken("AirTemp")), CheckDataTypefloat(item.SelectToken("RH")), CheckDataTypefloat(item.SelectToken("WindSpeed")), CheckDataTypefloat(item.SelectToken("WindDir")), CheckDataTypefloat(item.SelectToken("WindGust")), CheckDataTypefloat(item.SelectToken("Pressure")), CheckDataTypefloat(item.SelectToken("Rainfall")), CheckDataTypefloat(item.SelectToken("Visibility")));
			num++;
		}
		return dataTable;
	}

	public static float? CheckDataTypefloat(JToken token)
	{
		float? result = null;
		if (!string.IsNullOrEmpty(token.ToString()))
		{
			result = Convert.ToSingle(token);
		}
		return result;
	}

	public static string GetLastObservationData()
	{
		string requestUriString = ConfigurationManager.AppSettings["Get_LastObservation_Url"];
		string uriString = ConfigurationManager.AppSettings["ProxyHost"];
		HttpWebRequest obj = (HttpWebRequest)WebRequest.Create(requestUriString);
		_ = obj.Proxy;
		obj.Method = "GET";
		obj.ContentType = "application/json";
		WebProxy webProxy = new WebProxy();
		Uri uri2 = (webProxy.Address = new Uri(uriString));
		obj.Proxy = webProxy;
		_requestTime = DateTime.Now;
		HttpWebResponse obj2 = (HttpWebResponse)obj.GetResponse();
		string result = new StreamReader(encoding: Encoding.GetEncoding("utf-8"), stream: obj2.GetResponseStream()).ReadToEnd();
		obj2.Close();
		_responseTime = DateTime.Now;
		return result;
	}
}

