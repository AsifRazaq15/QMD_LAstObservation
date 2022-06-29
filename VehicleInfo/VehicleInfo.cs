using System;
using System.Collections.Generic;
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
[assembly: AssemblyTitle("AVLS")]
[assembly: AssemblyDescription("")]
[assembly: AssemblyConfiguration("")]
[assembly: AssemblyCompany("")]
[assembly: AssemblyProduct("AVLS")]
[assembly: AssemblyCopyright("Copyright ©  2022")]
[assembly: AssemblyTrademark("")]
[assembly: XmlConfigurator]
[assembly: ComVisible(false)]
[assembly: Guid("9d101793-541a-423a-a519-89e901945e27")]
[assembly: AssemblyFileVersion("1.0.0.0")]
[assembly: TargetFramework(".NETFramework,Version=v4.6.1", FrameworkDisplayName = ".NET Framework 4.6.1")]
[assembly: AssemblyVersion("1.0.0.0")]
namespace AVLS.VehicleInfo;

internal class Program
{
	private static DateTime RequestTime = default(DateTime);

	private static DateTime ResponseTime = default(DateTime);

	private static string batchId = Guid.NewGuid().ToString();

	private static void Main(string[] args)
	{
		BasicConfigurator.Configure();
		ILog logger = LogManager.GetLogger(typeof(Program));
		SqlConnection sqlConnection = null;
		try
		{
			logger.Info("Batch ID: " + batchId);
			string text = CallRestMethod();
			JToken jToken = JObject.Parse(text)["data"]!["vehicles"];
			logger.Info("RequestTime: " + RequestTime);
			logger.Info("ResponseTime: " + ResponseTime);
			logger.Info("Response Message: " + jToken.ToString().Substring(0, 100) + "...");
			using (sqlConnection = new SqlConnection(ConfigurationManager.ConnectionStrings["ODW_STG_ConnectionString"].ConnectionString))
			{
				sqlConnection.Open();
				logger.Info("Connection with SQL Server made successfully.");
				using (SqlCommand sqlCommand = new SqlCommand())
				{
					sqlCommand.Connection = sqlConnection;
					sqlCommand.CommandType = CommandType.StoredProcedure;
					sqlCommand.CommandText = "AVLS.Insert_STG_VehicleInfo";
					sqlCommand.Parameters.Add("@RequestTime", SqlDbType.DateTime).Value = RequestTime;
					sqlCommand.Parameters.Add("@ResponsePayload", SqlDbType.NVarChar, -1).Value = text;
					sqlCommand.Parameters.Add("@ResponseTime", SqlDbType.DateTime).Value = ResponseTime;
					sqlCommand.Parameters.Add("@ServiceOperation", SqlDbType.VarChar, 50).Value = "AVLS.VehicleInfo";
					sqlCommand.Parameters.Add("@AVLSVehicleInfoType", SqlDbType.Structured).Value = GetVehicleInfoDetail(jToken);
					sqlCommand.ExecuteNonQuery();
					sqlCommand.Dispose();
				}
				sqlConnection.Close();
				sqlConnection.Dispose();
			}
			logger.Info("Succesfully inserted the vehicle details in VehicleInfo staging table.");
			logger.Info(string.Concat("***********************************", DateTime.Now, "***********************************"));
		}
		catch (Exception ex)
		{
			logger.Error("Error Message: " + ex.Message + "\n Error Inner Exception: " + ex.InnerException);
			logger.Info(string.Concat("***********************************", DateTime.Now, "***********************************"));
		}
	}

	private static DataTable GetVehicleInfoDetail(JToken obj)
	{
		DataTable dataTable = new DataTable();
		dataTable.Columns.Add("Avls_Id");
		dataTable.Columns.Add("Id");
		dataTable.Columns.Add("Title");
		dataTable.Columns.Add("Plate_Number");
		dataTable.Columns.Add("Vehicle_Details");
		dataTable.Columns.Add("Brand");
		dataTable.Columns.Add("Model");
		dataTable.Columns.Add("Latitude");
		dataTable.Columns.Add("Longitude");
		dataTable.Columns.Add("Digital_Input_1");
		dataTable.Columns.Add("Speed");
		dataTable.Columns.Add("Angle");
		dataTable.Columns.Add("Last_Update_Timestamp");
		dataTable.Columns.Add("LastUpdate");
		dataTable.Columns.Add("IMEI_Number");
		dataTable.Columns.Add("Odometer");
		dataTable.Columns.Add("Initial_Odometer");
		dataTable.Columns.Add("Duration_Since_Last_Record");
		dataTable.Columns.Add("Php_Timezone");
		dataTable.Columns.Add("Avls_Status");
		dataTable.Columns.Add("Movement_Status");
		dataTable.Columns.Add("Movement_Status_Code");
		dataTable.Columns.Add("Offset");
		dataTable.Columns.Add("PlateNumber");
		dataTable.Columns.Add("Lat");
		dataTable.Columns.Add("Lng");
		dataTable.Columns.Add("Last_Location");
		dataTable.Columns.Add("Last_Record_GMT");
		dataTable.Columns.Add("Driver");
		dataTable.Columns.Add("DriverMobile");
		dataTable.Columns.Add("Department");
		dataTable.Columns.Add("Zone");
		dataTable.Columns.Add("Section");
		dataTable.Columns.Add("Contractor");
		dataTable.Columns.Add("VehicleType");
		dataTable.Columns.Add("Capacity");
		dataTable.Columns.Add("Date_Created");
		dataTable.Columns.Add("Batch_Id");
		int num = 0;
		foreach (JToken item in (IEnumerable<JToken>)obj)
		{
			dataTable.Rows.Add(num, item.SelectToken("id"), item.SelectToken("title")!.ToString(), item.SelectToken("plate_number")!.ToString(), item.SelectToken("vehicleDetails")!.ToString(), item.SelectToken("brand")!.ToString(), item.SelectToken("model")!.ToString(), CheckDataTypefloat(item.SelectToken("latitude")), CheckDataTypefloat(item.SelectToken("longitude")), CheckDataTypeInt(item.SelectToken("digital_input_1")), CheckDataTypeInt(item.SelectToken("speed")), CheckDataTypeInt(item.SelectToken("angle")), UnixTimeStampToDateTime(Convert.ToDouble(item.SelectToken("lastUpdateTimestamp"))), UnixTimeStampToDateTime(Convert.ToDouble(item.SelectToken("lastUpdate"))), item.SelectToken("imei_number")!.ToString(), CheckDataTypeInt(item.SelectToken("odometer")), CheckDataTypeInt(item.SelectToken("initialOdometer")), CheckDataTypeInt(item.SelectToken("durationSinceLastRecord")), item.SelectToken("php_timezone")!.ToString(), CheckDataTypeInt(item.SelectToken("status")), item.SelectToken("movementStatus")!.ToString(), CheckDataTypeInt(item.SelectToken("movementStatusCode")), CheckDataTypeInt(item.SelectToken("offset")), item.SelectToken("plateNumber")!.ToString(), CheckDataTypefloat(item.SelectToken("lat")), CheckDataTypefloat(item.SelectToken("lng")), item.SelectToken("lastLocation")!.ToString(), UnixTimeStampToDateTime(Convert.ToDouble(item.SelectToken("lastRecordGMT"))), item.SelectToken("driver")!.ToString(), item.SelectToken("driverMobile")!.ToString(), item.SelectToken("department")!.ToString(), item.SelectToken("zone")!.ToString(), item.SelectToken("section")!.ToString(), item.SelectToken("contractor")!.ToString(), item.SelectToken("vehicleType")!.ToString(), item.SelectToken("capacity")!.ToString(), DateTime.Now, batchId);
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

	public static int? CheckDataTypeInt(JToken token)
	{
		int? result = null;
		if (!string.IsNullOrEmpty(token.ToString()))
		{
			result = Convert.ToInt32(token);
		}
		return result;
	}

	public static DateTime UnixTimeStampToDateTime(double unixTimeStamp)
	{
		return new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc).AddSeconds(unixTimeStamp).ToLocalTime();
	}

	public static string CallRestMethod()
	{
		HttpWebRequest obj = (HttpWebRequest)WebRequest.Create(ConfigurationManager.AppSettings["Vehicle_Info_Url"]);
		obj.Method = "GET";
		obj.ContentType = "application/json";
		RequestTime = DateTime.Now;
		HttpWebResponse httpWebResponse = (HttpWebResponse)obj.GetResponse();
		Encoding encoding = Encoding.GetEncoding("utf-8");
		StreamReader streamReader = new StreamReader(httpWebResponse.GetResponseStream(), encoding);
		_ = string.Empty;
		string result = streamReader.ReadToEnd();
		httpWebResponse.Close();
		ResponseTime = DateTime.Now;
		return result;
	}
}

