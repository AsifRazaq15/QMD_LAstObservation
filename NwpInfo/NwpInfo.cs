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
using QMD.NwpInfo.Model;

[assembly: CompilationRelaxations(8)]
[assembly: RuntimeCompatibility(WrapNonExceptionThrows = true)]
[assembly: Debuggable(DebuggableAttribute.DebuggingModes.IgnoreSymbolStoreSequencePoints)]
[assembly: AssemblyTitle("QMD.NwpInfo")]
[assembly: AssemblyDescription("")]
[assembly: AssemblyConfiguration("")]
[assembly: AssemblyCompany("")]
[assembly: AssemblyProduct("QMD.NwpInfo")]
[assembly: AssemblyCopyright("Copyright ©  2022")]
[assembly: AssemblyTrademark("")]
[assembly: XmlConfigurator]
[assembly: ComVisible(false)]
[assembly: Guid("612c19ab-3573-4cd5-9e4d-815b6b2aa850")]
[assembly: AssemblyFileVersion("1.0.0.0")]
[assembly: TargetFramework(".NETFramework,Version=v4.6.1", FrameworkDisplayName = ".NET Framework 4.6.1")]
[assembly: AssemblyVersion("1.0.0.0")]
namespace QMD.NwpInfo
{
	internal class Program
	{
		private static void Main(string[] args)
		{
			new QMDNwpInfo().MainMethod();
		}
	}
	public class QMDNwpInfo
	{
		private DateTime _requestTime;

		private DateTime _responseTime;

		private List<AvlsAudit> audits = new List<AvlsAudit>();

		private ILog _log;

		public QMDNwpInfo()
		{
			BasicConfigurator.Configure();
			_log = LogManager.GetLogger(typeof(QMDNwpInfo));
		}

		public void MainMethod()
		{
			try
			{
				List<QmdNwpInfoDetail> nwpInfoService = GetNwpInfoService();
				using (SqlConnection sqlConnection = new SqlConnection(ConfigurationManager.ConnectionStrings["ODW_STG_ConnectionString"].ConnectionString))
				{
					sqlConnection.Open();
					_log.Info("Connection with SQL Server made successfully.");
					using (SqlCommand sqlCommand = new SqlCommand())
					{
						sqlCommand.Connection = sqlConnection;
						sqlCommand.CommandType = CommandType.StoredProcedure;
						sqlCommand.CommandText = "QMD.Insert_STG_NwpInfo";
						sqlCommand.Parameters.Add("@AuditServiceType", SqlDbType.Structured).Value = GetAuditDetail(audits);
						sqlCommand.Parameters.Add("@NwpInfoType", SqlDbType.Structured).Value = GetNwpInfoDetail(nwpInfoService);
						sqlCommand.ExecuteNonQuery();
						sqlCommand.Dispose();
					}
					sqlConnection.Close();
					sqlConnection.Dispose();
				}
				_log.Info("Succesfully inserted the weather details in WeatherAlert and CityWeatherInfo staging table.");
				_log.Info(string.Concat("***********************************", DateTime.Now, "***********************************"));
			}
			catch (Exception ex)
			{
				_log.Error("Error Message: " + ex.Message + "\n Error Inner Exception: " + ex.InnerException);
				_log.Info(string.Concat("***********************************", DateTime.Now, "***********************************"));
			}
		}

		private List<NwpInfoStations> GetStations()
		{
			_log.Info("[GetStations] step 2 => Get all stations data from ODW database");
			List<NwpInfoStations> list = new List<NwpInfoStations>();
			using SqlConnection sqlConnection = new SqlConnection(ConfigurationManager.ConnectionStrings["ODW_ConnectionString"].ConnectionString);
			sqlConnection.Open();
			using (SqlDataAdapter sqlDataAdapter = new SqlDataAdapter("SELECT DISTINCT Station_Name_En,Lat,Lon FROM QMD.NwpInfo", sqlConnection))
			{
				DataSet dataSet = new DataSet();
				sqlDataAdapter.Fill(dataSet);
				foreach (DataRow row in dataSet.Tables[0].Rows)
				{
					list.Add(new NwpInfoStations
					{
						Station_Name = Convert.ToString(row["Station_Name_En"]),
						Latitude = Convert.ToSingle(row["Lat"]),
						Longitude = Convert.ToSingle(row["Lon"])
					});
				}
			}
			sqlConnection.Close();
			sqlConnection.Dispose();
			return list;
		}

		private List<QmdNwpInfoDetail> GetNwpInfoService()
		{
			List<QmdNwpInfoDetail> list = new List<QmdNwpInfoDetail>();
			try
			{
				_log.Info("[GetNwpInfoService] step 1 => Call GetStation method ");
				List<NwpInfoStations> stations = GetStations();
				_log.Info("[GetNwpInfoService] step 3 => Total number of station's " + stations.Count);
				foreach (NwpInfoStations item2 in stations)
				{
					string nwpInfo = GetNwpInfo(item2.Latitude, item2.Longitude);
					JArray jArray = JArray.Parse(nwpInfo);
					_log.Info("RequestTime: " + _requestTime);
					_log.Info("ResponseTime: " + _responseTime);
					_log.Info("Response Message: " + nwpInfo.Substring(0, 100) + "...");
					audits.Add(new AvlsAudit
					{
						RequestTime = _requestTime,
						ResponsePayload = nwpInfo,
						ResponseTime = _responseTime,
						ServiceOperation = "QMD.NwpInfo"
					});
					foreach (JToken item3 in jArray)
					{
						QmdNwpInfoDetail item = new QmdNwpInfoDetail
						{
							Lat = CheckDataTypefloat(item3.SelectToken("Lat")),
							Lon = CheckDataTypefloat(item3.SelectToken("Lon")),
							UtcTime = Convert.ToDateTime(item3.SelectToken("UtcTime")),
							AirTemp = CheckDataTypefloat(item3.SelectToken("AirTemp")),
							RH = CheckDataTypefloat(item3.SelectToken("RH")),
							WindSpeed = CheckDataTypefloat(item3.SelectToken("WindSpeed")),
							WindDir = CheckDataTypefloat(item3.SelectToken("WindDir")),
							WindCardinal = Convert.ToString(item3.SelectToken("WindCardinal")),
							WindGust = CheckDataTypefloat(item3.SelectToken("WindGust")),
							Pressure = CheckDataTypefloat(item3.SelectToken("Pressure")),
							Rainfall = CheckDataTypefloat(item3.SelectToken("Rainfall")),
							CloudCoverage = CheckDataTypefloat(item3.SelectToken("CloudCoverage")),
							Weather = Convert.ToString(item3.SelectToken("Weather")),
							Station = item2.Station_Name
						};
						list.Add(item);
					}
				}
				_log.Info("[GetNwpInfoService] step 4 => Total number of record's get from service is " + list.Count);
				return list;
			}
			catch (Exception ex)
			{
				_log.Error("[GetNwpInfoService]: Error Message: " + ex.Message + "\n Error Inner Exception: " + ex.InnerException);
				return list;
			}
		}

		private static DataTable GetNwpInfoDetail(List<QmdNwpInfoDetail> detail)
		{
			DataTable dataTable = new DataTable();
			dataTable.Columns.Add("Id");
			dataTable.Columns.Add("Latitude");
			dataTable.Columns.Add("Longitude");
			dataTable.Columns.Add("Utc_Time");
			dataTable.Columns.Add("Air_Temp");
			dataTable.Columns.Add("RH");
			dataTable.Columns.Add("Wind_Speed");
			dataTable.Columns.Add("Wind_Dir");
			dataTable.Columns.Add("Wind_Cardinal");
			dataTable.Columns.Add("Wind_Gust");
			dataTable.Columns.Add("Pressure");
			dataTable.Columns.Add("Rainfall");
			dataTable.Columns.Add("Cloud_Coverage");
			dataTable.Columns.Add("Weather");
			dataTable.Columns.Add("Station_Name_En");
			int num = 0;
			foreach (QmdNwpInfoDetail item in detail)
			{
				dataTable.Rows.Add(num, item.Lat, item.Lon, item.UtcTime, item.AirTemp, item.RH, item.WindSpeed, item.WindDir, item.WindCardinal, item.WindGust, item.Pressure, item.Rainfall, item.CloudCoverage, item.Weather, item.Station);
				num++;
			}
			return dataTable;
		}

		private static DataTable GetAuditDetail(List<AvlsAudit> detail)
		{
			DataTable dataTable = new DataTable();
			dataTable.Columns.Add("Id");
			dataTable.Columns.Add("RequestTime");
			dataTable.Columns.Add("ResponsePayload");
			dataTable.Columns.Add("ResponseTime");
			dataTable.Columns.Add("ServiceOperation");
			int num = 0;
			foreach (AvlsAudit item in detail)
			{
				dataTable.Rows.Add(num, item.RequestTime, item.ResponsePayload, item.ResponseTime, item.ServiceOperation);
				num++;
			}
			return dataTable;
		}

		public float? CheckDataTypefloat(JToken token)
		{
			float? result = null;
			if (!string.IsNullOrEmpty(token.ToString()))
			{
				result = Convert.ToSingle(token);
			}
			return result;
		}

		public int? CheckDataTypeInt(JToken token)
		{
			int? result = null;
			if (!string.IsNullOrEmpty(token.ToString()))
			{
				result = Convert.ToInt32(token);
			}
			return result;
		}

		public DateTime UnixTimeStampToDateTime(double unixTimeStamp)
		{
			return new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc).AddSeconds(unixTimeStamp).ToLocalTime();
		}

		public string GetNwpInfo(float latitude, float longitude)
		{
			string requestUriString = ConfigurationManager.AppSettings["Get_NwpInfo_Url"] + latitude + "/" + longitude;
			string uriString = ConfigurationManager.AppSettings["ProxyHost"];
			HttpWebRequest obj = (HttpWebRequest)WebRequest.Create(requestUriString);
			_ = obj.Proxy;
			obj.Method = "GET";
			obj.ContentType = "application/json";
			WebProxy webProxy = new WebProxy();
			Uri uri2 = (webProxy.Address = new Uri(uriString));
			obj.Proxy = webProxy;
			_requestTime = DateTime.Now;
			HttpWebResponse httpWebResponse = (HttpWebResponse)obj.GetResponse();
			Encoding encoding = Encoding.GetEncoding("utf-8");
			StreamReader streamReader = new StreamReader(httpWebResponse.GetResponseStream(), encoding);
			_ = string.Empty;
			string result = streamReader.ReadToEnd();
			httpWebResponse.Close();
			_responseTime = DateTime.Now;
			return result;
		}
	}
}
namespace QMD.NwpInfo.Model
{
	public class AvlsAudit
	{
		public DateTime RequestTime { get; set; }

		public DateTime ResponseTime { get; set; }

		public string ResponsePayload { get; set; }

		public string ServiceOperation { get; set; }
	}
	public class NwpInfoStations
	{
		public string Station_Name { get; set; }

		public float Latitude { get; set; }

		public float Longitude { get; set; }
	}
	public class QmdNwpInfoDetail
	{
		public float? Lat { get; set; }

		public float? Lon { get; set; }

		public DateTime UtcTime { get; set; }

		public float? AirTemp { get; set; }

		public float? WindSpeed { get; set; }

		public float? WindDir { get; set; }

		public float? RH { get; set; }

		public string WindCardinal { get; set; }

		public float? WindGust { get; set; }

		public float? Pressure { get; set; }

		public float? Rainfall { get; set; }

		public float? CloudCoverage { get; set; }

		public string Weather { get; set; }

		public string Station { get; set; }
	}
}

