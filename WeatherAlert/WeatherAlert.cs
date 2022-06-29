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
using QMD.WeatherAlert.Model;

[assembly: CompilationRelaxations(8)]
[assembly: RuntimeCompatibility(WrapNonExceptionThrows = true)]
[assembly: Debuggable(DebuggableAttribute.DebuggingModes.IgnoreSymbolStoreSequencePoints)]
[assembly: AssemblyTitle("QMD.WeatherAlert")]
[assembly: AssemblyDescription("")]
[assembly: AssemblyConfiguration("")]
[assembly: AssemblyCompany("")]
[assembly: AssemblyProduct("QMD.WeatherAlert")]
[assembly: AssemblyCopyright("Copyright ©  2022")]
[assembly: AssemblyTrademark("")]
[assembly: XmlConfigurator]
[assembly: ComVisible(false)]
[assembly: Guid("954e2cf4-8326-4e0a-a7f9-8d871ee4fc95")]
[assembly: AssemblyFileVersion("1.0.0.0")]
[assembly: TargetFramework(".NETFramework,Version=v4.6.1", FrameworkDisplayName = ".NET Framework 4.6.1")]
[assembly: AssemblyVersion("1.0.0.0")]
namespace QMD.WeatherAlert
{
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
				string weatherData = GetWeatherData();
				JObject weatherParse = JObject.Parse(weatherData);
				List<WeatherAlerts> list = new List<WeatherAlerts>();
				List<CityWeatherInfo> list2 = new List<CityWeatherInfo>();
				string text = Guid.NewGuid().ToString();
				logger.Info("Batch ID: " + text);
				logger.Info("RequestTime: " + _requestTime);
				logger.Info("ResponseTime: " + _responseTime);
				logger.Info("Response Message: " + weatherData.Substring(0, 100) + "...");
				list.AddRange(GeneratingWeatherAlertData(weatherParse, "Qatar_Inshore", text));
				list.AddRange(GeneratingWeatherAlertData(weatherParse, "Qatar_Offshore", text));
				list2.AddRange(GeneratingCityWeatherData(weatherParse, text));
				using (SqlConnection sqlConnection = new SqlConnection(ConfigurationManager.ConnectionStrings["ODW_STG_ConnectionString"].ConnectionString))
				{
					sqlConnection.Open();
					logger.Info("Connection with SQL Server made successfully.");
					using (SqlCommand sqlCommand = new SqlCommand())
					{
						sqlCommand.Connection = sqlConnection;
						sqlCommand.CommandType = CommandType.StoredProcedure;
						sqlCommand.CommandText = "QMD.Insert_STG_WeatherInfo_Alert";
						sqlCommand.Parameters.Add("@RequestTime", SqlDbType.DateTime).Value = _requestTime;
						sqlCommand.Parameters.Add("@ResponsePayload", SqlDbType.NVarChar, -1).Value = weatherData;
						sqlCommand.Parameters.Add("@ResponseTime", SqlDbType.DateTime).Value = _responseTime;
						sqlCommand.Parameters.Add("@ServiceOperation", SqlDbType.VarChar, 50).Value = "QMD.WeatherAlert";
						sqlCommand.Parameters.Add("@BatchId", SqlDbType.NVarChar, 256).Value = text;
						sqlCommand.Parameters.Add("@QMDWeatherAlertType", SqlDbType.Structured).Value = GetWeatherAlertDetail(list);
						sqlCommand.Parameters.Add("@QMDCityWeatherInfoType", SqlDbType.Structured).Value = GetCityWeatherInfoDetail(list2);
						sqlCommand.ExecuteNonQuery();
						sqlCommand.Dispose();
					}
					sqlConnection.Close();
					sqlConnection.Dispose();
				}
				logger.Info("Succesfully inserted the weather details in WeatherAlert and CityWeatherInfo staging table.");
				logger.Info(string.Concat("***********************************", DateTime.Now, "***********************************"));
			}
			catch (Exception ex)
			{
				logger.Error("Error Message: " + ex.Message + "\n Error Inner Exception: " + ex.InnerException);
				logger.Info(string.Concat("***********************************", DateTime.Now, "***********************************"));
			}
		}

		private static DataTable GetWeatherAlertDetail(List<WeatherAlerts> weatherAlerts)
		{
			DataTable dataTable = new DataTable();
			dataTable.Columns.Add("Id");
			dataTable.Columns.Add("Wind");
			dataTable.Columns.Add("Weather");
			dataTable.Columns.Add("Visibility");
			dataTable.Columns.Add("Sea_State");
			dataTable.Columns.Add("Warning");
			dataTable.Columns.Add("Outlook");
			dataTable.Columns.Add("Wa_Day");
			dataTable.Columns.Add("Type");
			int num = 0;
			foreach (WeatherAlerts weatherAlert in weatherAlerts)
			{
				dataTable.Rows.Add(num, weatherAlert.wind, weatherAlert.weather, weatherAlert.visibility, weatherAlert.seaState, weatherAlert.warning, weatherAlert.outlook, weatherAlert.day, weatherAlert.type);
				num++;
			}
			return dataTable;
		}

		private static DataTable GetCityWeatherInfoDetail(List<CityWeatherInfo> weatherInfos)
		{
			DataTable dataTable = new DataTable();
			dataTable.Columns.Add("Id");
			dataTable.Columns.Add("City");
			dataTable.Columns.Add("Temp_Max");
			dataTable.Columns.Add("Temp_Min");
			dataTable.Columns.Add("Hum_Max");
			dataTable.Columns.Add("Hum_Min");
			dataTable.Columns.Add("Weather");
			dataTable.Columns.Add("Ca_Day");
			int num = 0;
			foreach (CityWeatherInfo weatherInfo in weatherInfos)
			{
				dataTable.Rows.Add(num, weatherInfo.city, weatherInfo.tmax, weatherInfo.tmin, weatherInfo.humax, weatherInfo.humin, weatherInfo.weather, weatherInfo.day);
				num++;
			}
			return dataTable;
		}

		private static List<CityWeatherInfo> GeneratingCityWeatherData(JObject weatherParse, string b_id)
		{
			List<CityWeatherInfo> list = new List<CityWeatherInfo>();
			JToken? jToken = weatherParse["Cities"];
			int num = 0;
			string text = string.Empty;
			CityWeatherInfo cityWeatherInfo = new CityWeatherInfo();
			CityWeatherInfo cityWeatherInfo2 = new CityWeatherInfo();
			CityWeatherInfo cityWeatherInfo3 = new CityWeatherInfo();
			foreach (JToken item in (IEnumerable<JToken>)(jToken!))
			{
				if (string.IsNullOrEmpty(text) || item.SelectToken("City")!.ToString() != text)
				{
					if (!string.IsNullOrEmpty(text))
					{
						list.Add(cityWeatherInfo);
						list.Add(cityWeatherInfo2);
						list.Add(cityWeatherInfo3);
					}
					cityWeatherInfo = new CityWeatherInfo();
					cityWeatherInfo2 = new CityWeatherInfo();
					cityWeatherInfo3 = new CityWeatherInfo();
					text = (cityWeatherInfo3.city = (cityWeatherInfo2.city = (cityWeatherInfo.city = item.SelectToken("City")!.ToString())));
					cityWeatherInfo.batch_Id = b_id;
					cityWeatherInfo2.batch_Id = b_id;
					cityWeatherInfo3.batch_Id = b_id;
				}
				foreach (JToken item2 in (IEnumerable<JToken>)(item.SelectToken("Vals")!))
				{
					string text5 = item.SelectToken("Elem")!.ToString();
					switch (num)
					{
					case 0:
						cityWeatherInfo.day = num.ToString();
						switch (text5)
						{
						case "tmin":
							cityWeatherInfo.tmin = item2.ToString();
							num++;
							break;
						case "tmax":
							cityWeatherInfo.tmax = item2.ToString();
							num++;
							break;
						case "humin":
							cityWeatherInfo.humin = item2.ToString();
							num++;
							break;
						case "humax":
							cityWeatherInfo.humax = item2.ToString();
							num++;
							break;
						}
						break;
					case 1:
						cityWeatherInfo2.day = num.ToString();
						switch (text5)
						{
						case "tmin":
							cityWeatherInfo2.tmin = item2.ToString();
							num++;
							break;
						case "tmax":
							cityWeatherInfo2.tmax = item2.ToString();
							num++;
							break;
						case "humin":
							cityWeatherInfo2.humin = item2.ToString();
							num++;
							break;
						case "humax":
							cityWeatherInfo2.humax = item2.ToString();
							num++;
							break;
						}
						break;
					default:
						cityWeatherInfo3.day = num.ToString();
						switch (text5)
						{
						case "tmin":
							cityWeatherInfo3.tmin = item2.ToString();
							num = 0;
							break;
						case "tmax":
							cityWeatherInfo3.tmax = item2.ToString();
							num = 0;
							break;
						case "humin":
							cityWeatherInfo3.humin = item2.ToString();
							num = 0;
							break;
						case "humax":
							cityWeatherInfo3.humax = item2.ToString();
							num = 0;
							break;
						}
						break;
					}
				}
			}
			list.Add(cityWeatherInfo);
			list.Add(cityWeatherInfo2);
			list.Add(cityWeatherInfo3);
			return list;
		}

		private static List<WeatherAlerts> GeneratingWeatherAlertData(JObject weatherParse, string Type, string b_id)
		{
			List<WeatherAlerts> list = new List<WeatherAlerts>();
			JToken? jToken = weatherParse[Type]!["Wind"];
			WeatherAlerts weatherAlerts = new WeatherAlerts();
			WeatherAlerts weatherAlerts2 = new WeatherAlerts();
			WeatherAlerts weatherAlerts3 = new WeatherAlerts();
			int num = 0;
			foreach (JToken item in (IEnumerable<JToken>)(jToken!))
			{
				switch (num)
				{
				case 0:
					weatherAlerts.wind = item.ToString();
					weatherAlerts.day = num;
					weatherAlerts.batch_Id = b_id;
					weatherAlerts.type = Type;
					num++;
					break;
				case 1:
					weatherAlerts2.wind = item.ToString();
					weatherAlerts2.day = num;
					weatherAlerts2.batch_Id = b_id;
					weatherAlerts2.type = Type;
					num++;
					break;
				default:
					weatherAlerts3.wind = item.ToString();
					weatherAlerts3.day = num;
					weatherAlerts3.batch_Id = b_id;
					weatherAlerts3.type = Type;
					num = 0;
					break;
				}
			}
			foreach (JToken item2 in (IEnumerable<JToken>)(weatherParse[Type]!["Weather"]!))
			{
				switch (num)
				{
				case 0:
					weatherAlerts.weather = item2.ToString();
					num++;
					break;
				case 1:
					weatherAlerts2.weather = item2.ToString();
					num++;
					break;
				default:
					weatherAlerts3.weather = item2.ToString();
					num = 0;
					break;
				}
			}
			foreach (JToken item3 in (IEnumerable<JToken>)(weatherParse[Type]!["Visibility"]!))
			{
				switch (num)
				{
				case 0:
					weatherAlerts.visibility = item3.ToString();
					num++;
					break;
				case 1:
					weatherAlerts2.visibility = item3.ToString();
					num++;
					break;
				default:
					weatherAlerts3.visibility = item3.ToString();
					num = 0;
					break;
				}
			}
			foreach (JToken item4 in (IEnumerable<JToken>)(weatherParse[Type]!["SeaState"]!))
			{
				switch (num)
				{
				case 0:
					weatherAlerts.seaState = item4.ToString();
					num++;
					break;
				case 1:
					weatherAlerts2.seaState = item4.ToString();
					num++;
					break;
				default:
					weatherAlerts3.seaState = item4.ToString();
					num = 0;
					break;
				}
			}
			foreach (JToken item5 in (IEnumerable<JToken>)(weatherParse[Type]!["Warning"]!))
			{
				switch (num)
				{
				case 0:
					weatherAlerts.warning = item5.ToString();
					num++;
					break;
				case 1:
					weatherAlerts2.warning = item5.ToString();
					num++;
					break;
				default:
					weatherAlerts3.warning = item5.ToString();
					num = 0;
					break;
				}
			}
			JToken jToken2 = weatherParse[Type]!["Outlook"];
			weatherAlerts.outlook = jToken2.ToString();
			weatherAlerts2.outlook = jToken2.ToString();
			weatherAlerts3.outlook = jToken2.ToString();
			list.Add(weatherAlerts);
			list.Add(weatherAlerts2);
			list.Add(weatherAlerts3);
			return list;
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

		public static string GetWeatherData()
		{
			string requestUriString = ConfigurationManager.AppSettings["DNMC_Weather_Url"];
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
namespace QMD.WeatherAlert.Model
{
	public class CityWeatherInfo
	{
		public string city { get; set; }

		public string tmax { get; set; }

		public string tmin { get; set; }

		public string humax { get; set; }

		public string humin { get; set; }

		public string weather { get; set; }

		public string day { get; set; }

		public string batch_Id { get; set; }
	}
	public class WeatherAlerts
	{
		public string wind { get; set; }

		public string weather { get; set; }

		public string visibility { get; set; }

		public string seaState { get; set; }

		public string warning { get; set; }

		public string outlook { get; set; }

		public int day { get; set; }

		public string batch_Id { get; set; }

		public string type { get; set; }
	}
}

