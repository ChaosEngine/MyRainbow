using Microsoft.Extensions.Configuration;
using MyRainbow.DBProviders;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Threading.Tasks;

namespace MyRainbow
{
	public class HashCreator
	{
		private IEnumerable<IEnumerable<char>> _tableOfTableOfChars;
		private MD5 _hasherMD5;
		private SHA256 _hasherSHA256;
		private Stopwatch _stopwatch;
		private bool _purge;
		private bool _interrupted;

		private IConfiguration Configuration { get; set; }

		public HashCreator()
		{
		}

		public void SetupConfiguration(string[] args)
		{
			var builder = new ConfigurationBuilder()
			  // For more details on using the user secret store see http://go.microsoft.com/fwlink/?LinkID=532709
			  .AddUserSecrets<HashCreator>()
			  .AddEnvironmentVariables()
			  .AddCommandLine(args);

			var configuration = builder.Build();
			Configuration = configuration;
		}

		private T GetParamFromCmdSecretOrEnv<T>(string configParam = "SqlConnection")
		{
			string from = Configuration[configParam];
			if (from == null)
				return default(T);

			return TConverter.ChangeType<T>(from);
		}

		private IConfigurationSection GetSectionFromCmdSecretOrEnv(string section)
		{
			IConfigurationSection from = Configuration.GetSection(section);
			if (from == null)
				return null;

			return from;
		}

		private bool ShouldStopFunction(string key, string hashMD5, string hashSHA256, long counter, long tps)
		{
			Console.WriteLine($"MD5({key})={hashMD5},SHA256({key})={hashSHA256},counter={counter},tps={tps}");
			if (Console.KeyAvailable)
			{
				_interrupted = true;
				return true;
			}
			return false;
		}

		internal async Task ExecuteSteps(IDbHasher dbase)
		{
			await dbase.EnsureExist();
			if (_purge)
				await dbase.Purge();

			_interrupted = false;

			await dbase.Generate(_tableOfTableOfChars, _hasherMD5, _hasherSHA256, ShouldStopFunction, _stopwatch);

			_stopwatch.Stop();

			if (!_interrupted)
				await dbase.PostGenerateExecute();
			await dbase.Verify();
		}

		#region Examples

		internal async Task SqlServerExample()
		{
			using (var dbase = new SqlDatabaseHasher(GetParamFromCmdSecretOrEnv<string>("SqlConnection")))
				await ExecuteSteps(dbase);
		}

		internal async Task RedisExample()
		{
			using (var dbase = new RedisHasher(GetParamFromCmdSecretOrEnv<string>("Redis")))
				await ExecuteSteps(dbase);
		}

		internal async Task MongoDBExampleAsync()
		{
			using (var dbase = new MongoDBHasher(GetParamFromCmdSecretOrEnv<string>("MongoDB")))
				await ExecuteSteps(dbase);
		}

		internal async Task CassandraExampleAsync()
		{
			using (var dbase = new CassandraDBHasher(GetParamFromCmdSecretOrEnv<string>("Cassandra")))
				await ExecuteSteps(dbase);
		}

		internal async Task MySqlExampleAsync()
		{
			using (var dbase = new MySqlDatabaseHasher(GetParamFromCmdSecretOrEnv<string>("MySQL")))
				await ExecuteSteps(dbase);
		}

		internal async Task PostgreSqlExampleAsync()
		{
			using (var dbase = new PostgreSqlDatabaseHasher(GetParamFromCmdSecretOrEnv<string>("PostgreSql")))
				await ExecuteSteps(dbase);
		}

		internal async Task SqlLiteExampleAsync()
		{
			using (var dbase = new SqliteHasher(GetParamFromCmdSecretOrEnv<string>("Sqlite")))
				await ExecuteSteps(dbase);
		}

		internal async Task CosmosDBExampleAsync()
		{
			using (var dbase = new CosmosDBHasher(GetSectionFromCmdSecretOrEnv("CosmosDB")))
				await ExecuteSteps(dbase);
		}

		internal async Task OracleExampleAsync()
		{
			using (var dbase = new OracleHasher(GetParamFromCmdSecretOrEnv<string>("Oracle")))
				await ExecuteSteps(dbase);
		}

		#endregion Examples

		internal async Task<int> ExeuteAsync()
		{
			string db_kind;
			try
			{
				db_kind = GetParamFromCmdSecretOrEnv<string>("DBKind");
				var alphabet = GetParamFromCmdSecretOrEnv<string>("alphabet") ?? "abcdefghijklmopqrstuvwxyz";
				var length = GetParamFromCmdSecretOrEnv<int?>("length") ?? 5;
				_purge = GetParamFromCmdSecretOrEnv<bool?>("purge") ?? false;

				MyCartesian cart = new MyCartesian(/*5*/length, /*"abcdefghijklmopqrstuvwxyz"*/alphabet);

				Console.WriteLine($"Alphabet = {cart.Alphabet}{Environment.NewLine}" +
					$"AlphabetPower = {cart.AlphabetPower}{Environment.NewLine}Length = {cart.Length}{Environment.NewLine}" +
					$"Combination count = {cart.CombinationCount}{Environment.NewLine}" +
					$"DBKind = {db_kind.Trim().ToLower()}{Environment.NewLine}");

				_tableOfTableOfChars = cart.Generate2();
				Console.WriteLine("Keys generated");
			}
			catch (Exception)
			{
				await Console.Error.WriteLineAsync($@"{Environment.NewLine}Rainbow table simplistic generator. Running method: {GetType().Namespace} /DBKind=[string,values(sqlserver,mysql,redis,cassandra,sqlite)]"
					+ $@"/Alphabet=[string,default:abcdefghijklmopqrstuvwxyz] /Length=[int,default:5] /Purge=[bool,default:false]{Environment.NewLine}{Environment.NewLine}"
					+ $@"/SqlConnection='...' or /MySQL='...' or /Sqlite='Filename=./database.db' or /[other db connection]='...'");
				Console.ReadKey();
				throw;
			}
			using (_hasherMD5 = MD5.Create())
			using (_hasherSHA256 = SHA256.Create())
			{
				_stopwatch = new Stopwatch();

				switch (db_kind?.Trim()?.ToLower())
				{
					case "sqlserver":
					case "mssql":
						await SqlServerExample();
						break;

					case "mysql":
					case "mariadb":
					case "maria":
						await MySqlExampleAsync();
						break;

					case "redis":
						await RedisExample();
						break;

					case "mongo":
					case "mongodb":
						await MongoDBExampleAsync();
						break;

					case "cassandra":
					case "casandra":
					case "cassandradb":
						await CassandraExampleAsync();
						break;

					case "sqlite":
					case "lite":
						await SqlLiteExampleAsync();
						break;

					case "psql":
					case "npsql":
					case "postgres":
					case "postgresql":
						await PostgreSqlExampleAsync();
						break;

					case "cosmosdb":
					case "cosmos":
					case "azuredb":
					case "azure":
						await CosmosDBExampleAsync();
						break;

					case "oracle":
					case "orcle":
					case "orcl":
						await OracleExampleAsync();
						break;

					default:
						throw new NotSupportedException($"Unknown DBKind {db_kind}");
				}

				Console.WriteLine($"Done. Elpased time = {_stopwatch.Elapsed}");

				return 0;
			}//end using(s)
		}

		public static async Task<int> Main(string[] args)
		{
			try
			{
				HashCreator hc = new HashCreator();
				hc.SetupConfiguration(args);
				return await hc.ExeuteAsync();
			}
			catch (Exception ex)
			{
				await Console.Error.WriteLineAsync(ex.Message + Environment.NewLine + ex.StackTrace);
				Debug.WriteLine(ex.Message + Environment.NewLine + ex.StackTrace);
				return 1;
			}
		}
	}
}
