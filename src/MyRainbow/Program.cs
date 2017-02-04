using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MyRainbow
{
	public class HashCreator
	{
		private IConfiguration Configuration { get; set; }

		/*private Dictionary<string, string> GetSwitchMappings(IReadOnlyDictionary<string, string> configurationStrings)
		{
			return configurationStrings.Select(item =>
				new KeyValuePair<string, string>(
					"-" + item.Key.Substring(item.Key.LastIndexOf(':') + 1),
					item.Key))
					.ToDictionary(
						item => item.Key, item => item.Value);
		}*/

		public void SetupConfiguration(string[] args)
		{
			/*var opt_dict = new Dictionary<string, string>
			{
				{ "aaSqlConnection", "Rick" },
				{ "zzzMySQL", "11" },
				{ "zzzRedis", "blah" },
				{ "zzzMongoDB", "buuu" },
				{ "zzCassandra", "cass" },
				{ "zzzDBKind", "unknown" }
			};*/

			var builder = new ConfigurationBuilder()
			  // For more details on using the user secret store see http://go.microsoft.com/fwlink/?LinkID=532709
			  .AddUserSecrets()
			  .AddEnvironmentVariables()
			  //.AddInMemoryCollection(opt_dict)
			  .AddCommandLine(args/*, GetSwitchMappings(opt_dict)*/);
			var configuration = builder.Build();

			Configuration = configuration;
		}

		private string GetParamFromCmdSecretOrEnv(string configParam = "SqlConnection")
		{
			string conn_str = Configuration[configParam];
			return conn_str;
		}

		internal void SqlServerExample()
		{
			MyCartesian cart = new MyCartesian(5, "abcdefghijklmopqrstuvwxyz");

			Console.WriteLine($"Alphabet = {cart.Alphabet}{Environment.NewLine}" +
				$"AlphabetPower = {cart.AlphabetPower}{Environment.NewLine}Length = {cart.Length}{Environment.NewLine}" +
				$"Combination count = {cart.CombinationCount}");

			var table_of_table_of_chars = cart.Generate2();
			Console.WriteLine("Keys generated");
			var hasherMD5 = MD5.Create();
			var hasherSHA256 = SHA256.Create();
			var stopwatch = new Stopwatch();

			using (var dbase = new SqlDatabaseHasher(GetParamFromCmdSecretOrEnv("SqlConnection")))
			{
				dbase.EnsureExist();
				//dbase.Purge();

				dbase.Generate(table_of_table_of_chars, hasherMD5, hasherSHA256,
					(key, hashMD5, hashSHA256, counter, tps) =>
					{
						Console.WriteLine($"MD5({key})={hashMD5},SHA256({key})={hashSHA256},counter={counter},tps={tps}");
						if (Console.KeyAvailable)
							return true;
						return false;
					}, stopwatch);

				stopwatch.Stop();

				dbase.Verify();
			}

			Console.WriteLine($"Done. Elpased time = {stopwatch.Elapsed}");
			Console.ReadKey();
		}

		internal void RedisExample()
		{
			MyCartesian cart = new MyCartesian(5, "abcdefghijklmopqrstuvwxyz");

			Console.WriteLine($"Alphabet = {cart.Alphabet}{Environment.NewLine}" +
				$"AlphabetPower = {cart.AlphabetPower}{Environment.NewLine}Length = {cart.Length}{Environment.NewLine}" +
				$"Combination count = {cart.CombinationCount}");

			var table_of_table_of_chars = cart.Generate2();
			Console.WriteLine("Keys generated");
			var hasherMD5 = MD5.Create();
			var hasherSHA256 = SHA256.Create();
			var stopwatch = new Stopwatch();

			using (var redis = new RedisHasher(GetParamFromCmdSecretOrEnv("Redis")))
			{
				redis.EnsureExist();
				redis.Purge();

				redis.Generate(table_of_table_of_chars, hasherMD5, hasherSHA256,
					(key, hashMD5, hashSHA256, counter, tps) =>
					{
						Console.WriteLine($"MD5({key})={hashMD5},SHA256({key})={hashSHA256},counter={counter},tps={tps}");
						if (Console.KeyAvailable)
							return true;
						return false;
					}, stopwatch);

				stopwatch.Stop();

				redis.Verify();
			}

			Console.WriteLine($"Done. Elpased time = {stopwatch.Elapsed}");
			Console.ReadKey();
		}

		internal void MongoDBExample()
		{
			MyCartesian cart = new MyCartesian(5, "abcdefghijklmopqrstuvwxyz");

			Console.WriteLine($"Alphabet = {cart.Alphabet}{Environment.NewLine}" +
				$"AlphabetPower = {cart.AlphabetPower}{Environment.NewLine}Length = {cart.Length}{Environment.NewLine}" +
				$"Combination count = {cart.CombinationCount}");

			var table_of_table_of_chars = cart.Generate2();
			Console.WriteLine("Keys generated");
			var hasherMD5 = MD5.Create();
			var hasherSHA256 = SHA256.Create();
			var stopwatch = new Stopwatch();

			using (var dbase = new MongoDBHasher(GetParamFromCmdSecretOrEnv("MongoDB")))
			{
				dbase.EnsureExist();
				//dbase.Purge();

				dbase.Generate(table_of_table_of_chars, hasherMD5, hasherSHA256,
					(key, hashMD5, hashSHA256, counter, tps) =>
					{
						Console.WriteLine($"MD5({key})={hashMD5},SHA256({key})={hashSHA256},counter={counter},tps={tps}");
						if (Console.KeyAvailable)
							return true;
						return false;
					}, stopwatch);

				stopwatch.Stop();

				dbase.Verify();
			}

			Console.WriteLine($"Done. Elpased time = {stopwatch.Elapsed}");
			Console.ReadKey();
		}

		internal void CassandraExample()
		{
			MyCartesian cart = new MyCartesian(3, "abcdefghijklmopqrstuvwxyz");

			Console.WriteLine($"Alphabet = {cart.Alphabet}{Environment.NewLine}" +
				$"AlphabetPower = {cart.AlphabetPower}{Environment.NewLine}Length = {cart.Length}{Environment.NewLine}" +
				$"Combination count = {cart.CombinationCount}");

			var table_of_table_of_chars = cart.Generate2();
			Console.WriteLine("Keys generated");
			var hasherMD5 = MD5.Create();
			var hasherSHA256 = SHA256.Create();
			var stopwatch = new Stopwatch();

			using (var dbase = new CassandraDBHasher(GetParamFromCmdSecretOrEnv("Cassandra")))
			{
				dbase.EnsureExist();
				//dbase.Purge();

				dbase.Generate(table_of_table_of_chars, hasherMD5, hasherSHA256,
					(key, hashMD5, hashSHA256, counter, tps) =>
					{
						Console.WriteLine($"MD5({key})={hashMD5},SHA256({key})={hashSHA256},counter={counter},tps={tps}");
						if (Console.KeyAvailable)
							return true;
						return false;
					}, stopwatch);

				stopwatch.Stop();

				dbase.Verify();
			}

			Console.WriteLine($"Done. Elpased time = {stopwatch.Elapsed}");
			Console.ReadKey();
		}

		internal void MySqlExample()
		{
			MyCartesian cart = new MyCartesian(5, "abcdefghijklmopqrstuvwxyz");

			Console.WriteLine($"Alphabet = {cart.Alphabet}{Environment.NewLine}" +
				$"AlphabetPower = {cart.AlphabetPower}{Environment.NewLine}Length = {cart.Length}{Environment.NewLine}" +
				$"Combination count = {cart.CombinationCount}");

			var table_of_table_of_chars = cart.Generate2();
			Console.WriteLine("Keys generated");
			var hasherMD5 = MD5.Create();
			var hasherSHA256 = SHA256.Create();
			var stopwatch = new Stopwatch();

			using (var dbase = new MySqlDatabaseHasher(GetParamFromCmdSecretOrEnv("MySQL")))
			{
				dbase.EnsureExist();
				//dbase.Purge();

				dbase.Generate(table_of_table_of_chars, hasherMD5, hasherSHA256,
					(key, hashMD5, hashSHA256, counter, tps) =>
					{
						Console.WriteLine($"MD5({key})={hashMD5},SHA256({key})={hashSHA256},counter={counter},tps={tps}");
						if (Console.KeyAvailable)
							return true;
						return false;
					}, stopwatch);

				stopwatch.Stop();

				dbase.Verify();
			}

			Console.WriteLine($"Done. Elpased time = {stopwatch.Elapsed}");
			Console.ReadKey();
		}

		internal void Exeute()
		{
			var db_kind = GetParamFromCmdSecretOrEnv("DBKind");

			switch (db_kind?.Trim()?.ToLower())
			{
				case "sqlserver":
				case "mssql":
					SqlServerExample();
					break;

				case "mysql":
				case "mariadb":
				case "maria":
					MySqlExample();
					break;

				case "redis":
					RedisExample();
					break;

				case "mongo":
				case "mongodb":
					MongoDBExample();
					break;

				case "cassandra":
				case "cassandradb":
					CassandraExample();
					break;

				default:
					throw new NotSupportedException($"Unknown DBKind {db_kind}");
			}
		}
	}

	public class Program
	{
		public static int Main(string[] args)
		{
			try
			{
				HashCreator hc = new HashCreator();
				hc.SetupConfiguration(args);
				hc.Exeute();

				return 0;
			}
			catch (Exception ex)
			{
				Console.Error.WriteLineAsync(ex.Message + Environment.NewLine + ex.StackTrace);
				return 1;
			}
		}
	}
}
