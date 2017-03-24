using Microsoft.Extensions.Configuration;
using MyRainbow.DBProviders;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Security.Cryptography;

namespace MyRainbow
{
	public class HashCreator
	{
		private IEnumerable<IEnumerable<char>> _tableOfTableOfChars;
		private MD5 _hasherMD5;
		private Stopwatch _stopwatch;
		private SHA256 _hasherSHA256;
		private bool _purge;

		private IConfiguration Configuration { get; set; }

		public HashCreator()
		{
		}

		public void SetupConfiguration(string[] args)
		{
			var builder = new ConfigurationBuilder()
			  // For more details on using the user secret store see http://go.microsoft.com/fwlink/?LinkID=532709
			  .AddUserSecrets()
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

		internal void SqlServerExample()
		{
			using (var dbase = new SqlDatabaseHasher(GetParamFromCmdSecretOrEnv<string>("SqlConnection")))
			{
				dbase.EnsureExist();
				if (_purge)
					dbase.Purge();

				bool interrupted = false;

				dbase.Generate(_tableOfTableOfChars, _hasherMD5, _hasherSHA256,
					(key, hashMD5, hashSHA256, counter, tps) =>
					{
						Console.WriteLine($"MD5({key})={hashMD5},SHA256({key})={hashSHA256},counter={counter},tps={tps}");
						if (Console.KeyAvailable)
						{
							interrupted = true;
							return true;
						}
						return false;
					}, _stopwatch);

				_stopwatch.Stop();

				if (!interrupted)
					dbase.PostGenerateExecute();
				dbase.Verify();
			}
		}

		internal void RedisExample()
		{
			using (var dbase = new RedisHasher(GetParamFromCmdSecretOrEnv<string>("Redis")))
			{
				dbase.EnsureExist();
				if (_purge)
					dbase.Purge();

				bool interrupted = false;

				dbase.Generate(_tableOfTableOfChars, _hasherMD5, _hasherSHA256,
					(key, hashMD5, hashSHA256, counter, tps) =>
					{
						Console.WriteLine($"MD5({key})={hashMD5},SHA256({key})={hashSHA256},counter={counter},tps={tps}");
						if (Console.KeyAvailable)
						{
							interrupted = true;
							return true;
						}
						return false;
					}, _stopwatch);

				_stopwatch.Stop();

				if (!interrupted)
					dbase.PostGenerateExecute();
				dbase.Verify();
			}
		}

		internal void MongoDBExample()
		{
			using (var dbase = new MongoDBHasher(GetParamFromCmdSecretOrEnv<string>("MongoDB")))
			{
				dbase.EnsureExist();
				if (_purge)
					dbase.Purge();

				bool interrupted = false;

				dbase.Generate(_tableOfTableOfChars, _hasherMD5, _hasherSHA256,
					(key, hashMD5, hashSHA256, counter, tps) =>
					{
						Console.WriteLine($"MD5({key})={hashMD5},SHA256({key})={hashSHA256},counter={counter},tps={tps}");
						if (Console.KeyAvailable)
						{
							interrupted = true;
							return true;
						}
						return false;
					}, _stopwatch);

				_stopwatch.Stop();

				if (!interrupted)
					dbase.PostGenerateExecute();
				dbase.Verify();
			}
		}

		internal void CassandraExample()
		{
			using (var dbase = new CassandraDBHasher(GetParamFromCmdSecretOrEnv<string>("Cassandra")))
			{
				dbase.EnsureExist();
				if (_purge)
					dbase.Purge();

				bool interrupted = false;

				dbase.Generate(_tableOfTableOfChars, _hasherMD5, _hasherSHA256,
					(key, hashMD5, hashSHA256, counter, tps) =>
					{
						Console.WriteLine($"MD5({key})={hashMD5},SHA256({key})={hashSHA256},counter={counter},tps={tps}");
						if (Console.KeyAvailable)
						{
							interrupted = true;
							return true;
						}
						return false;
					}, _stopwatch);

				_stopwatch.Stop();

				if (!interrupted)
					dbase.PostGenerateExecute();
				dbase.Verify();
			}
		}

		internal void MySqlExample()
		{
			using (var dbase = new MySqlDatabaseHasher(GetParamFromCmdSecretOrEnv<string>("MySQL")))
			{
				dbase.EnsureExist();
				if (_purge)
					dbase.Purge();

				bool interrupted = false;

				dbase.Generate(_tableOfTableOfChars, _hasherMD5, _hasherSHA256,
					(key, hashMD5, hashSHA256, counter, tps) =>
					{
						Console.WriteLine($"MD5({key})={hashMD5},SHA256({key})={hashSHA256},counter={counter},tps={tps}");
						if (Console.KeyAvailable)
						{
							interrupted = true;
							return true;
						}
						return false;
					}, _stopwatch);

				_stopwatch.Stop();

				if (!interrupted)
					dbase.PostGenerateExecute();
				dbase.Verify();
			}
		}

		internal void SqlLiteExample()
		{
			using (var dbase = new SqliteHasher(GetParamFromCmdSecretOrEnv<string>("Sqlite")))
			{
				dbase.EnsureExist();
				if (_purge)
					dbase.Purge();

				bool interrupted = false;

				dbase.Generate(_tableOfTableOfChars, _hasherMD5, _hasherSHA256,
					(key, hashMD5, hashSHA256, counter, tps) =>
					{
						Console.WriteLine($"MD5({key})={hashMD5},SHA256({key})={hashSHA256},counter={counter},tps={tps}");
						if (Console.KeyAvailable)
						{
							interrupted = true;
							return true;
						}
						return false;
					}, _stopwatch);

				_stopwatch.Stop();

				if (!interrupted)
					dbase.PostGenerateExecute();
				dbase.Verify();
			}
		}

		internal int Exeute()
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
			catch (Exception ex)
			{
				Console.Error.WriteLineAsync($@"{Environment.NewLine}Rainbow table simplistic generator. Running method: {GetType().Namespace} /DBKind=[string,values(sqlserver,mysql,redis,cassandra,sqlite)]"
					+ $@"/Alphabet=[string,default:abcdefghijklmopqrstuvwxyz] /Length=[int,default:5] /Purge=[bool,default:false]{Environment.NewLine}{Environment.NewLine}"
					+ $@"/SqlConnection='...' or /MySQL='...' or /[other db connection]='...'");
				Console.ReadKey();
				throw ex;
			}
			using (_hasherMD5 = MD5.Create())
			using (_hasherSHA256 = SHA256.Create())
			{
				_stopwatch = new Stopwatch();


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
					case "casandra":
					case "cassandradb":
						CassandraExample();
						break;

					case "sqlite":
					case "lite":
						SqlLiteExample();
						break;

					default:
						throw new NotSupportedException($"Unknown DBKind {db_kind}");
				}

				Console.WriteLine($"Done. Elpased time = {_stopwatch.Elapsed}");
				//Console.ReadKey();

				return 0;
			}//end using(s)
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
				return hc.Exeute();
			}
			catch (Exception ex)
			{
				Console.Error.WriteLineAsync(ex.Message + Environment.NewLine + ex.StackTrace);
				return 1;
			}
		}
	}
}
