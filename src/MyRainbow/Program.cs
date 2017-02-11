using Microsoft.Extensions.Configuration;
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
			/*MyCartesian cart = new MyCartesian(5, "abcdefghijklmopqrstuvwxyz");

			Console.WriteLine($"Alphabet = {cart.Alphabet}{Environment.NewLine}" +
				$"AlphabetPower = {cart.AlphabetPower}{Environment.NewLine}Length = {cart.Length}{Environment.NewLine}" +
				$"Combination count = {cart.CombinationCount}");

			var table_of_table_of_chars = cart.Generate2();
			Console.WriteLine("Keys generated");
			var hasherMD5 = MD5.Create();
			var hasherSHA256 = SHA256.Create();
			var stopwatch = new Stopwatch();*/

			using (var dbase = new SqlDatabaseHasher(GetParamFromCmdSecretOrEnv<string>("SqlConnection")))
			{
				dbase.EnsureExist();
				if (_purge)
					dbase.Purge();

				dbase.Generate(_tableOfTableOfChars, _hasherMD5, _hasherSHA256,
					(key, hashMD5, hashSHA256, counter, tps) =>
					{
						Console.WriteLine($"MD5({key})={hashMD5},SHA256({key})={hashSHA256},counter={counter},tps={tps}");
						if (Console.KeyAvailable)
							return true;
						return false;
					}, _stopwatch);

				_stopwatch.Stop();

				dbase.Verify();
			}

			Console.WriteLine($"Done. Elpased time = {_stopwatch.Elapsed}");
			Console.ReadKey();
		}

		internal void RedisExample()
		{
			/*MyCartesian cart = new MyCartesian(5, "abcdefghijklmopqrstuvwxyz");

			Console.WriteLine($"Alphabet = {cart.Alphabet}{Environment.NewLine}" +
				$"AlphabetPower = {cart.AlphabetPower}{Environment.NewLine}Length = {cart.Length}{Environment.NewLine}" +
				$"Combination count = {cart.CombinationCount}");

			var table_of_table_of_chars = cart.Generate2();
			Console.WriteLine("Keys generated");
			var hasherMD5 = MD5.Create();
			var hasherSHA256 = SHA256.Create();
			var stopwatch = new Stopwatch();*/

			using (var dbase = new RedisHasher(GetParamFromCmdSecretOrEnv<string>("Redis")))
			{
				dbase.EnsureExist();
				if (_purge)
					dbase.Purge();

				dbase.Generate(_tableOfTableOfChars, _hasherMD5, _hasherSHA256,
					(key, hashMD5, hashSHA256, counter, tps) =>
					{
						Console.WriteLine($"MD5({key})={hashMD5},SHA256({key})={hashSHA256},counter={counter},tps={tps}");
						if (Console.KeyAvailable)
							return true;
						return false;
					}, _stopwatch);

				_stopwatch.Stop();

				dbase.Verify();
			}

			Console.WriteLine($"Done. Elpased time = {_stopwatch.Elapsed}");
			Console.ReadKey();
		}

		internal void MongoDBExample()
		{
			/*MyCartesian cart = new MyCartesian(5, "abcdefghijklmopqrstuvwxyz");

			Console.WriteLine($"Alphabet = {cart.Alphabet}{Environment.NewLine}" +
				$"AlphabetPower = {cart.AlphabetPower}{Environment.NewLine}Length = {cart.Length}{Environment.NewLine}" +
				$"Combination count = {cart.CombinationCount}");

			var table_of_table_of_chars = cart.Generate2();
			Console.WriteLine("Keys generated");
			var hasherMD5 = MD5.Create();
			var hasherSHA256 = SHA256.Create();
			var stopwatch = new Stopwatch();*/

			using (var dbase = new MongoDBHasher(GetParamFromCmdSecretOrEnv<string>("MongoDB")))
			{
				dbase.EnsureExist();
				if (_purge)
					dbase.Purge();

				dbase.Generate(_tableOfTableOfChars, _hasherMD5, _hasherSHA256,
					(key, hashMD5, hashSHA256, counter, tps) =>
					{
						Console.WriteLine($"MD5({key})={hashMD5},SHA256({key})={hashSHA256},counter={counter},tps={tps}");
						if (Console.KeyAvailable)
							return true;
						return false;
					}, _stopwatch);

				_stopwatch.Stop();

				dbase.Verify();
			}

			Console.WriteLine($"Done. Elpased time = {_stopwatch.Elapsed}");
			Console.ReadKey();
		}

		internal void CassandraExample()
		{
			/*MyCartesian cart = new MyCartesian(5, "abcdefghijklmopqrstuvwxyz");

			Console.WriteLine($"Alphabet = {cart.Alphabet}{Environment.NewLine}" +
				$"AlphabetPower = {cart.AlphabetPower}{Environment.NewLine}Length = {cart.Length}{Environment.NewLine}" +
				$"Combination count = {cart.CombinationCount}");

			var table_of_table_of_chars = cart.Generate2();
			Console.WriteLine("Keys generated");
			var hasherMD5 = MD5.Create();
			var hasherSHA256 = SHA256.Create();
			var stopwatch = new Stopwatch();*/

			using (var dbase = new CassandraDBHasher(GetParamFromCmdSecretOrEnv<string>("Cassandra")))
			{
				dbase.EnsureExist();
				if (_purge)
					dbase.Purge();

				dbase.Generate(_tableOfTableOfChars, _hasherMD5, _hasherSHA256,
					(key, hashMD5, hashSHA256, counter, tps) =>
					{
						Console.WriteLine($"MD5({key})={hashMD5},SHA256({key})={hashSHA256},counter={counter},tps={tps}");
						if (Console.KeyAvailable)
							return true;
						return false;
					}, _stopwatch);

				_stopwatch.Stop();

				dbase.Verify();
			}

			Console.WriteLine($"Done. Elpased time = {_stopwatch.Elapsed}");
			Console.ReadKey();
		}

		internal void MySqlExample()
		{
			/*MyCartesian cart = new MyCartesian(5, "abcdefghijklmopqrstuvwxyz");

			Console.WriteLine($"Alphabet = {cart.Alphabet}{Environment.NewLine}" +
				$"AlphabetPower = {cart.AlphabetPower}{Environment.NewLine}Length = {cart.Length}{Environment.NewLine}" +
				$"Combination count = {cart.CombinationCount}");

			var table_of_table_of_chars = cart.Generate2();
			Console.WriteLine("Keys generated");
			var hasherMD5 = MD5.Create();
			var hasherSHA256 = SHA256.Create();
			var stopwatch = new Stopwatch();*/

			using (var dbase = new MySqlDatabaseHasher(GetParamFromCmdSecretOrEnv<string>("MySQL")))
			{
				dbase.EnsureExist();
				if (_purge)
					dbase.Purge();

				dbase.Generate(_tableOfTableOfChars, _hasherMD5, _hasherSHA256,
					(key, hashMD5, hashSHA256, counter, tps) =>
					{
						Console.WriteLine($"MD5({key})={hashMD5},SHA256({key})={hashSHA256},counter={counter},tps={tps}");
						if (Console.KeyAvailable)
							return true;
						return false;
					}, _stopwatch);

				_stopwatch.Stop();

				dbase.Verify();
			}

			Console.WriteLine($"Done. Elpased time = {_stopwatch.Elapsed}");
			Console.ReadKey();
		}

		internal void SqlLiteExample()
		{
			/*MyCartesian cart = new MyCartesian(5, "abcdefghijklmopqrstuvwxyz");

			Console.WriteLine($"Alphabet = {cart.Alphabet}{Environment.NewLine}" +
				$"AlphabetPower = {cart.AlphabetPower}{Environment.NewLine}Length = {cart.Length}{Environment.NewLine}" +
				$"Combination count = {cart.CombinationCount}");

			var table_of_table_of_chars = cart.Generate2();
			Console.WriteLine("Keys generated");
			var hasherMD5 = MD5.Create();
			var hasherSHA256 = SHA256.Create();
			var stopwatch = new Stopwatch();*/

			using (var dbase = new SqliteHasher(GetParamFromCmdSecretOrEnv<string>("Sqlite")))
			{
				dbase.EnsureExist();
				if (_purge)
					dbase.Purge();

				dbase.Generate(_tableOfTableOfChars, _hasherMD5, _hasherSHA256,
					(key, hashMD5, hashSHA256, counter, tps) =>
					{
						Console.WriteLine($"MD5({key})={hashMD5},SHA256({key})={hashSHA256},counter={counter},tps={tps}");
						if (Console.KeyAvailable)
							return true;
						return false;
					}, _stopwatch);

				_stopwatch.Stop();

				dbase.Verify();
			}

			Console.WriteLine($"Done. Elpased time = {_stopwatch.Elapsed}");
			Console.ReadKey();
		}

		internal void Exeute()
		{
			var db_kind = GetParamFromCmdSecretOrEnv<string>("DBKind");
			var alphabet = GetParamFromCmdSecretOrEnv<string>("alphabet") ?? "abcdefghijklmopqrstuvwxyz";
			var length = GetParamFromCmdSecretOrEnv<int?>("length") ?? 3;
			_purge = GetParamFromCmdSecretOrEnv<bool?>("purge") ?? false;

			MyCartesian cart = new MyCartesian(/*5*/length, /*"abcdefghijklmopqrstuvwxyz"*/alphabet);

			Console.WriteLine($"Alphabet = {cart.Alphabet}{Environment.NewLine}" +
				$"AlphabetPower = {cart.AlphabetPower}{Environment.NewLine}Length = {cart.Length}{Environment.NewLine}" +
				$"Combination count = {cart.CombinationCount}");

			_tableOfTableOfChars = cart.Generate2();
			Console.WriteLine("Keys generated");
			_hasherMD5 = MD5.Create();
			_hasherSHA256 = SHA256.Create();
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
