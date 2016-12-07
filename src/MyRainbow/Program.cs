using Microsoft.Extensions.Configuration;
using System;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MyRainbow
{
	public class Program
	{
		static string GetConnectionStringFromSecret(string[] args)
		{
			var builder = new ConfigurationBuilder()
			  // For more details on using the user secret store see http://go.microsoft.com/fwlink/?LinkID=532709
			  .AddUserSecrets()
			  .AddEnvironmentVariables()
			  .AddCommandLine(args);
			var configuration = builder.Build();

			string conn_str = configuration["SqlConnection"];
			return conn_str;
		}

		public static void Main(string[] args)
		{
			MyCartesian cart = new MyCartesian(5, "abcdefghijklmopqrstuvwxyz");

			Console.WriteLine($"Alphabet = {cart.Alphabet}{Environment.NewLine}" +
				$"AlphabetPower = {cart.AlphabetPower}{Environment.NewLine}Length = {cart.Length}{Environment.NewLine}" +
				$"Combination count = {cart.CombinationCount}");

			var table_of_table_of_chars = cart.Generate2();
			Console.WriteLine("Keys generated");
			var hasher = MD5.Create();
			var stopwatch = new Stopwatch();
			using (var conn = new SqlConnection(GetConnectionStringFromSecret(args)))
			{
				conn.Open();
				using (var dbase = new DatabaseHasher(conn))
				{
					dbase.EnsureExist();
					dbase.Purge();

					stopwatch.Start();
					long counter = 0;
					dbase.Tran = conn.BeginTransaction(System.Data.IsolationLevel.ReadUncommitted);
					foreach (var chars_table in table_of_table_of_chars)
					{
						var value = string.Concat(chars_table);
						//if ("tdtzz".CompareTo(value) >= 0) continue;
						var hash = BitConverter.ToString(hasher.ComputeHash(Encoding.UTF8.GetBytes(value))).Replace("-", "").ToLowerInvariant();

						dbase.Insert(value, hash);

						if (counter % 10000 == 0)
						{
							dbase.Tran.Commit();

							Console.WriteLine($"MD5({value}) = {hash}, counter = {counter}");
							if (Console.KeyAvailable)
								break;

							dbase.Tran = dbase.Conn.BeginTransaction(System.Data.IsolationLevel.ReadUncommitted);
						}
						counter++;
					}

					if (dbase.Tran != null)
						dbase.Tran.Commit();

					//Parallel.ForEach(table_of_table_of_chars, (chars_table, parallelLoopState, counter) =>
					//{
					//    if (parallelLoopState.IsStopped || parallelLoopState.IsExceptional || parallelLoopState.ShouldExitCurrentIteration)
					//        return;

					//    var value = string.Concat(chars_table);
					//    var hash = BitConverter.ToString(hasher.ComputeHash(Encoding.UTF8.GetBytes(value))).Replace("-", "").ToLowerInvariant();

					//    if (counter % 10000 == 0)
					//    {
					//        Console.WriteLine($"MD5({value}) = {hash}, counter = {counter}");
					//        if (Console.KeyAvailable)
					//            parallelLoopState.Stop();
					//    }

					//    //dbase.Insert(value, hash);
					//});
					stopwatch.Stop();
				}
			}
			/*try
			{
				var conn_str = GetConnectionStringFromSecret(args);

				Parallel.ForEach(
					table_of_table_of_chars,                          // source collection
					() =>                                             // thread local initializer
					{
						var conn = new SqlConnection(conn_str);
						conn.Open();
						var tran = conn.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);

						var database = new DatabaseHasher(conn, tran);
						Console.WriteLine("new DatabaseHasher");
						return database;
					},
					(chars_table, parallelLoopState, counter, database) =>     // body
					{
						if (parallelLoopState.IsStopped || parallelLoopState.IsExceptional || parallelLoopState.ShouldExitCurrentIteration)
							return database;

						var value = string.Concat(chars_table);
						var hash = BitConverter.ToString(hasher.ComputeHash(Encoding.UTF8.GetBytes(value))).Replace("-", "").ToLowerInvariant();

						database.Insert(value, hash);

						if (counter % 10000 == 0)
						{
							Console.WriteLine($"MD5({value}) = {hash}, counter = {counter}");
							database.Tran.Commit();

							if (Console.KeyAvailable)
								parallelLoopState.Stop();

							database.Tran = database.Conn.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
						}

					
						//var bulk = new SqlBulkCopy(conn_str);

						Candidates candidates = new Candidates()
						{
							new Candidate() { Key = "sdsd", Hash = "John" },
							new Candidate() { Key = "ghfgh", Hash = "Joe" }
						};

						//bulk.WriteToServer(//new DbDataReader()
						//    candidates.GetDataReader()
						//    );

						return database;
					},
					(database) =>
					{
						// thread local aggregator
						//Interlocked.Add(ref sum, localSum)

						if (database.Tran != null)
							database.Tran.Commit();

						database.Dispose();
						Console.WriteLine("database.Dispose");
						database = null;
					}
					);

				//Console.WriteLine("\nSum={0}", sum);
			}
			// No exception is expected in this example, but if one is still thrown from a task,
			// it will be wrapped in AggregateException and propagated to the main thread.
			catch (AggregateException e)
			{
				Console.WriteLine("Parallel.ForEach has thrown an exception. THIS WAS NOT EXPECTED.\n{0}", e);
			}*/







			Console.WriteLine($"Done. Elpased time = {stopwatch.Elapsed}");
			Console.ReadKey();
		}
	}
}
