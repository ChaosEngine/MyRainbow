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
					var tran = conn.BeginTransaction(System.Data.IsolationLevel.ReadUncommitted);
					SqlCommand cmd = new SqlCommand("", conn, tran);
					cmd.CommandType = System.Data.CommandType.Text;
					int param_counter = 0;
					foreach (var chars_table in table_of_table_of_chars)
					{
						var key = string.Concat(chars_table);
						//if ("tdtzz".CompareTo(value) >= 0) continue;
						var hash = BitConverter.ToString(hasher.ComputeHash(Encoding.UTF8.GetBytes(key))).Replace("-", "").ToLowerInvariant();

						////dbase.Insert(value, hash);
						cmd.CommandText += $"insert into hashes_md5([key], hash) values(@key{param_counter}, @hash{param_counter});{Environment.NewLine}";
						var param_key = new SqlParameter("@key" + param_counter, System.Data.SqlDbType.VarChar, 20);
						param_key.Value = key;
						cmd.Parameters.Add(param_key);
						var hash_key = new SqlParameter("@hash" + param_counter, System.Data.SqlDbType.VarChar, 32);
						hash_key.Value = hash;
						cmd.Parameters.Add(hash_key);
						param_counter++;

						if (counter % 200 == 0)
						{
							cmd.Prepare();
							cmd.ExecuteNonQuery();
							cmd.Dispose();
							cmd = new SqlCommand("", conn, tran);
							cmd.CommandType = System.Data.CommandType.Text;
							param_counter = 0;
						}

						if (counter % 20000 == 0)
						{
							if (cmd != null && !string.IsNullOrEmpty(cmd.CommandText))
							{
								cmd.Prepare();
								cmd.ExecuteNonQuery();
								cmd.Dispose();
								cmd = new SqlCommand("", conn, tran);
								cmd.CommandType = System.Data.CommandType.Text;
								param_counter = 0;
							}
							tran.Commit();
							tran = null;

							Console.WriteLine($"MD5({key}) = {hash}, counter = {counter}");
							if (Console.KeyAvailable)
								break;

							cmd.Transaction = tran = conn.BeginTransaction(System.Data.IsolationLevel.ReadUncommitted);
						}
						counter++;
					}

					if (cmd != null && !string.IsNullOrEmpty(cmd.CommandText))
					{
						cmd.Prepare();
						cmd.ExecuteNonQuery();
						cmd.Dispose();
					}
					if (tran != null)
						tran.Commit();

					stopwatch.Stop();
				}
			}

			Console.WriteLine($"Done. Elpased time = {stopwatch.Elapsed}");
			Console.ReadKey();
		}
	}
}
