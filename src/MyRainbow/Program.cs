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
			var hasherMD5 = MD5.Create();
			var hasherSHA256 = SHA256.Create();
			var stopwatch = new Stopwatch();
			using (var conn = new SqlConnection(GetConnectionStringFromSecret(args)))
			{
				conn.Open();
				using (var dbase = new DatabaseHasher(conn))
				{
					dbase.EnsureExist();
					//dbase.Purge();
					string last_key_entry = dbase.GetLastKeyEntry();

					stopwatch.Start();
					double nextPause = stopwatch.Elapsed.TotalMilliseconds + 1000;//next check after 1sec
					long counter = 0, last_pause_counter = 0, tps = 0;
					var tran = conn.BeginTransaction(System.Data.IsolationLevel.ReadUncommitted);
					SqlCommand cmd = new SqlCommand("", conn, tran);
					cmd.CommandType = System.Data.CommandType.Text;
					int param_counter = 0;
					foreach (var chars_table in table_of_table_of_chars)
					{
						var key = string.Concat(chars_table);
						if (!string.IsNullOrEmpty(last_key_entry) && last_key_entry.CompareTo(key) >= 0) continue;
						var hashMD5 = BitConverter.ToString(hasherMD5.ComputeHash(Encoding.UTF8.GetBytes(key))).Replace("-", "").ToLowerInvariant();
						var hashSHA256 = BitConverter.ToString(hasherSHA256.ComputeHash(Encoding.UTF8.GetBytes(key))).Replace("-", "").ToLowerInvariant();

						//dbase.Insert(value, hash);
						cmd.CommandText += $"insert into hashes([key], hashMD5, hashSHA256)" +
							$"values(@key{param_counter}, @hashMD5{param_counter}, @hashSHA256{param_counter});{Environment.NewLine}";
						SqlParameter param;
						if (cmd.Parameters.Contains($"@key{param_counter}"))
						{
							param = cmd.Parameters[$"@key{param_counter}"];
							param.Value = key;
						}
						else
						{
							param = new SqlParameter($"@key{param_counter}", System.Data.SqlDbType.VarChar, 20);
							param.Value = key;
							cmd.Parameters.Add(param);
						}

						if (cmd.Parameters.Contains($"@hashMD5{param_counter}"))
						{
							param = cmd.Parameters[$"@hashMD5{param_counter}"];
							param.Value = hashMD5;
						}
						else
						{
							param = new SqlParameter($"@hashMD5{param_counter}", System.Data.SqlDbType.Char, 32);
							param.Value = hashMD5;
							cmd.Parameters.Add(param);
						}

						if (cmd.Parameters.Contains($"@hashSHA256{param_counter}"))
						{
							param = cmd.Parameters[$"@hashSHA256{param_counter}"];
							param.Value = hashSHA256;
						}
						else
						{
							param = new SqlParameter($"@hashSHA256{param_counter}", System.Data.SqlDbType.Char, 64);
							param.Value = hashSHA256;
							cmd.Parameters.Add(param);
						}

						param_counter++;

						if (counter % 200 == 0)
						{
							cmd.Prepare();
							cmd.ExecuteNonQuery();
							cmd.Dispose();
							cmd.CommandText = "";
							cmd.Connection = conn;
							cmd.Transaction = tran;
							param_counter = 0;
						}

						if (counter % 20000 == 0)
						{
							if (cmd != null && !string.IsNullOrEmpty(cmd.CommandText))
							{
								cmd.Prepare();
								cmd.ExecuteNonQuery();
								cmd.Dispose();
								cmd.CommandText = "";
								cmd.Connection = conn;
								cmd.Transaction = tran;
								param_counter = 0;
							}
							tran.Commit(); tran.Dispose();
							tran = null;

							Console.WriteLine($"MD5({key})={hashMD5},SHA256({key})={hashSHA256},counter={counter},tps={tps}");
							if (Console.KeyAvailable)
								break;

							cmd.Transaction = tran = conn.BeginTransaction(System.Data.IsolationLevel.ReadUncommitted);
						}

						if (stopwatch.Elapsed.TotalMilliseconds >= nextPause)
						{
							if (last_pause_counter > 0)
							{
								tps = counter - last_pause_counter;
								nextPause = stopwatch.Elapsed.TotalMilliseconds + 1000;
							}
							last_pause_counter = counter;
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
					{
						tran.Commit(); tran.Dispose();
					}
					stopwatch.Stop();
				}
			}

			Console.WriteLine($"Done. Elpased time = {stopwatch.Elapsed}");
			Console.ReadKey();
		}
	}
}
