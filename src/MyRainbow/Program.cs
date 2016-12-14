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

					dbase.Generate(table_of_table_of_chars, hasherMD5, hasherSHA256,
						(key, hashMD5, hashSHA256, counter, tps) =>
						{
							Console.WriteLine($"MD5({key})={hashMD5},SHA256({key})={hashSHA256},counter={counter},tps={tps}");
							if (Console.KeyAvailable)
								return true;
							return false;
						}, stopwatch);
					
					stopwatch.Stop();
				}
			}

			Console.WriteLine($"Done. Elpased time = {stopwatch.Elapsed}");
			Console.ReadKey();
		}
	}
}
