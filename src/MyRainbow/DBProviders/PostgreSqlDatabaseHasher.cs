using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.RegularExpressions;

namespace MyRainbow.DBProviders
{
	internal class PostgreSqlDatabaseHasher : DbHasher, IDisposable
	{
		private NpgsqlConnection Conn { get; set; }
		private NpgsqlTransaction Tran { get; set; }

		public PostgreSqlDatabaseHasher(string connectionString)
		{
			Conn = new NpgsqlConnection(connectionString);

			//TODO: depend validation of client CA upon SSl Mode=Require (only that)
			if (IsSSLRequired(connectionString))
				Conn.ProvideClientCertificatesCallback = MyProvideClientCertificatesCallback;

			Conn.Open();
			Tran = null;

			void MyProvideClientCertificatesCallback(X509CertificateCollection clientCerts)
			{
				//TODO: On linux there is no C cert store
				if (Environment.OSVersion.Platform == PlatformID.Win32NT)
				{
					using (X509Store store = new X509Store(StoreLocation.CurrentUser))
					{
						store.Open(OpenFlags.ReadOnly | OpenFlags.OpenExistingOnly);

						var currentCerts = store.Certificates;
						currentCerts = currentCerts.Find(X509FindType.FindByTimeValid, DateTime.Now, false);
						currentCerts = currentCerts.Find(X509FindType.FindByIssuerName, "theBrain.ca", false);
						currentCerts = currentCerts.Find(X509FindType.FindBySubjectName, Environment.MachineName, false);
						if (currentCerts != null && currentCerts.Count > 0)
						{
							var cert = currentCerts[0];
							clientCerts.Add(cert);
						}
					}
				}
				else
				{
					//TODO: figure someting out on Unixes
				}
			}
		}

		/// <summary>
		/// SSL Mode=Require, Disable, or Prefer
		/// </summary>
		/// <param name="connectionString"></param>
		/// <returns></returns>
		private bool IsSSLRequired(string connectionString)
		{
			Dictionary<string, string> dict =
				Regex.Matches(connectionString, @"\s*(?<key>[^;=]+)\s*=\s*((?<value>[^'][^;]*)|'(?<value>[^']*)')")
				.Cast<Match>()
				.ToDictionary(m => m.Groups["key"].Value, m => m.Groups["value"].Value);

			//Console.WriteLine(string.Join(", ", results));
			var result = dict.ContainsKey("SSL Mode") && dict["SSL Mode"] == "Require";
			return result;
		}

		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing)
		{
			if (disposing)
			{
				//free managed resources
				if (Conn != null)
				{
					if (Conn.State != ConnectionState.Closed)
						Conn.Close();
					Conn.Dispose();
				}
			}
			// free native resources if there are any.
		}

		public override void EnsureExist()
		{
			string table_name = "Hashes";

			string cmd_text = $@"CREATE TABLE IF NOT EXISTS ""{table_name}"" (
	""Key"" character varying(20) NOT NULL PRIMARY KEY,
	""hashMD5"" character(32) NOT NULL,
	""hashSHA256"" character(64) NOT NULL
);";

			using (var cmd = new NpgsqlCommand(cmd_text, Conn, Tran))
			{
				cmd.CommandType = CommandType.Text;
				cmd.ExecuteNonQuery();
			}
		}

		public override void Generate(IEnumerable<IEnumerable<char>> tableOfTableOfChars, MD5 hasherMD5, SHA256 hasherSHA256,
			Func<string, string, string, long, long, bool> shouldBreakFunc, Stopwatch stopwatch = null,
			int batchInsertCount = 200, int batchTransactionCommitCount = 20000)
		{
			string last_key_entry = GetLastKeyEntry();

			double? nextPause = null;
			if (stopwatch != null)
			{
				stopwatch.Start();
				nextPause = stopwatch.Elapsed.TotalMilliseconds + 1000;//next check after 1sec
			}
			long counter = 0, last_pause_counter = 0, tps = 0;
			var tran = Conn.BeginTransaction(System.Data.IsolationLevel.ReadUncommitted) as NpgsqlTransaction;
			string insert_into = "INSERT INTO \"Hashes\"(\"Key\", \"hashMD5\", \"hashSHA256\") VALUES";
			NpgsqlCommand cmd = new NpgsqlCommand("", Conn, tran);
			cmd.CommandType = CommandType.Text;
			cmd.CommandText = insert_into;
			string comma = "";
			int param_counter = 0;
			foreach (var chars_table in tableOfTableOfChars)
			{
				var key = string.Concat(chars_table);
				if (!string.IsNullOrEmpty(last_key_entry) && last_key_entry.CompareTo(key) >= 0) continue;
				var hashMD5 = BitConverter.ToString(hasherMD5.ComputeHash(Encoding.UTF8.GetBytes(key))).Replace("-", "").ToLowerInvariant();
				var hashSHA256 = BitConverter.ToString(hasherSHA256.ComputeHash(Encoding.UTF8.GetBytes(key))).Replace("-", "").ToLowerInvariant();

				//dbase.Insert(value, hash);
				cmd.CommandText += $"{comma}(@Key{param_counter}, @hashMD5{param_counter}, @hashSHA256{param_counter}){Environment.NewLine}";
				comma = ",";
				NpgsqlParameter param;
				if (cmd.Parameters.TryGetValue($"@Key{param_counter}", out param))
				{
					//param = cmd.Parameters[$"@Key{param_counter}"] as NpgsqlParameter;
					param.Value = key;
				}
				else
				{
					param = new NpgsqlParameter();
					param.ParameterName = $"@Key{param_counter}";
					param.DbType = DbType.String;
					param.Size = 20;
					param.Value = key;
					cmd.Parameters.Add(param);
				}

				if (cmd.Parameters.TryGetValue($"@hashMD5{param_counter}", out param))
				{
					//param = cmd.Parameters[$"@hashMD5{param_counter}"] as NpgsqlParameter;
					param.Value = hashMD5;
				}
				else
				{
					param = new NpgsqlParameter();
					param.ParameterName = $"@hashMD5{param_counter}";
					param.DbType = DbType.String;
					param.Size = 32;
					param.Value = hashMD5;
					cmd.Parameters.Add(param);
				}

				if (cmd.Parameters.TryGetValue($"@hashSHA256{param_counter}", out param))
				{
					//param = cmd.Parameters[$"@hashSHA256{param_counter}"] as NpgsqlParameter;
					param.Value = hashSHA256;
				}
				else
				{
					param = new NpgsqlParameter();
					param.ParameterName = $"@hashSHA256{param_counter}";
					param.DbType = DbType.String;
					param.Size = 64;
					param.Value = hashSHA256;
					cmd.Parameters.Add(param);
				}


				param_counter++;

				if (counter % batchInsertCount == 0)
				{
					cmd.CommandText += ";";
					cmd.Prepare();
					cmd.ExecuteNonQuery();
					//cmd.Parameters.Clear();
					//cmd.Dispose();
					//cmd = new NpgsqlCommand("", Conn, tran);
					//cmd.CommandType = System.Data.CommandType.Text;
					cmd.CommandText = insert_into;
					cmd.Connection = Conn;
					cmd.Transaction = tran;
					param_counter = 0;
					comma = "";
				}

				if (counter % batchTransactionCommitCount == 0)
				{
					if (cmd != null && !string.IsNullOrEmpty(cmd.CommandText) && cmd.CommandText != insert_into)
					{
						cmd.CommandText += ";";
						cmd.ExecuteNonQuery();
						//cmd.Parameters.Clear();
						//cmd.Dispose();
						//cmd = new NpgsqlCommand("", Conn, tran);
						//cmd.CommandType = System.Data.CommandType.Text;
						cmd.CommandText = insert_into;
						cmd.Connection = Conn;
						cmd.Transaction = tran;
						param_counter = 0;
						comma = "";
					}
					tran.Commit(); tran.Dispose();
					tran = null;

					//Console.WriteLine($"MD5({key})={hashMD5},SHA256({key})={hashSHA256},counter={counter},tps={tps}");
					//if (Console.KeyAvailable)
					//	break;
					if (shouldBreakFunc(key, hashMD5, hashSHA256, counter, tps))
						break;

					cmd.Transaction = tran = Conn.BeginTransaction(IsolationLevel.ReadUncommitted) as NpgsqlTransaction;
				}

				if (stopwatch != null && stopwatch.Elapsed.TotalMilliseconds >= nextPause)
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

			if (cmd != null && !string.IsNullOrEmpty(cmd.CommandText) && cmd.CommandText != insert_into)
			{
				cmd.CommandText += ";";
				cmd.ExecuteNonQuery();
				cmd.Parameters.Clear();
				cmd.Dispose();
			}
			if (tran != null)
			{
				tran.Commit(); tran.Dispose();
			}
		}

		public override string GetLastKeyEntry()
		{
			using (var cmd = new NpgsqlCommand("SELECT \"Key\" FROM \"Hashes\" ORDER BY 1 DESC LIMIT 1", Conn, Tran))
			{
				cmd.CommandType = System.Data.CommandType.Text;
				var str = cmd.ExecuteScalar();
				return str == null ? null : (string)str;
			}
		}

		public override void Purge()
		{
			using (var cmd = new NpgsqlCommand("truncate table \"Hashes\"", Conn, Tran))
			{
				cmd.CommandType = System.Data.CommandType.Text;
				cmd.ExecuteNonQuery();
			}
		}

		public override void Verify()
		{
			using (var cmd = new NpgsqlCommand("SELECT * FROM \"Hashes\" WHERE \"hashMD5\" = @hashMD5", Conn, Tran))
			{
				cmd.CommandType = System.Data.CommandType.Text;

				var param_key = new NpgsqlParameter();
				param_key.ParameterName = "@hashMD5";
				param_key.DbType = DbType.String;
				param_key.Size = 32;
				param_key.Value = "b25319faaaea0bf397b2bed872b78c45";
				cmd.Parameters.Add(param_key);
				using (var rdr = cmd.ExecuteReader())
				{
					while (rdr.Read())
					{
						Console.WriteLine("key={0} md5={1} sha256={2}", rdr["Key"], rdr["hashMD5"], rdr["hashSHA256"]);
					}
				}
			}
		}

		public override void PostGenerateExecute()
		{
			string table_name = "Hashes";

			string database = Conn.ConnectionString.Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries)
				.ToList().FirstOrDefault(x => x.ToLower().StartsWith("database"))
				.Split(new char[] { '=' }, StringSplitOptions.RemoveEmptyEntries)[1].Trim();

			string cmd_text = $@"
CREATE INDEX IF NOT EXISTS IX_MD5 ON ""{table_name}"" (""hashMD5"");
CREATE INDEX IF NOT EXISTS IX_SHA256 ON ""{table_name}"" (""hashSHA256"");
";

			Console.Write("Creating indexes...");
			using (var cmd = new NpgsqlCommand(cmd_text, Conn, Tran))
			{
				cmd.CommandTimeout = 0;
				cmd.CommandType = CommandType.Text;
				cmd.ExecuteNonQuery();
			}
			Console.WriteLine("done");
		}
	}
}
