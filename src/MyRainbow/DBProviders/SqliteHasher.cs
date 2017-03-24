using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;

namespace MyRainbow.DBProviders
{
	internal class SqliteHasher : DbHasher, IDisposable
	{
		private SqliteConnection Conn { get; set; }
		private SqliteTransaction Tran { get; set; }

		public SqliteHasher(string connectionString)
		{
			Conn = new SqliteConnection(connectionString);
			Conn.Open();
			Tran = null;
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
			string table_name = "hashes";

			string cmd_text = $@"CREATE TABLE IF NOT EXISTS {table_name} (
  [key] varchar(20) NOT NULL PRIMARY KEY,
  hashMD5 char(32) NOT NULL,
  hashSHA256 char(64) NOT NULL
 );
CREATE INDEX IF NOT EXISTS IX_MD5 ON {table_name}(hashMD5);
CREATE INDEX IF NOT EXISTS IX_SHA256 ON {table_name}(hashSHA256);
";

			using (var cmd = new SqliteCommand(cmd_text, Conn, Tran))
			{
				cmd.CommandType = CommandType.Text;
				cmd.ExecuteNonQuery();
			}
		}

		public override void Generate(IEnumerable<IEnumerable<char>> tableOfTableOfChars, MD5 hasherMD5, SHA256 hasherSHA256,
			Func<string, string, string, long, long, bool> shouldBreakFunc, 
			Stopwatch stopwatch = null, int batchInsertCount = 50, int batchTransactionCommitCount = 200)
		{
			string last_key_entry = GetLastKeyEntry();

			double? nextPause = null;
			if (stopwatch != null)
			{
				stopwatch.Start();
				nextPause = stopwatch.Elapsed.TotalMilliseconds + 1000;//next check after 1sec
			}
			long counter = 0, last_pause_counter = 0, tps = 0;
			var tran = Conn.BeginTransaction();
			SqliteCommand cmd = new SqliteCommand("", Conn, tran);
			cmd.CommandType = CommandType.Text;
			int param_counter = 0;
			foreach (var chars_table in tableOfTableOfChars)
			{
				var key = string.Concat(chars_table);
				if (!string.IsNullOrEmpty(last_key_entry) && last_key_entry.CompareTo(key) >= 0) continue;
				var hashMD5 = BitConverter.ToString(hasherMD5.ComputeHash(Encoding.UTF8.GetBytes(key))).Replace("-", "").ToLowerInvariant();
				var hashSHA256 = BitConverter.ToString(hasherSHA256.ComputeHash(Encoding.UTF8.GetBytes(key))).Replace("-", "").ToLowerInvariant();

				//dbase.Insert(value, hash);
				cmd.CommandText += $"insert into hashes([key], hashMD5, hashSHA256)" +
					$"values(@key{param_counter}, @MD5{param_counter}, @SHA256{param_counter});{Environment.NewLine}";
				SqliteParameter param;
				if (cmd.Parameters.Contains($"@key{param_counter}"))
				{
					param = cmd.Parameters[$"@key{param_counter}"];
					param.Value = key;
				}
				else
				{
					param = new SqliteParameter($"@key{param_counter}", SqliteType.Text, 20);
					param.Value = key;
					cmd.Parameters.Add(param);
				}

				if (cmd.Parameters.Contains($"@MD5{param_counter}"))
				{
					param = cmd.Parameters[$"@MD5{param_counter}"];
					param.Value = hashMD5;
				}
				else
				{
					param = new SqliteParameter($"@MD5{param_counter}", SqliteType.Text, 32);
					param.Value = hashMD5;
					cmd.Parameters.Add(param);
				}

				if (cmd.Parameters.Contains($"@SHA256{param_counter}"))
				{
					param = cmd.Parameters[$"@SHA256{param_counter}"];
					param.Value = hashSHA256;
				}
				else
				{
					param = new SqliteParameter($"@SHA256{param_counter}", SqliteType.Text, 64);
					param.Value = hashSHA256;
					cmd.Parameters.Add(param);
				}

				param_counter++;

				if (counter % batchInsertCount == 0)
				{
					//cmd.Prepare();
					cmd.ExecuteNonQuery();
					cmd.Dispose();
					cmd.CommandText = "";
					cmd.Connection = Conn;
					cmd.Transaction = tran;
					param_counter = 0;
				}

				if (counter % batchTransactionCommitCount == 0)
				{
					if (cmd != null && !string.IsNullOrEmpty(cmd.CommandText))
					{
						//cmd.Prepare();
						cmd.ExecuteNonQuery();
						cmd.Dispose();
						cmd.CommandText = "";
						cmd.Connection = Conn;
						cmd.Transaction = tran;
						param_counter = 0;
					}
					tran.Commit(); tran.Dispose();
					tran = null;

					//Console.WriteLine($"MD5({key})={hashMD5},SHA256({key})={hashSHA256},counter={counter},tps={tps}");
					//if (Console.KeyAvailable)
					//	break;
					if (shouldBreakFunc(key, hashMD5, hashSHA256, counter, tps))
						break;

					cmd.Transaction = tran = Conn.BeginTransaction();
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

			if (cmd != null && !string.IsNullOrEmpty(cmd.CommandText))
			{
				//cmd.Prepare();
				cmd.ExecuteNonQuery();
				cmd.Dispose();
			}
			if (tran != null)
			{
				tran.Commit(); tran.Dispose();
			}
		}

		public override string GetLastKeyEntry()
		{
			using (var cmd = new SqliteCommand("SELECT [key] FROM hashes ORDER BY 1 DESC LIMIT 1", Conn, Tran))
			{
				cmd.CommandType = System.Data.CommandType.Text;
				var str = cmd.ExecuteScalar();
				return str == null ? null : (string)str;
			}
		}

		public override void Purge()
		{
			using (var cmd = new SqliteCommand("DELETE FROM hashes", Conn, Tran))
			{
				cmd.CommandType = System.Data.CommandType.Text;
				cmd.ExecuteNonQuery();
			}
		}

		public override void Verify()
		{
			using (var cmd = new SqliteCommand("SELECT * FROM hashes WHERE hashMD5 = @hashMD5", Conn, Tran))
			{
				cmd.CommandType = System.Data.CommandType.Text;

				var param_key = new SqliteParameter("@hashMD5", SqliteType.Text, 32);
				param_key.Value = "b25319faaaea0bf397b2bed872b78c45";
				cmd.Parameters.Add(param_key);
				using (var rdr = cmd.ExecuteReader())
				{
					while (rdr.Read())
					{
						Console.WriteLine("key={0} md5={1} sha256={2}", rdr["key"], rdr["hashMD5"], rdr["hashSHA256"]);
					}
				}
			}
		}
	}
}