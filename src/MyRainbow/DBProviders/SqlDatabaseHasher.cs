using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;

namespace MyRainbow.DBProviders
{
	internal class SqlDatabaseHasher : DbHasher, IDisposable
	{
		//private readonly string _connectionString = "Server=chaos2;MultipleActiveResultSets=True;Initial Catalog=test;User ID=test;Password=XXXXXXXXX;";

		private SqlConnection Conn { get; set; }
		private SqlTransaction Tran { get; set; }

		public SqlDatabaseHasher(string connectionString)
		{
			Conn = new SqlConnection(connectionString);
			Conn.Open();
			Tran = null;
		}

		#region Implementation

		public override void Purge()
		{
			using (var cmd = new SqlCommand("truncate table hashes", Conn, Tran))
			{
				cmd.CommandType = System.Data.CommandType.Text;
				cmd.ExecuteNonQuery();
			}
		}

		public override void EnsureExist()
		{
			string table_name = "hashes";

			string cmd_text = $@"IF(NOT EXISTS(SELECT *
                     FROM INFORMATION_SCHEMA.TABLES
                     WHERE TABLE_SCHEMA = 'dbo'
                     AND  TABLE_NAME = '{table_name}'))
                BEGIN

               CREATE TABLE [dbo].[{table_name}](
					[key] [varchar](20) NOT NULL,
					[hashMD5] [char](32) NOT NULL,
					[hashSHA256] [char](64) NOT NULL,
				 CONSTRAINT [PK_hashes] PRIMARY KEY CLUSTERED 
				(
					[key] ASC
				)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = OFF) ON [PRIMARY],
				 CONSTRAINT [IX_hashMD5] UNIQUE NONCLUSTERED 
				(
					[hashMD5] ASC
				)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = OFF) ON [PRIMARY],
				 CONSTRAINT [IX_hashSHA256] UNIQUE NONCLUSTERED 
				(
					[hashSHA256] ASC
				)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = OFF) ON [PRIMARY]
				) ON [PRIMARY]
               
            END";

			using (var cmd = new SqlCommand(cmd_text, Conn, Tran))
			{
				cmd.CommandType = CommandType.Text;
				cmd.ExecuteNonQuery();
			}
		}

		public override void Generate(IEnumerable<IEnumerable<char>> tableOfTableOfChars, MD5 hasherMD5, SHA256 hasherSHA256,
			Func<string, string, string, long, long, bool> shouldBreakFunc, Stopwatch stopwatch = null,
			int batchInsertCount = 200, int batchTransactionCommitCount = 20_000)
		{
			string last_key_entry = GetLastKeyEntry();

			double? nextPause = null;
			if (stopwatch != null)
			{
				stopwatch.Start();
				nextPause = stopwatch.Elapsed.TotalMilliseconds + 1_000;//next check after 1sec
			}
			long counter = 0, last_pause_counter = 0, tps = 0;
			var tran = Conn.BeginTransaction(System.Data.IsolationLevel.ReadUncommitted);
			SqlCommand cmd = new SqlCommand("", Conn, tran);
			cmd.CommandType = System.Data.CommandType.Text;
			int param_counter = 0;
			foreach (var chars_table in tableOfTableOfChars)
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

				if (counter % batchInsertCount == 0)
				{
					//cmd.Prepare();
					cmd.ExecuteNonQuery();
					cmd.Parameters.Clear();
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
						cmd.Parameters.Clear();
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

					cmd.Transaction = tran = Conn.BeginTransaction(System.Data.IsolationLevel.ReadUncommitted);
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
			using (var cmd = new SqlCommand("SELECT TOP (1) [key] FROM [hashes] ORDER BY 1 desc", Conn, Tran))
			{
				cmd.CommandType = System.Data.CommandType.Text;
				var str = cmd.ExecuteScalar();
				return str == null ? null : (string)str;
			}
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

		public override void Verify()
		{
			using (var cmd = new SqlCommand("SELECT * FROM hashes WHERE hashMD5 = @hashMD5", Conn, Tran))
			{
				cmd.CommandType = System.Data.CommandType.Text;

				var param_key = new SqlParameter("@hashMD5", System.Data.SqlDbType.Char, 32);
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

		public override void PostGenerateExecute()
		{
			string cmd_text = $@"
			IF NOT EXISTS(SELECT * FROM sys.indexes WHERE name = 'IX_MD5' AND object_id = OBJECT_ID('hashes'))
			BEGIN
				CREATE NONCLUSTERED INDEX [IX_MD5] ON [dbo].[hashes]([hashMD5] ASC)
				INCLUDE ([hashSHA256]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
			END
			IF NOT EXISTS(SELECT * FROM sys.indexes WHERE name = 'IX_SHA256' AND object_id = OBJECT_ID('hashes'))
			BEGIN
				CREATE NONCLUSTERED INDEX[IX_SHA256] ON [dbo].[hashes]([hashSHA256] ASC)
				INCLUDE([hashMD5]) WITH(PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON[PRIMARY]
			END";

			Console.Write("Creating indexes...");
			using (var cmd = new SqlCommand(cmd_text, Conn, Tran))
			{
				cmd.CommandTimeout = 0;
				cmd.CommandType = CommandType.Text;
				cmd.ExecuteNonQuery();
			}
			Console.WriteLine("done");
		}

		#endregion Implementation
	}
}