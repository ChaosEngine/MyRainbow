using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;

namespace MyRainbow
{
	internal class DatabaseHasher : IDisposable
	{
		//private readonly string _connectionString = "Server=chaos2;MultipleActiveResultSets=True;Initial Catalog=test;User ID=test;Password=XXXXXXXXX;";

		private SqlConnection Conn { get; set; }
		private SqlTransaction Tran { get; set; }

		public DatabaseHasher(SqlConnection conn, SqlTransaction tran = null)
		{
			Conn = conn;
			Tran = tran;
		}

		internal void Purge()
		{
			using (var cmd = new SqlCommand("truncate table hashes", Conn, Tran))
			{
				cmd.CommandType = System.Data.CommandType.Text;
				cmd.ExecuteNonQuery();
			}
		}

		internal void EnsureExist()
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

		internal void Generate(IEnumerable<IEnumerable<char>> tableOfTableOfChars, MD5 hasherMD5, SHA256 hasherSHA256,
			Func<string, string, string, long, long, bool> shouldBreakFun, Stopwatch stopwatch = null, int batchInsertCount = 200, int batchTransactionCommitCount = 20000)
		{
			string last_key_entry = GetLastKeyEntry();

			double? nextPause = null;
			if (stopwatch != null)
			{
				stopwatch.Start();
				nextPause = stopwatch.Elapsed.TotalMilliseconds + 1000;//next check after 1sec
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
					if (shouldBreakFun(key, hashMD5, hashSHA256, counter, tps))
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
				cmd.Dispose();
			}
			if (tran != null)
			{
				tran.Commit(); tran.Dispose();
			}
		}

		internal string GetLastKeyEntry()
		{
			using (var cmd = new SqlCommand("SELECT TOP (1)[key] FROM[test].[dbo].[hashes] ORDER BY 1 desc", Conn, Tran))
			{
				cmd.CommandType = System.Data.CommandType.Text;
				var str = cmd.ExecuteScalar();
				return str == null ? null : (string)str;
			}
		}

		internal void Insert(string key, string hash)
		{
			using (var cmd = new SqlCommand("insert into hashes_md5([key], hash) values(@key, @hash);", Conn, Tran))
			{
				cmd.CommandType = System.Data.CommandType.Text;

				var param_key = new SqlParameter("@key", System.Data.SqlDbType.VarChar, 200, key);
				param_key.Value = key;
				cmd.Parameters.Add(param_key);
				var hash_key = new SqlParameter("@hash", System.Data.SqlDbType.VarChar, 200, key);
				hash_key.Value = hash;
				cmd.Parameters.Add(hash_key);

				cmd.Prepare();
				cmd.ExecuteNonQuery();
			}
		}

		#region Implementation
		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing)
		{
			if (disposing)
			{
				// free managed resources
				//if (Conn != null)
				//{
				//	if (Conn.State != ConnectionState.Closed)
				//		Conn.Close();
				//	Conn.Dispose();
				//}
			}
			// free native resources if there are any.
		}
		#endregion Implementation
	}
}