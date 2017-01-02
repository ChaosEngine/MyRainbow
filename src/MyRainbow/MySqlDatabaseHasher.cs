﻿using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;

namespace MyRainbow
{
	internal class MySqlDatabaseHasher : IDbHasher, IDisposable
	{
		private MySqlConnection Conn { get; set; }
		private MySqlTransaction Tran { get; set; }

		public MySqlDatabaseHasher(string connectionString)
		{
			Conn = new MySqlConnection(connectionString);
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

		public void EnsureExist()
		{
			string table_name = "Hashes";

			string cmd_text = $@"CREATE TABLE IF NOT EXISTS `{table_name}` (
  `SourceKey` varchar(20) NOT NULL,
  `hashMD5` char(32) NOT NULL,
  `hashSHA256` char(64) NOT NULL,
  PRIMARY KEY (`SourceKey`),
  UNIQUE KEY `IX_MD5` (`hashMD5`) USING BTREE,
  UNIQUE KEY `IX_SHA256` (`hashSHA256`) USING BTREE
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8";

			using (var cmd = new MySqlCommand(cmd_text, Conn, Tran))
			{
				cmd.CommandType = CommandType.Text;
				cmd.ExecuteNonQuery();
			}
		}

		public void Generate(IEnumerable<IEnumerable<char>> tableOfTableOfChars, MD5 hasherMD5, SHA256 hasherSHA256,
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
			var tran = Conn.BeginTransaction(System.Data.IsolationLevel.ReadUncommitted);
			MySqlCommand cmd = new MySqlCommand("", Conn, tran);
			cmd.CommandType = System.Data.CommandType.Text;
			int param_counter = 0;
			foreach (var chars_table in tableOfTableOfChars)
			{
				var key = string.Concat(chars_table);
				if (!string.IsNullOrEmpty(last_key_entry) && last_key_entry.CompareTo(key) >= 0) continue;
				var hashMD5 = BitConverter.ToString(hasherMD5.ComputeHash(Encoding.UTF8.GetBytes(key))).Replace("-", "").ToLowerInvariant();
				var hashSHA256 = BitConverter.ToString(hasherSHA256.ComputeHash(Encoding.UTF8.GetBytes(key))).Replace("-", "").ToLowerInvariant();

				//dbase.Insert(value, hash);
				cmd.CommandText += $"insert into Hashes(SourceKey, hashMD5, hashSHA256)" +
					$"values(@sourceKey{param_counter}, @hashMD5{param_counter}, @hashSHA256{param_counter});{Environment.NewLine}";
				MySqlParameter param;
				if (cmd.Parameters.Contains($"@sourceKey{param_counter}"))
				{
					param = cmd.Parameters[$"@sourceKey{param_counter}"];
					param.Value = key;
				}
				else
				{
					param = new MySqlParameter($"@sourceKey{param_counter}", MySqlDbType.VarChar, 20);
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
					param = new MySqlParameter($"@hashMD5{param_counter}", MySqlDbType.String, 32);
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
					param = new MySqlParameter($"@hashSHA256{param_counter}", MySqlDbType.String, 64);
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

					cmd.Transaction = tran = Conn.BeginTransaction(IsolationLevel.ReadUncommitted);
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

		public string GetLastKeyEntry()
		{
			using (var cmd = new MySqlCommand("SELECT SourceKey FROM Hashes ORDER BY 1 DESC LIMIT 1", Conn, Tran))
			{
				cmd.CommandType = System.Data.CommandType.Text;
				var str = cmd.ExecuteScalar();
				return str == null ? null : (string)str;
			}
		}

		public void Purge()
		{
			using (var cmd = new MySqlCommand("truncate table Hashes", Conn, Tran))
			{
				cmd.CommandType = System.Data.CommandType.Text;
				cmd.ExecuteNonQuery();
			}
		}

		public void Verify()
		{
			using (var cmd = new MySqlCommand("SELECT * FROM Hashes WHERE hashMD5 = @hashMD5", Conn, Tran))
			{
				cmd.CommandType = System.Data.CommandType.Text;

				var param_key = new MySqlParameter("@hashMD5", MySqlDbType.String, 32);
				param_key.Value = "b25319faaaea0bf397b2bed872b78c45";
				cmd.Parameters.Add(param_key);
				using (var rdr = cmd.ExecuteReader())
				{
					while (rdr.Read())
					{
						Console.WriteLine("key={0} md5={1} sha256={2}", rdr["SourceKey"], rdr["hashMD5"], rdr["hashSHA256"]);
					}
				}
			}
		}
	}
}