using MySqlConnector;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace MyRainbow.DBProviders
{
	internal class MySqlDatabaseHasher : DbHasher, IDisposable
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

		public override void EnsureExist()
		{
			string table_name = "Hashes";

			string cmd_text = $@"CREATE TABLE IF NOT EXISTS `{table_name}` (
  `Key` varchar(20) NOT NULL,
  `hashMD5` char(32) NOT NULL,
  `hashSHA256` char(64) NOT NULL,
  PRIMARY KEY (`Key`),
  UNIQUE KEY `IX_MD5` (`hashMD5`) USING BTREE,
  UNIQUE KEY `IX_SHA256` (`hashSHA256`) USING BTREE
 ) ENGINE=MyISAM DEFAULT CHARSET=utf8 ROW_FORMAT=FIXED";

			using (var cmd = new MySqlCommand(cmd_text, Conn, Tran))
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
			var tran = Conn.BeginTransaction(System.Data.IsolationLevel.ReadUncommitted) as MySqlTransaction;
			string insert_into = "INSERT INTO Hashes(`Key`, hashMD5, hashSHA256) VALUES";
			MySqlCommand cmd = new MySqlCommand("", Conn, tran);
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
				MySqlParameter param;
				if (cmd.Parameters.Contains($"@Key{param_counter}"))
				{
					param = cmd.Parameters[$"@Key{param_counter}"] as MySqlParameter;
					param.Value = key;
				}
				else
				{
					param = new MySqlParameter();
					param.ParameterName = $"@Key{param_counter}";
					param.DbType = DbType.String;
					param.Size = 20;
					param.Value = key;
					cmd.Parameters.Add(param);
				}

				if (cmd.Parameters.Contains($"@hashMD5{param_counter}"))
				{
					param = cmd.Parameters[$"@hashMD5{param_counter}"] as MySqlParameter;
					param.Value = hashMD5;
				}
				else
				{
					param = new MySqlParameter();
					param.ParameterName = $"@hashMD5{param_counter}";
					param.DbType = DbType.String;
					param.Size = 32;
					param.Value = hashMD5;
					cmd.Parameters.Add(param);
				}

				if (cmd.Parameters.Contains($"@hashSHA256{param_counter}"))
				{
					param = cmd.Parameters[$"@hashSHA256{param_counter}"] as MySqlParameter;
					param.Value = hashSHA256;
				}
				else
				{
					param = new MySqlParameter();
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
					cmd.ExecuteNonQuery();
					cmd.Parameters.Clear();
					cmd.Dispose();
					cmd = new MySqlCommand("", Conn, tran);
					cmd.CommandType = System.Data.CommandType.Text;
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
						cmd.Parameters.Clear();
						cmd.Dispose();
						cmd = new MySqlCommand("", Conn, tran);
						cmd.CommandType = System.Data.CommandType.Text;
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

					cmd.Transaction = tran = Conn.BeginTransaction(IsolationLevel.ReadUncommitted) as MySqlTransaction;
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
			using (var cmd = new MySqlCommand("SELECT `Key` FROM Hashes ORDER BY 1 DESC LIMIT 1", Conn, Tran))
			{
				cmd.CommandType = System.Data.CommandType.Text;
				var str = cmd.ExecuteScalar();
				return str == null ? null : (string)str;
			}
		}

		public override void Purge()
		{
			using (var cmd = new MySqlCommand("truncate table Hashes", Conn, Tran))
			{
				cmd.CommandType = System.Data.CommandType.Text;
				cmd.ExecuteNonQuery();
			}
		}

		public override void Verify()
		{
			using (var cmd = new MySqlCommand("SELECT * FROM Hashes WHERE hashMD5 = @hashMD5", Conn, Tran))
			{
				cmd.CommandType = System.Data.CommandType.Text;

				var param_key = new MySqlParameter();
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
select if (
    exists(
        select distinct index_name from information_schema.statistics where table_schema = '{database}' and table_name = '{table_name}' and index_name like 'IX_MD5'
)
    ,'select ''index IX_MD5 exists'' _______;'
    ,'create index IX_MD5 on {table_name}(`hashMD5`)') into @txt;
PREPARE stmt1 FROM @txt;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

select if (
    exists(
        select distinct index_name from information_schema.statistics where table_schema = '{database}' and table_name = '{table_name}' and index_name like 'IX_SHA256'
)
    ,'select ''index IX_SHA256 exists'' _______;'
    ,'create index IX_SHA256 on {table_name}(`hashSHA256`)') into @txt;
PREPARE stmt1 FROM @txt;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;";

			Console.Write("Creating indexes...");
			using (var cmd = new MySqlCommand(cmd_text, Conn, Tran))
			{
				cmd.CommandTimeout = 0;
				cmd.CommandType = CommandType.Text;
				cmd.ExecuteNonQuery();
			}
			Console.WriteLine("done");
		}
	}
}
