using Cassandra;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MyRainbow.DBProviders
{
	internal class CassandraDBHasher : DbHasher, IDisposable
	{
		private Cluster Cache { get; set; }

		public CassandraDBHasher(string cassandraConnectionString)
		{
			Cluster cluster = Cluster.Builder().AddContactPoint(/*new IPEndPoint(new IPAddress(new byte[] { 192, 168, 0, 2 }), 7000)*/cassandraConnectionString).Build();
			//ISession session = cluster.Connect("demo");
			Cache = cluster;
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
				//free managed resources
				if (Cache != null)
				{
					Cache.Dispose();
				}
			}
			// free native resources if there are any.
		}

		public override void EnsureExist()
		{
			ISession session = Cache.Connect("test");
			//session.CreateKeyspaceIfNotExists("test");
			//session.ChangeKeyspace("test");

			session.Execute("drop table hashes");
			session.Execute(
				@"CREATE TABLE IF NOT EXISTS hashes(
				   id int,
				   key varchar,
				   MD5 varchar,
				   SHA256 varchar,
						PRIMARY KEY(id)
				   )");
			session.Execute("CREATE INDEX ON hashes(key);");

			/*
			 * 
			 CREATE TABLE attr_mapping_class_table(
				partition_key int, 
				clustering_key_0 bigint, 
				clustering_key_1 text,
				clustering_key_2 uuid,
				bool_value_col boolean,
				float_value_col float,
				decimal_value_col decimal,
				PRIMARY KEY (partition_key, clustering_key_0, clustering_key_1, clustering_key_2)
				) WITH CLUSTERING ORDER BY (clustering_key_0 ASC, clustering_key_1 ASC, clustering_key_2 DESC)
			 * 
			 * */

			//session.Execute("insert into hashes(key, MD5, SHA256) values('alamakota', 'bum', 'bam')");
			//Row result = session.Execute("select * from hashes where key='alamakota'").First();
			//Console.WriteLine("{0} {1} {2}", result["key"], result["md5"], result["sha256"]);
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

			var dbase = Cache.Connect("test");
			var profileStmt = dbase.Prepare("INSERT INTO hashes(id, key, MD5, SHA256) VALUES (?, ?, ?, ?)");
			var batch = new BatchStatement();

			//int param_counter = 0;
			using (var canc = new CancellationTokenSource())
			{
				foreach (var chars_table in tableOfTableOfChars)
				{
					var key = string.Concat(chars_table);
					if (!string.IsNullOrEmpty(last_key_entry) && last_key_entry.CompareTo(key) >= 0) continue;
					var hashMD5 = BitConverter.ToString(hasherMD5.ComputeHash(Encoding.UTF8.GetBytes(key))).Replace("-", "").ToLowerInvariant();
					var hashSHA256 = BitConverter.ToString(hasherSHA256.ComputeHash(Encoding.UTF8.GetBytes(key))).Replace("-", "").ToLowerInvariant();

					//work
					batch.Add(profileStmt.Bind(counter, key, hashMD5, hashSHA256));



					//param_counter++;

					if (counter % batchTransactionCommitCount == 0)
					{
						dbase.Execute(batch);
						batch = new BatchStatement();

						if (shouldBreakFunc(key, hashMD5, hashSHA256, counter, tps))
						{
							canc.Cancel();
							break;
						}
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
				}//end foreach

				if (!batch.IsEmpty)
					dbase.Execute(batch);
			}
		}

		public override string GetLastKeyEntry()
		{
			ISession session = Cache.Connect("test");
			Row result = session.Execute("SELECT * FROM hashes ORDER BY id DESC limit 1").First();
			if (result != null)
				return result["key"].ToString();
			return null;
		}

		public override void Purge()
		{
			ISession session = Cache.Connect("test");
			// Delete Bob, then try to read all users and print them to the console
			session.Execute("TRUNCATE TABLE hashes");
		}

		public override void Verify()
		{
			//TODO: implement
		}

		#endregion Implementation
	}
}
