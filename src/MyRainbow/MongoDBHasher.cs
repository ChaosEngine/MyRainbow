using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MyRainbow
{
	internal class MongoDBHasher : DbHasher, IDisposable
	{
		private MongoClient Cache { get; set; }

		public MongoDBHasher(string mongoConnectionString)
		{
			//"mongodb://test:PASSPASSPASS@192.168.0.2:27017/test"
			var client = new MongoClient(mongoConnectionString);
			Cache = client;
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
				////free managed resources
				//if (Conn != null)
				//{
				//	if (Conn.State != ConnectionState.Closed)
				//		Conn.Close();
				//	Conn.Dispose();
				//}
			}
			// free native resources if there are any.
		}

		public override void EnsureExist()
		{
			var database = Cache.GetDatabase("test");
			var collection = database.GetCollection<BsonDocument>("hashes");

			/*var ind_def = new BsonDocument
			{
				{ "MD5", 1 },
			};
			collection.Indexes.CreateOne(ind_def);
			ind_def = new BsonDocument
			{
				{ "SHA256", 1 },
			};
			collection.Indexes.CreateOne(ind_def);*/

			var idxes = collection.Indexes.ListAsync();
			foreach (var idx in idxes.Result.ToEnumerable())
			{
				Console.WriteLine($"index {idx}");
			}
		}

		public override void Generate(IEnumerable<IEnumerable<char>> tableOfTableOfChars, MD5 hasherMD5, SHA256 hasherSHA256,
			Func<string, string, string, long, long, bool> shouldBreakFunc, Stopwatch stopwatch = null,
			int batchInsertCount = 200, int batchTransactionCommitCount = 20000)
		{
			string last_key_entry = GetLastKeyEntryAsync().Result;

			double? nextPause = null;
			if (stopwatch != null)
			{
				stopwatch.Start();
				nextPause = stopwatch.Elapsed.TotalMilliseconds + 1000;//next check after 1sec
			}
			long counter = 0, last_pause_counter = 0, tps = 0;

			var dbase = Cache.GetDatabase("test");
			var collection = dbase.GetCollection<BsonDocument>("hashes");
			var models = new List<WriteModel<BsonDocument>>(batchTransactionCommitCount);
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
					var doc = new BsonDocument
				{
					{ "key", key },
					{ "MD5", hashMD5 },
					{ "SHA256", hashSHA256 }
				};
					//collection.InsertOne(doc);
					//if (models.ElementAtOrDefault(param_counter) == null)
					models.Add(new InsertOneModel<BsonDocument>(doc));
					//else
					//	models[param_counter] = new InsertOneModel<BsonDocument>(doc);

					//param_counter++;

					if (counter % batchTransactionCommitCount == 0)
					{
						if (models.Count > 0)
						{
							collection.BulkWriteAsync(models, new BulkWriteOptions { IsOrdered = false }, canc.Token);
							models = new List<WriteModel<BsonDocument>>(batchTransactionCommitCount);
							//param_counter = 0;
						}

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

				if (models.Count > 0)
					collection.BulkWriteAsync(models, new BulkWriteOptions { IsOrdered = false });
			}
		}

		public override void Verify()
		{
			var dbase = Cache.GetDatabase("test");
			var collection = dbase.GetCollection<BsonDocument>("hashes");

			//echo -n "gdg" | md5sum - 9409135542c79d1ed50c9fde07fa600a
			//var r_val = dbase.StringGet("MD5_" + "9409135542c79d1ed50c9fde07fa600a");
			//echo -n "ilfad" | md5sum - b25319faaaea0bf397b2bed872b78c45

			var filter = Builders<BsonDocument>.Filter.Eq("MD5", "b25319faaaea0bf397b2bed872b78c45");
			var task = collection.FindAsync(filter);//.ToCursor();
			task.ContinueWith(async (firstTask) =>
			{
				var count = await collection.CountAsync(new BsonDocument());

				Console.WriteLine($"count = {count}");
				var cursor = firstTask.Result;
				foreach (var document in cursor.ToEnumerable())
				{
					Console.WriteLine(document);
				}
			});
		}

		private async Task<string> GetLastKeyEntryAsync()
		{
			var dbase = Cache.GetDatabase("test");
			var collection = dbase.GetCollection<BsonDocument>("hashes");
			var count = await collection.CountAsync(new BsonDocument());
			if (count <= 0)
				return null;

			var filter = Builders<BsonDocument>.Filter.Exists("key");
			var sort = Builders<BsonDocument>.Sort.Descending("key");
			var document = await collection.Find(filter).Sort(sort).FirstAsync();

			var str = document.GetValue("key").AsString;
			return str;
		}

		public override string GetLastKeyEntry()
		{
			return GetLastKeyEntryAsync().Result;
		}

		public override void Purge()
		{
			var dbase = Cache.GetDatabase("test");
			dbase.GetCollection<BsonDocument>("hashes").DeleteManyAsync(new BsonDocument()).Wait();
		}

		#endregion Implementation
	}
}
