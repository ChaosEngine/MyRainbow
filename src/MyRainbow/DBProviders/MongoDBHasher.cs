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

namespace MyRainbow.DBProviders
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

		public override async Task EnsureExist()
		{
			var database = Cache.GetDatabase("test");
			var collection = database.GetCollection<ThinHashes>("hashes");

			var idxes = await collection.Indexes.ListAsync();
			foreach (var idx in idxes.ToEnumerable())
			{
				Console.WriteLine($"index {idx}");
			}
		}

		public override async Task Generate(IEnumerable<IEnumerable<char>> tableOfTableOfChars, MD5 hasherMD5, SHA256 hasherSHA256,
			Func<string, string, string, long, long, bool> shouldBreakFunc, Stopwatch stopwatch = null,
			int batchInsertCount = 200, int batchTransactionCommitCount = 20000)
		{
			string last_key_entry = await GetLastKeyEntry();

			double? nextPause = null;
			if (stopwatch != null)
			{
				stopwatch.Start();
				nextPause = stopwatch.Elapsed.TotalMilliseconds + 1000;//next check after 1sec
			}
			long counter = 0, last_pause_counter = 0, tps = 0;

			var dbase = Cache.GetDatabase("test");
			var collection = dbase.GetCollection<ThinHashes>("hashes");
			var models = new List<WriteModel<ThinHashes>>(batchTransactionCommitCount);
			//int param_counter = 0;
			using (var canc = new CancellationTokenSource())
			{
				foreach (var chars_table in tableOfTableOfChars)
				{
					var key = string.Concat(chars_table);
					if (!string.IsNullOrEmpty(last_key_entry) && last_key_entry.CompareTo(key) >= 0) continue;
					var (hashMD5, hashSHA256) = CalculateTwoHashes(hasherMD5, hasherSHA256, key);

					//work
					var doc = new ThinHashes
					{
						Key = key,
						HashMD5 = hashMD5,
						HashSHA256 = hashSHA256
					};
					//collection.InsertOne(doc);
					//if (models.ElementAtOrDefault(param_counter) == null)
					models.Add(new InsertOneModel<ThinHashes>(doc));
					//else
					//	models[param_counter] = new InsertOneModel<BsonDocument>(doc);

					//param_counter++;

					if (counter % batchTransactionCommitCount == 0)
					{
						if (models.Count > 0)
						{
							await collection.BulkWriteAsync(models, new BulkWriteOptions { IsOrdered = false }, canc.Token);
							models.Clear();
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
					await collection.BulkWriteAsync(models, new BulkWriteOptions { IsOrdered = false });
			}
		}

		public override async Task Verify()
		{
			var dbase = Cache.GetDatabase("test");
			var collection = dbase.GetCollection<ThinHashes>("hashes");

			//echo -n "gdg" | md5sum - 9409135542c79d1ed50c9fde07fa600a
			//var r_val = dbase.StringGet("MD5_" + "9409135542c79d1ed50c9fde07fa600a");
			//echo -n "ilfad" | md5sum - b25319faaaea0bf397b2bed872b78c45

			var filter = Builders<ThinHashes>.Filter.Eq(p => p.HashMD5, "b25319faaaea0bf397b2bed872b78c45");
			var count_task = collection.CountDocumentsAsync(new BsonDocument());
			var find_task = collection.FindAsync(filter);
			await Task.WhenAll(find_task, count_task).ContinueWith((firstTask) =>
			{
				Console.WriteLine($"count = {count_task.Result}");
				var cursor = find_task.Result;
				foreach (var d in cursor.ToEnumerable())
				{
					Console.WriteLine($"MD5({d.Key})={d.HashMD5},SHA256({d.Key})={d.HashSHA256}");
				}
			});
		}

		public override async Task<string> GetLastKeyEntry()
		{
			var dbase = Cache.GetDatabase("test");
			var collection = dbase.GetCollection<ThinHashes>("hashes");
			var count = await collection.CountDocumentsAsync(new BsonDocument());
			if (count <= 0)
				return null;

			var filter = Builders<ThinHashes>.Filter.Exists(p => p.Key);
			var sort = Builders<ThinHashes>.Sort.Descending(p => p.Key);
			var document = await collection.Find(filter).Sort(sort).FirstAsync();

			var str = document.Key;
			return str;
		}

		public override async Task Purge()
		{
			var dbase = Cache.GetDatabase("test");
			await dbase.GetCollection<BsonDocument>("hashes").DeleteManyAsync(new BsonDocument());
		}

		public override async Task PostGenerateExecute()
		{
			var database = Cache.GetDatabase("test");
			var collection = database.GetCollection<ThinHashes>("hashes");

			//var ind_def = new BsonDocument
			//{
			//	{ "MD5", 1 },
			//};
			//await collection.Indexes.CreateOneAsync(ind_def);
			//ind_def = new BsonDocument
			//{
			//	{ "SHA256", 1 },
			//};
			//await collection.Indexes.CreateOneAsync(ind_def);

			var options = new CreateIndexOptions() { Unique = true };
			
			var md5_ind = collection.Indexes.CreateOneAsync(new CreateIndexModel<ThinHashes>(
				new IndexKeysDefinitionBuilder<ThinHashes>().Ascending(t => t.HashMD5), options));

			var sha256_ind = collection.Indexes.CreateOneAsync(new CreateIndexModel<ThinHashes>(
				new IndexKeysDefinitionBuilder<ThinHashes>().Ascending(t => t.HashSHA256), options));

			await Task.WhenAll(md5_ind, sha256_ind);

			var idxes = await collection.Indexes.ListAsync();
			foreach (var idx in idxes.ToEnumerable())
			{
				Console.WriteLine($"index {idx}");
			}
		}

		#endregion Implementation
	}
}
