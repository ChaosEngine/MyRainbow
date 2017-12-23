using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using System.Linq.Expressions;
using Newtonsoft.Json;
using System.Text;
using System.Collections.ObjectModel;
using System.IO;

namespace MyRainbow.DBProviders
{
	internal class CosmosDBHasher : DbHasher, IDisposable
	{
		private readonly ThinHashesDocumentDBRepository _db;

		public CosmosDBHasher(IConfigurationSection conf)
		{
			if (conf == null)
				throw new NullReferenceException("bad config");

			string endpoint = conf["Endpoint"];
			string key = conf["Key"];
			string databaseId = conf["DatabaseId"];
			string collectionId = conf["CollectionId"];

			_db = new ThinHashesDocumentDBRepository(endpoint, key, databaseId, collectionId);
		}

		#region IDisposable Support
		private bool disposedValue = false; // To detect redundant calls

		protected virtual void Dispose(bool disposing)
		{
			if (!disposedValue)
			{
				if (disposing)
				{
					// TODO: dispose managed state (managed objects).
					if (_db != null)
					{
						_db.Cleanup();
					}
				}

				// TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
				// TODO: set large fields to null.

				disposedValue = true;
			}
		}

		// TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
		// ~CosmosDBHasher() {
		//   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
		//   Dispose(false);
		// }

		// This code added to correctly implement the disposable pattern.
		public void Dispose()
		{
			// Do not change this code. Put cleanup code in Dispose(bool disposing) above.
			Dispose(true);
			// TODO: uncomment the following line if the finalizer is overridden above.
			// GC.SuppressFinalize(this);
		}
		#endregion IDisposable Support

		public override void EnsureExist()
		{
			_db.Initialize();


			string scriptFileName = "DBScripts" + Path.DirectorySeparatorChar + "CosmosDB-bulkDelete.js";
			string scriptName = "bulkDelete.js";
			_db.CreateSprocIfNotExists(scriptFileName, scriptName, scriptName).Wait();

			scriptFileName = "DBScripts" + Path.DirectorySeparatorChar + "CosmosDB-bulkImport.js";
			scriptName = "bulkImport.js";
			_db.CreateSprocIfNotExists(scriptFileName, scriptName, scriptName).Wait();
		}

		public override void Generate(IEnumerable<IEnumerable<char>> tableOfTableOfChars, MD5 hasherMD5, SHA256 hasherSHA256,
			Func<string, string, string, long, long, bool> shouldBreakFunc, Stopwatch stopwatch = null,
			int batchInsertCount = 1_000, int batchTransactionCommitCount = 5_000)
		{
			string last_key_entry = GetLastKeyEntry();

			double? nextPause = null;
			if (stopwatch != null)
			{
				stopwatch.Start();
				nextPause = stopwatch.Elapsed.TotalMilliseconds + 1000;//next check after 1sec
			}
			long counter = 0, last_pause_counter = 0, tps = 0, id = 0;
			var documents = new List<DocumentDBHash>(1000);

			foreach (var chars_table in tableOfTableOfChars)
			{
				id++;
				var key = string.Concat(chars_table);
				if (!string.IsNullOrEmpty(last_key_entry) && last_key_entry.CompareTo(key) >= 0) continue;
				var hashMD5 = BitConverter.ToString(hasherMD5.ComputeHash(Encoding.UTF8.GetBytes(key))).Replace("-", "").ToLowerInvariant();
				var hashSHA256 = BitConverter.ToString(hasherSHA256.ComputeHash(Encoding.UTF8.GetBytes(key))).Replace("-", "").ToLowerInvariant();

				var doc = new DocumentDBHash
				{
					Id = id.ToString(),
					Key = key,
					HashMD5 = hashMD5,
					HashSHA256 = hashSHA256
				};
				//var job = _db.CreateItemAsync(doc);
				//job.Wait();
				//var ret_document = job.Result;
				documents.Add(doc);

				if (counter % batchTransactionCommitCount == 0)
				{
					var job = _db.InvokeBulkInsertSproc(documents);
					job.Wait();
					documents.Clear();

					if (shouldBreakFunc(key, hashMD5, hashSHA256, counter, tps))
						break;
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

			if (documents.Count > 0)
			{
				var job = _db.InvokeBulkInsertSproc(documents);
				job.Wait();
				documents.Clear();
			}
		}

		public override string GetLastKeyEntry()
		{
			var job = GetLastKeyEntryAsync();
			job.Wait();
			return job.Result;
		}

		public async Task<string> GetLastKeyEntryAsync()
		{
			var last = await _db.GetItemsSortedDescByKeyAsync(1);
			var el = last.FirstOrDefault();
			if (el == null)
				return null;
			else
				return el.Key;
		}

		public override void Purge()
		{
			var job = _db.InvokeBulkDeleteSproc();
			job.Wait();
		}

		public override void Verify()
		{
			var job = _db.GetItemsAsync(d => d.HashMD5 == "b25319faaaea0bf397b2bed872b78c45");
			job.Wait();
			var lst = job.Result;
			foreach (DocumentDBHash rdr in lst)
			{
				Console.WriteLine("key={0} md5={1} sha256={2}", rdr.Key, rdr.HashMD5, rdr.HashSHA256);
			}
		}
	}

	public class DocumentDBHash : ThinHashes
	{
		[JsonProperty(PropertyName = "id")]
		public string Id { get; set; }
	}

	public class ThinHashes
	{
		//[JsonProperty(PropertyName = "Key")]
		public string Key { get; set; }

		//[JsonProperty(PropertyName = "HashMD5")]
		public string HashMD5 { get; set; }

		//[JsonProperty(PropertyName = "HashSHA256")]
		public string HashSHA256 { get; set; }
	}

	class ThinHashesDocumentDBRepository : DocumentDBRepository<DocumentDBHash>
	{
		private Guid transactionId;

		public ThinHashesDocumentDBRepository(string endpoint, string key, string databaseId, string collectionId)
			: base(endpoint, key, databaseId, collectionId)
		{
			transactionId = Guid.NewGuid();
		}

		public override void Initialize()
		{
			_client = new DocumentClient(new Uri(_endpoint), _key);
			CreateDatabaseIfNotExistsAsync().Wait();
			CreateCollectionIfNotExistsAsync(new DocumentCollection
			{
				Id = _collectionId,
				//myCollection.PartitionKey.Paths.Add("/Key"),
				UniqueKeyPolicy = new UniqueKeyPolicy
				{
					UniqueKeys = new Collection<UniqueKey>
					{
						new UniqueKey { Paths = new Collection<string> { "/Key", "/HashMD5", "/HashSHA256" }},
					}
				},
				IndexingPolicy = new IndexingPolicy(new RangeIndex(DataType.String) { Precision = -1 }),
			}).Wait();
		}

		public async Task<IEnumerable<DocumentDBHash>> GetItemsSortedDescByKeyAsync(int itemsCount = -1)
		{
			IDocumentQuery<DocumentDBHash> query = _client.CreateDocumentQuery<DocumentDBHash>(
				UriFactory.CreateDocumentCollectionUri(_databaseId, _collectionId),
				new FeedOptions { MaxItemCount = itemsCount/*, EnableCrossPartitionQuery = true*/ })
				//.Where(predicate)
				.OrderByDescending(o => o.Key)
				.Take(itemsCount)
				.AsDocumentQuery();

			List<DocumentDBHash> results = new List<DocumentDBHash>();
			while (query.HasMoreResults)
			{
				results.AddRange(await query.ExecuteNextAsync<DocumentDBHash>());
			}

			return results;
		}

		internal async Task InvokeBulkDeleteSproc()
		{
			var client = new DocumentClient(new Uri(_endpoint), _key);
			Uri collectionLink = UriFactory.CreateDocumentCollectionUri(_databaseId, _collectionId);

			string scriptName = "bulkDelete.js";

			Uri sprocUri = UriFactory.CreateStoredProcedureUri(_databaseId, _collectionId, scriptName);

			try
			{
				int count = 20;
				int deleted;
				bool continuation;
				do
				{
					var response = await client.ExecuteStoredProcedureAsync<Document>(sprocUri,
						//new RequestOptions { PartitionKey = new PartitionKey("mmmmm") },
						transactionId);
					continuation = response.Response.GetPropertyValue<bool>("continuation");
					deleted = response.Response.GetPropertyValue<int>("deleted");
				}
				while (continuation && count-- > 0);
			}
			catch (DocumentClientException ex)
			{
				throw;
			}
		}

		internal async Task<int> InvokeBulkInsertSproc(List<DocumentDBHash> documents)
		{
			int maxFiles = 2000, maxScriptSize = 50000;
			int currentCount = 0;
			int fileCount = maxFiles != 0 ? Math.Min(maxFiles, documents.Count) : documents.Count;


			Uri sproc = UriFactory.CreateStoredProcedureUri(_databaseId, _collectionId, "bulkImport.js");


			// 4. Create a batch of docs (MAX is limited by request size (2M) and to script for execution.           
			// We send batches of documents to create to script.
			// Each batch size is determined by MaxScriptSize.
			// MaxScriptSize should be so that:
			// -- it fits into one request (MAX reqest size is 16Kb).
			// -- it doesn't cause the script to time out.
			// -- it is possible to experiment with MaxScriptSize to get best perf given number of throttles, etc.
			while (currentCount < fileCount)
			{
				// 5. Create args for current batch.
				//    Note that we could send a string with serialized JSON and JSON.parse it on the script side,
				//    but that would cause script to run longer. Since script has timeout, unload the script as much
				//    as we can and do the parsing by client and framework. The script will get JavaScript objects.
				string argsJson = CreateBulkInsertScriptArguments(documents, currentCount, fileCount, maxScriptSize);

				var args = new dynamic[] { JsonConvert.DeserializeObject<dynamic>(argsJson) };

				// 6. execute the batch.
				StoredProcedureResponse<int> scriptResult = await _client.ExecuteStoredProcedureAsync<int>(
					sproc,
					//new RequestOptions { PartitionKey = new PartitionKey("mmmmm") },
					args);

				// 7. Prepare for next batch.
				int currentlyInserted = scriptResult.Response;
				currentCount += currentlyInserted;
			}

			return currentCount;
		}

		private static string CreateBulkInsertScriptArguments(List<DocumentDBHash> docs, int currentIndex, int maxCount, int maxScriptSize)
		{
			var jsonDocumentArray = new StringBuilder(1000);

			if (currentIndex >= maxCount) return string.Empty;

			string serialized = JsonConvert.SerializeObject(docs[currentIndex]);
			jsonDocumentArray.Append("[").Append(serialized);

			int scriptCapacityRemaining = maxScriptSize;

			int i = 1;
			while (jsonDocumentArray.Length < scriptCapacityRemaining && (currentIndex + i) < maxCount)
			{
				jsonDocumentArray.Append(", ").Append(JsonConvert.SerializeObject(docs[currentIndex + i]));
				i++;
			}

			jsonDocumentArray.Append("]");
			return jsonDocumentArray.ToString();
		}
	}

	abstract class DocumentDBRepository<T> where T : class
	{
		protected readonly string _endpoint, _key, _databaseId, _collectionId;
		protected DocumentClient _client;

		public DocumentDBRepository(string endpoint, string key, string databaseId, string collectionId)
		{
			_endpoint = endpoint;
			_key = key;
			_databaseId = databaseId;
			_collectionId = collectionId;
		}

		public async Task<T> GetByIDAsync(string id)
		{
			try
			{
				Document document = await _client.ReadDocumentAsync(UriFactory.CreateDocumentUri(_databaseId, _collectionId, id));
				return (T)(dynamic)document;
			}
			catch (DocumentClientException e)
			{
				if (e.StatusCode == System.Net.HttpStatusCode.NotFound)
				{
					return null;
				}
				else
				{
					throw;
				}
			}
		}

		public async Task<IEnumerable<T>> GetItemsAsync(Expression<Func<T, bool>> predicate, int itemsCount = -1)
		{
			IDocumentQuery<T> query = _client.CreateDocumentQuery<T>(
				UriFactory.CreateDocumentCollectionUri(_databaseId, _collectionId),
				new FeedOptions { MaxItemCount = itemsCount })
				.Where(predicate)
				.AsDocumentQuery();

			List<T> results = new List<T>();
			while (query.HasMoreResults)
			{
				results.AddRange(await query.ExecuteNextAsync<T>());
			}

			return results;
		}

		public async Task<Document> CreateItemAsync(T item)
		{
			return await _client.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri(_databaseId, _collectionId), item,
				disableAutomaticIdGeneration: true);
		}

		public async Task<Document> UpdateItemAsync(string id, T item)
		{
			return await _client.ReplaceDocumentAsync(UriFactory.CreateDocumentUri(_databaseId, _collectionId, id), item);
		}

		public async Task DeleteItemAsync(string id)
		{
			await _client.DeleteDocumentAsync(UriFactory.CreateDocumentUri(_databaseId, _collectionId, id));
		}

		public abstract void Initialize();

		public void Cleanup()
		{
			if (_client != null)
			{
				_client.Dispose();
				_client = null;
			}
		}

		protected async Task CreateDatabaseIfNotExistsAsync()
		{
			try
			{
				await _client.ReadDatabaseAsync(UriFactory.CreateDatabaseUri(_databaseId));
			}
			catch (DocumentClientException e)
			{
				if (e.StatusCode == System.Net.HttpStatusCode.NotFound)
				{
					await _client.CreateDatabaseAsync(new Database { Id = _databaseId });
				}
				else
				{
					throw;
				}
			}
		}

		protected async Task CreateCollectionIfNotExistsAsync(DocumentCollection myNewCollection)
		{
			try
			{
				await _client.ReadDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(_databaseId, _collectionId));
			}
			catch (DocumentClientException e)
			{
				if (e.StatusCode == System.Net.HttpStatusCode.NotFound)
				{
					await _client.CreateDocumentCollectionAsync(
						UriFactory.CreateDatabaseUri(_databaseId),
						myNewCollection,
						new RequestOptions { OfferThroughput = 2500 });
				}
				else
				{
					throw;
				}
			}
		}

		internal async Task CreateSprocIfNotExists(string scriptFileName, string scriptId, string scriptName)
		{
			var client = new DocumentClient(new Uri(_endpoint), _key);
			Uri collectionLink = UriFactory.CreateDocumentCollectionUri(_databaseId, _collectionId);

			var sproc = new StoredProcedure
			{
				Id = scriptId,
				Body = File.ReadAllText(scriptFileName)
			};

			bool needToCreate = false;
			Uri sprocUri = UriFactory.CreateStoredProcedureUri(_databaseId, _collectionId, scriptName);

			try
			{
				await client.ReadStoredProcedureAsync(sprocUri);
			}
			catch (DocumentClientException de)
			{
				if (de.StatusCode != System.Net.HttpStatusCode.NotFound)
				{
					throw;
				}
				else
				{
					needToCreate = true;
				}
			}

			if (needToCreate)
			{
				await client.CreateStoredProcedureAsync(collectionLink, sproc);
			}
		}
	}
}
