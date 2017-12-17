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
		}

		public override void Generate(IEnumerable<IEnumerable<char>> tableOfTableOfChars, MD5 hasherMD5, SHA256 hasherSHA256, Func<string, string, string, long, long, bool> shouldBreakFunc, Stopwatch stopwatch = null, int batchInsertCount = 200, int batchTransactionCommitCount = 20000)
		{
			string last_key_entry = GetLastKeyEntry();
		
			double? nextPause = null;
			if (stopwatch != null)
			{
				stopwatch.Start();
				nextPause = stopwatch.Elapsed.TotalMilliseconds + 1000;//next check after 1sec
			}
			long counter = 0, last_pause_counter = 0, tps = 0;

			foreach (var chars_table in tableOfTableOfChars)
			{
				var key = string.Concat(chars_table);
				if (!string.IsNullOrEmpty(last_key_entry) && last_key_entry.CompareTo(key) >= 0) continue;
				var hashMD5 = BitConverter.ToString(hasherMD5.ComputeHash(Encoding.UTF8.GetBytes(key))).Replace("-", "").ToLowerInvariant();
				var hashSHA256 = BitConverter.ToString(hasherSHA256.ComputeHash(Encoding.UTF8.GetBytes(key))).Replace("-", "").ToLowerInvariant();

				var job = _db.CreateItemAsync(new ThinHashes
				{
					Key = key,
					HashMD5 = hashMD5,
					HashSHA256 = hashSHA256
				});
				job.Wait();
				var document = job.Result;

				if (counter % batchTransactionCommitCount == 0)
				{
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
			foreach (ThinHashes rdr in lst)
			{
				Console.WriteLine("key={0} md5={1} sha256={2}", rdr.Key, rdr.HashMD5, rdr.HashSHA256);
			}
		}
	}


	public class ThinHashes
	{
		[JsonProperty(PropertyName = "id")]
		public string Id { get; set; }

		[JsonProperty(PropertyName = "Key")]
		public string Key { get; set; }

		[JsonProperty(PropertyName = "HashMD5")]
		public string HashMD5 { get; set; }

		[JsonProperty(PropertyName = "HashSHA256")]
		public string HashSHA256 { get; set; }
	}

	public class ThinHashesDocumentDBRepository : DocumentDBRepository<ThinHashes>
	{
		private Guid transactionId;

		public ThinHashesDocumentDBRepository(string endpoint, string key, string databaseId, string collectionId)
			: base(endpoint, key, databaseId, collectionId)
		{
			transactionId = Guid.NewGuid();
		}

		public async Task<IEnumerable<ThinHashes>> GetItemsSortedDescByKeyAsync(/*Expression<Func<ThinHashes, bool>> predicate, */int itemsCount = -1)
		{
			IDocumentQuery<ThinHashes> query = client.CreateDocumentQuery<ThinHashes>(
				UriFactory.CreateDocumentCollectionUri(DatabaseId, CollectionId),
				new FeedOptions { MaxItemCount = itemsCount })
				//.Where(predicate)
				.OrderByDescending(o => o.Key)
				.Take(itemsCount)
				.AsDocumentQuery();

			List<ThinHashes> results = new List<ThinHashes>();
			while (query.HasMoreResults)
			{
				results.AddRange(await query.ExecuteNextAsync<ThinHashes>());
			}

			return results;
		}

		public async Task InvokeBulkDeleteSproc()
		{
			var client = new DocumentClient(new Uri(Endpoint), Key);
			Uri collectionLink = UriFactory.CreateDocumentCollectionUri(DatabaseId, CollectionId);

			//string scriptFileName = @"bulkDelete.js";
			//string scriptId = Path.GetFileNameWithoutExtension(scriptFileName);
			string scriptName = "bulkDelete.js";

			//await CreateSprocIfNotExists(scriptFileName, scriptId, scriptName);

			Uri sprocUri = UriFactory.CreateStoredProcedureUri(DatabaseId, CollectionId, scriptName);

			try
			{
				int count = 20;
				int deleted;
				bool continuation;
				do
				{
					var response = await client.ExecuteStoredProcedureAsync<Document>(sprocUri, transactionId);
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

		internal void Cleanup()
		{
			if (client != null)
			{
				client.Dispose();
				client = null;
			}
		}
	}

	public class DocumentDBRepository<T> where T : class
	{
		protected readonly string Endpoint;
		protected readonly string Key;
		protected readonly string DatabaseId;
		protected readonly string CollectionId;
		protected DocumentClient client;

		public DocumentDBRepository(string endpoint, string key, string databaseId, string collectionId)
		{
			Endpoint = endpoint;
			Key = key;
			DatabaseId = databaseId;
			CollectionId = collectionId;
		}

		public async Task<T> GetItemAsync(string id)
		{
			try
			{
				Document document = await client.ReadDocumentAsync(UriFactory.CreateDocumentUri(DatabaseId, CollectionId, id));
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
			IDocumentQuery<T> query = client.CreateDocumentQuery<T>(
				UriFactory.CreateDocumentCollectionUri(DatabaseId, CollectionId),
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
			return await client.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri(DatabaseId, CollectionId), item);
		}

		public async Task<Document> UpdateItemAsync(string id, T item)
		{
			return await client.ReplaceDocumentAsync(UriFactory.CreateDocumentUri(DatabaseId, CollectionId, id), item);
		}

		public async Task DeleteItemAsync(string id)
		{
			await client.DeleteDocumentAsync(UriFactory.CreateDocumentUri(DatabaseId, CollectionId, id));
		}

		public void Initialize()
		{
			client = new DocumentClient(new Uri(Endpoint), Key);
			CreateDatabaseIfNotExistsAsync().Wait();
			CreateCollectionIfNotExistsAsync().Wait();
		}

		private async Task CreateDatabaseIfNotExistsAsync()
		{
			try
			{
				await client.ReadDatabaseAsync(UriFactory.CreateDatabaseUri(DatabaseId));
			}
			catch (DocumentClientException e)
			{
				if (e.StatusCode == System.Net.HttpStatusCode.NotFound)
				{
					await client.CreateDatabaseAsync(new Database { Id = DatabaseId });
				}
				else
				{
					throw;
				}
			}
		}

		private async Task CreateCollectionIfNotExistsAsync()
		{
			try
			{
				await client.ReadDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(DatabaseId, CollectionId));
			}
			catch (DocumentClientException e)
			{
				if (e.StatusCode == System.Net.HttpStatusCode.NotFound)
				{
					await client.CreateDocumentCollectionAsync(
						UriFactory.CreateDatabaseUri(DatabaseId),
						new DocumentCollection { Id = CollectionId },
						new RequestOptions { OfferThroughput = 1000 });
				}
				else
				{
					throw;
				}
			}
		}
	}
}
