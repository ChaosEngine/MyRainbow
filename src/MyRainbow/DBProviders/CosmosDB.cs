#if ENABLE_COSMOS_DB

using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace MyRainbow.DBProviders
{
    internal class CosmosDBHasher : DbHasher, IDisposable
    {
        private readonly ThinHashesCosmosRepository _db;

        public CosmosDBHasher(IConfigurationSection conf)
        {
            if (conf == null)
                throw new NullReferenceException("bad config");

            string accountendpoint = conf["AccountEndpoint"];
            string accountKey = conf["AccountKey"];
            string databaseId = conf["DatabaseName"];
            string containerId = conf["ContainerName"];

            _db = new ThinHashesCosmosRepository(accountendpoint, accountKey, databaseId, containerId);
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _db?.Cleanup();
                }

                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }
        #endregion

        public override async Task EnsureExist()
        {
            await _db.Initialize();
        }

        public override async Task Generate(IEnumerable<IEnumerable<char>> tableOfTableOfChars, MD5 hasherMD5, SHA256 hasherSHA256,
            Func<string, string, string, long, long, bool> shouldBreakFunc, Stopwatch stopwatch = null,
            int batchInsertCount = 1_000, int batchTransactionCommitCount = 5_000)
        {
            string last_key_entry = await GetLastKeyEntry();

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
                var (hashMD5, hashSHA256) = CalculateTwoHashes(hasherMD5, hasherSHA256, key);

                var doc = new DocumentDBHash
                {
                    Id = id.ToString(),
                    Key = key,
                    HashMD5 = hashMD5,
                    HashSHA256 = hashSHA256
                };
                documents.Add(doc);

                if (counter % batchTransactionCommitCount == 0)
                {
                    var inserted = await _db.InvokeBulkInsertAsync(documents, batchInsertCount);
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
                var inserted = await _db.InvokeBulkInsertAsync(documents, batchInsertCount);
                documents.Clear();
            }
        }

        public override async Task<string> GetLastKeyEntry()
        {
            var job = await GetLastKeyEntryAsync();
            return job;
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

        public override async Task Purge()
        {
            await _db.InvokeBulkDeleteAsync();
        }

        public override async Task Verify()
        {
            var lst = await _db.GetItemsAsync<DocumentDBHash>(d => d.HashMD5 == "b25319faaaea0bf397b2bed872b78c45");
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

    class ThinHashesCosmosRepository
    {
        private readonly string _endpoint, _key, _databaseId, _containerId;
        private CosmosClient _client;
        private Database _database;
        private Container _container;

        public ThinHashesCosmosRepository(string endpoint, string key, string databaseId, string containerId)
        {
            _endpoint = endpoint;
            _key = key;
            _databaseId = databaseId;
            _containerId = containerId;
        }

        public async Task Initialize()
        {
            _client = new CosmosClient(_endpoint, _key, new CosmosClientOptions());

            // create database if not exists
            var dbResponse = await _client.CreateDatabaseIfNotExistsAsync(_databaseId);
            _database = dbResponse.Database;

            // create container if not exists with partition key /Key
            var containerProperties = new ContainerProperties(_containerId, "/Key");

            // set unique keys if supported
            containerProperties.UniqueKeyPolicy = new UniqueKeyPolicy();
            var uk = new UniqueKey();
            uk.Paths.Add("/Key");
            uk.Paths.Add("/HashMD5");
            uk.Paths.Add("/HashSHA256");
            containerProperties.UniqueKeyPolicy.UniqueKeys.Add(uk);

            var containerResponse = await _database.CreateContainerIfNotExistsAsync(containerProperties, throughput: 2500);
            _container = containerResponse.Container;
        }

        public async Task<IEnumerable<DocumentDBHash>> GetItemsSortedDescByKeyAsync(int itemsCount = -1)
        {
            string sql = "SELECT * FROM c ORDER BY c.Key DESC";
            var qd = new QueryDefinition(sql);
            var requestOptions = new QueryRequestOptions { MaxItemCount = itemsCount };
            var iterator = _container.GetItemQueryIterator<DocumentDBHash>(qd, requestOptions: requestOptions);

            List<DocumentDBHash> results = new List<DocumentDBHash>();
            while (iterator.HasMoreResults && (itemsCount <= 0 || results.Count < itemsCount))
            {
                var page = await iterator.ReadNextAsync();
                results.AddRange(page.Resource);
            }

            if (itemsCount > 0 && results.Count > itemsCount)
                return results.Take(itemsCount).ToList();

            return results;
        }

        public async Task<IEnumerable<T>> GetItemsAsync<T>(Expression<Func<T, bool>> predicate, int itemsCount = -1) where T : class
        {
            var queryable = _container.GetItemLinqQueryable<T>(allowSynchronousQueryExecution: false).Where(predicate);
            var iterator = queryable.ToFeedIterator();

            List<T> results = new List<T>();
            while (iterator.HasMoreResults && (itemsCount <= 0 || results.Count < itemsCount))
            {
                var page = await iterator.ReadNextAsync();
                results.AddRange(page.Resource);
            }

            if (itemsCount > 0 && results.Count > itemsCount)
                return results.Take(itemsCount).ToList();

            return results;
        }

        public async Task<DocumentDBHash> GetByIdAsync(string id)
        {
            var sql = "SELECT * FROM c WHERE c.id = @id";
            var qd = new QueryDefinition(sql).WithParameter("@id", id);
            var it = _container.GetItemQueryIterator<DocumentDBHash>(qd);
            var lst = new List<DocumentDBHash>();
            while (it.HasMoreResults)
            {
                var pg = await it.ReadNextAsync();
                lst.AddRange(pg.Resource);
                if (lst.Count > 0) break;
            }
            return lst.FirstOrDefault();
        }

        public async Task<ItemResponse<DocumentDBHash>> CreateItemAsync(DocumentDBHash item)
        {
            return await _container.CreateItemAsync(item, new PartitionKey(item.Key));
        }

        public async Task<ItemResponse<DocumentDBHash>> UpdateItemAsync(string id, DocumentDBHash item)
        {
            return await _container.ReplaceItemAsync(item, id, new PartitionKey(item.Key));
        }

        public async Task DeleteItemAsync(string id)
        {
            var existing = await GetByIdAsync(id);
            if (existing == null) return;
            await _container.DeleteItemAsync<DocumentDBHash>(existing.Id, new PartitionKey(existing.Key));
        }

        public void Cleanup()
        {
            _client?.Dispose();
            _client = null;
        }

        public async Task<int> InvokeBulkInsertAsync(List<DocumentDBHash> documents, int batchInsertCount = 1000)
        {
            if (documents == null || documents.Count == 0) return 0;

            int totalInserted = 0;
            int maxDegree = 50; // limit concurrency

            for (int i = 0; i < documents.Count; i += batchInsertCount)
            {
                var batch = documents.Skip(i).Take(batchInsertCount).ToList();
                var tasks = new List<Task<ItemResponse<DocumentDBHash>>>();
                var throttler = new System.Threading.SemaphoreSlim(maxDegree);

                foreach (var doc in batch)
                {
                    await throttler.WaitAsync().ConfigureAwait(false);
                    var task = _container.CreateItemAsync(doc, new PartitionKey(doc.Key))
                        .ContinueWith(t => { throttler.Release(); return t.Result; });
                    tasks.Add(task);
                }

                var results = await Task.WhenAll(tasks).ConfigureAwait(false);
                totalInserted += results.Length;
                throttler.Dispose();
            }

            return totalInserted;
        }

        public async Task InvokeBulkDeleteAsync()
        {
            // iterate all items and delete them in parallel (bounded)
            var sql = "SELECT c.id, c.Key FROM c";
            var qd = new QueryDefinition(sql);
            var iterator = _container.GetItemQueryIterator<ThinHashes>(qd);

            int maxDegree = 50;
            var deleteTasks = new List<Task>();
            var throttler = new System.Threading.SemaphoreSlim(maxDegree);

            while (iterator.HasMoreResults)
            {
                var page = await iterator.ReadNextAsync();
                foreach (var item in page.Resource)
                {
                    await throttler.WaitAsync().ConfigureAwait(false);
                    var task = _container.DeleteItemAsync<ThinHashes>(item.Key, new PartitionKey(item.Key))
                        .ContinueWith(t => throttler.Release());
                    deleteTasks.Add(task);
                }
            }

            await Task.WhenAll(deleteTasks).ConfigureAwait(false);
            throttler.Dispose();
        }
    }
}

#endif