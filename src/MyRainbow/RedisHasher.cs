using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;

namespace MyRainbow
{
	internal class RedisHasher : DbHasher, IDisposable
	{
		private ConnectionMultiplexer Cache { get; set; }

		public RedisHasher(string redisConfiguration)
		{
			//var cache = new RedisCache(new RedisCacheOptions
			//{
			//	Configuration = redisConfiguration,
			//	InstanceName = "SampleInstance"
			//});

			ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(redisConfiguration);
			Cache = redis;
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
				if (Cache != null && Cache.IsConnected)
				{
					Cache.Close();
				}
			}
			// free native resources if there are any.
		}

		public override void EnsureExist()
		{
			var key = "myKey";
			var message = "Hello, World!";
			var value = Encoding.UTF8.GetBytes(message);

			Console.WriteLine("Connected");
			var dbase = Cache.GetDatabase();

			Console.WriteLine($"Setting value '{message}' in cache");
			dbase.StringSet(key, value);
			Console.WriteLine("Set");

			Console.WriteLine("Getting value from cache");
			value = dbase.StringGet(key);
			if (value != null)
			{
				Console.WriteLine("Retrieved: " + Encoding.UTF8.GetString(value));
			}
			else
			{
				Console.WriteLine("Not Found");
			}
		}

		public override void Generate(IEnumerable<IEnumerable<char>> tableOfTableOfChars, MD5 hasherMD5, SHA256 hasherSHA256,
			Func<string, string, string, long, long, bool> shouldBreakFunc, Stopwatch stopwatch = null,
			int batchInsertCount = 200, int batchTransactionCommitCount = 2000/*0*/)
		{
			double? nextPause = null;
			if (stopwatch != null)
			{
				stopwatch.Start();
				nextPause = stopwatch.Elapsed.TotalMilliseconds + 1000;//next check after 1sec
			}
			long counter = 0, last_pause_counter = 0, tps = 0;

			//var opts = new DistributedCacheEntryOptions();
			var dbase = Cache.GetDatabase();
			foreach (var chars_table in tableOfTableOfChars)
			{
				var key = string.Concat(chars_table);
				//if (!string.IsNullOrEmpty(last_key_entry) && last_key_entry.CompareTo(key) >= 0) continue;
				var hashMD5 = BitConverter.ToString(hasherMD5.ComputeHash(Encoding.UTF8.GetBytes(key))).Replace("-", "").ToLowerInvariant();
				var hashSHA256 = BitConverter.ToString(hasherSHA256.ComputeHash(Encoding.UTF8.GetBytes(key))).Replace("-", "").ToLowerInvariant();

				//work
				dbase.StringSet($"MD5_{hashMD5}", key);
				dbase.StringSet($"SHA256_{hashSHA256}", key);

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
			}//end foreach
		}

		public override void Verify()
		{
			var dbase = Cache.GetDatabase();

			var r_val = dbase.StringGet("MD5_" + "9409135542c79d1ed50c9fde07fa600a");
		}

		public override void Purge()
		{
			//Cache.GetDatabase().remove
		}

		public override string GetLastKeyEntry()
		{
			throw new NotImplementedException();
		}
		
		#endregion Implementation
	}
}