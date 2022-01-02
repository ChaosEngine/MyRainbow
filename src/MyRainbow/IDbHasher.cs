using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Threading.Tasks;

namespace MyRainbow
{
	internal interface IDbHasher
	{
		Task EnsureExist();
		Task Generate(IEnumerable<IEnumerable<char>> tableOfTableOfChars, MD5 hasherMD5, SHA256 hasherSHA256,
			Func<string, string, string, long, long, bool> shouldBreakFunc, Stopwatch stopwatch = null,
			int batchInsertCount = 200, int batchTransactionCommitCount = 20000);
		Task<string> GetLastKeyEntry();
		Task Purge();
		Task Verify();
		Task PostGenerateExecute();
	}

	internal abstract class DbHasher : IDbHasher
	{
		public abstract Task EnsureExist();

		public abstract Task Generate(IEnumerable<IEnumerable<char>> tableOfTableOfChars, MD5 hasherMD5, SHA256 hasherSHA256,
			Func<string, string, string, long, long, bool> shouldBreakFunc, Stopwatch stopwatch = null,
			int batchInsertCount = 200, int batchTransactionCommitCount = 20000);

		public abstract Task<string> GetLastKeyEntry();

		public abstract Task Purge();

		public abstract Task Verify();

		public virtual Task PostGenerateExecute()
		{
			//empty method implementation
			return Task.CompletedTask;
		}
	}
}
