using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Security.Cryptography;

namespace MyRainbow
{
	internal interface IDbHasher
	{
		void EnsureExist();
		void Generate(IEnumerable<IEnumerable<char>> tableOfTableOfChars, MD5 hasherMD5, SHA256 hasherSHA256,
			Func<string, string, string, long, long, bool> shouldBreakFunc, Stopwatch stopwatch = null,
			int batchInsertCount = 200, int batchTransactionCommitCount = 20000);
		string GetLastKeyEntry();
		void Purge();
		void Verify();
		void PostGenerateExecute();
	}

	internal abstract class DbHasher : IDbHasher
	{
		public abstract void EnsureExist();

		public abstract void Generate(IEnumerable<IEnumerable<char>> tableOfTableOfChars, MD5 hasherMD5, SHA256 hasherSHA256,
			Func<string, string, string, long, long, bool> shouldBreakFunc, Stopwatch stopwatch = null,
			int batchInsertCount = 200, int batchTransactionCommitCount = 20000);

		public abstract string GetLastKeyEntry();

		public abstract void Purge();

		public abstract void Verify();

		public virtual void PostGenerateExecute()
		{
			//empty method implementation
		}
	}
}
