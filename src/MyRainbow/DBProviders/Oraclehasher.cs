using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace MyRainbow.DBProviders
{
    internal class OracleHasher : IDbHasher, IDisposable
    {
        private OracleConnection Conn { get; set; }
        private OracleTransaction Tran { get; set; }

        public OracleHasher(string connectionString)
        {
            Conn = new OracleConnection(connectionString);
            Conn.Open();
            Tran = null;
        }

        public void EnsureExist()
        {
            try
            {
                string table_name = "Hashes";
                //Case sensitivity sucks for oracle DB.
                //On table name must encose within "" and on string compare we do not
                string cmd_text = $@"
DECLARE
	nCount NUMBER;
BEGIN

	SELECT count(*) INTO nCount FROM SYS.all_tables WHERE table_name = '{table_name}';
	IF(nCount <= 0)
	THEN

		EXECUTE IMMEDIATE 'CREATE TABLE ""{table_name}"" (
			""Key"" VARCHAR2(20 BYTE) NOT NULL ENABLE,
			""hashMD5"" CHAR(32 BYTE) NOT NULL ENABLE,
			""hashSHA256"" CHAR(64 BYTE) NOT NULL ENABLE,
			 CONSTRAINT ""{table_name}_PK"" PRIMARY KEY (""Key"")
			USING INDEX PCTFREE 10 INITRANS 2 MAXTRANS 255
			STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
			PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
			BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
			TABLESPACE ""USERS""  ENABLE
			 ) SEGMENT CREATION IMMEDIATE
			PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255
		   NOCOMPRESS LOGGING
			STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
			PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
			BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
			TABLESPACE ""USERS""';

	END IF;

END;
				";

                using (var cmd = new OracleCommand(cmd_text, Conn/*, Tran*/))
                {
                    cmd.Transaction = Tran;
                    cmd.CommandType = CommandType.Text;
                    cmd.ExecuteNonQuery();
                }
            }
            catch (Exception)
            {
                throw;
            }
        }

        public void Generate(IEnumerable<IEnumerable<char>> tableOfTableOfChars, MD5 hasherMD5, SHA256 hasherSHA256,
            Func<string, string, string, long, long, bool> shouldBreakFunc, Stopwatch stopwatch = null,
            int batchInsertCount = 200, int batchTransactionCommitCount = 20_000)
        {
            string last_key_entry = GetLastKeyEntry();

            double? nextPause = null;
            if (stopwatch != null)
            {
                stopwatch.Start();
                nextPause = stopwatch.Elapsed.TotalMilliseconds + 1_000;//next check after 1sec
            }
            long counter = 0, last_pause_counter = 0, tps = 0;
            var tran = Conn.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
            OracleCommand cmd = new OracleCommand("", Conn)
            {
                CommandType = System.Data.CommandType.Text,
                Transaction = tran
            };
            int param_counter = 0;
            foreach (var chars_table in tableOfTableOfChars)
            {
                var key = string.Concat(chars_table);
                if (!string.IsNullOrEmpty(last_key_entry) && last_key_entry.CompareTo(key) >= 0) continue;
                var hashMD5 = BitConverter.ToString(hasherMD5.ComputeHash(Encoding.UTF8.GetBytes(key))).Replace("-", "").ToLowerInvariant();
                var hashSHA256 = BitConverter.ToString(hasherSHA256.ComputeHash(Encoding.UTF8.GetBytes(key))).Replace("-", "").ToLowerInvariant();

                //dbase.Insert(value, hash);
                cmd.CommandText += $"INSERT INTO \"Hashes\"(\"Key\", \"hashMD5\", \"hashSHA256\")" +
                    $"VALUES(:key{param_counter}, :hashMD5{param_counter}, :hashSHA256{param_counter});{Environment.NewLine}";
                OracleParameter param;
                if (cmd.Parameters.Contains($"key{param_counter}"))
                {
                    param = cmd.Parameters[$"key{param_counter}"];
                    param.Value = key;
                }
                else
                {
                    param = new OracleParameter($"key{param_counter}", OracleDbType.Varchar2, 20);
                    param.Value = key;
                    cmd.Parameters.Add(param);
                }

                if (cmd.Parameters.Contains($"hashMD5{param_counter}"))
                {
                    param = cmd.Parameters[$"hashMD5{param_counter}"];
                    param.Value = hashMD5;
                }
                else
                {
                    param = new OracleParameter($"hashMD5{param_counter}", OracleDbType.Char, 32);
                    param.Value = hashMD5;
                    cmd.Parameters.Add(param);
                }

                if (cmd.Parameters.Contains($"hashSHA256{param_counter}"))
                {
                    param = cmd.Parameters[$"hashSHA256{param_counter}"];
                    param.Value = hashSHA256;
                }
                else
                {
                    param = new OracleParameter($"hashSHA256{param_counter}", OracleDbType.Char, 64);
                    param.Value = hashSHA256;
                    cmd.Parameters.Add(param);
                }

                param_counter++;

                if (counter % batchInsertCount == 0)
                {
                    //cmd.Prepare();
                    cmd.CommandText = $"BEGIN {cmd.CommandText} END;";
                    cmd.ExecuteNonQuery();
                    cmd.Parameters.Clear();
                    cmd.Dispose();
                    cmd = new OracleCommand("", Conn)
                    {
                        CommandType = System.Data.CommandType.Text,
                        Transaction = tran
                    };
                    param_counter = 0;
                }

                if (counter % batchTransactionCommitCount == 0)
                {
                    if (cmd != null && !string.IsNullOrEmpty(cmd.CommandText))
                    {
                        //cmd.Prepare();
                        cmd.CommandText = $"BEGIN {cmd.CommandText} END;";
                        cmd.ExecuteNonQuery();
                        cmd.Parameters.Clear();
                        cmd.Dispose();
                        cmd = new OracleCommand("", Conn)
                        {
                            CommandType = System.Data.CommandType.Text,
                            Transaction = tran
                        };
                        param_counter = 0;
                    }
                    tran.Commit(); tran.Dispose();
                    tran = null;

                    //Console.WriteLine($"MD5({key})={hashMD5},SHA256({key})={hashSHA256},counter={counter},tps={tps}");
                    //if (Console.KeyAvailable)
                    //	break;
                    if (shouldBreakFunc(key, hashMD5, hashSHA256, counter, tps))
                        break;

                    cmd.Transaction = tran = Conn.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
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

            if (cmd != null && !string.IsNullOrEmpty(cmd.CommandText))
            {
                //cmd.Prepare();
                cmd.CommandText = $"BEGIN {cmd.CommandText} END;";
                cmd.ExecuteNonQuery();
                cmd.Parameters.Clear();
                cmd.Dispose();
            }
            if (tran != null)
            {
                tran.Commit(); tran.Dispose();
            }
        }

        public string GetLastKeyEntry()
        {
            var sql = "SELECT * from (SELECT \"Key\" FROM \"Hashes\" ORDER BY \"Key\" DESC) WHERE rownum = 1";
            using (var cmd = new OracleCommand(sql, Conn/*, Tran*/))
            {
                cmd.Transaction = Tran;
                cmd.CommandType = CommandType.Text;
                var str = cmd.ExecuteScalar();
                return str == null ? null : (string)str;
            }
        }

        public void PostGenerateExecute()
        {
            string table_name = "Hashes";

            //string database = Conn.ConnectionString.Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries)
            //	.ToList().FirstOrDefault(x => x.ToLower().StartsWith("database"))
            //	.Split(new char[] { '=' }, StringSplitOptions.RemoveEmptyEntries)[1].Trim();

            string cmd_text = $@"
DECLARE
	nCount NUMBER;
BEGIN

	SELECT count(*) INTO nCount FROM all_constraints WHERE constraint_name = '{table_name}_UK_MD5' or constraint_name = '{table_name}_UK_SHA256';
	IF(nCount <= 0)
	THEN

		execute immediate 'ALTER TABLE ""{table_name}"" ADD CONSTRAINT ""{table_name}_UK_MD5"" UNIQUE (""hashMD5"" ) ENABLE
			ADD CONSTRAINT ""{table_name}_UK_SHA256"" UNIQUE (""hashSHA256"") ENABLE';  
	
	END IF;

END;
";

            Console.Write("Creating indexes...");
            using (var cmd = new OracleCommand(cmd_text, Conn/*, Tran*/))
            {
                cmd.Transaction = Tran;
                cmd.CommandTimeout = 0;
                cmd.CommandType = CommandType.Text;
                cmd.ExecuteNonQuery();
            }
            Console.WriteLine("done");
        }

        public void Purge()
        {
            using (var cmd = new OracleCommand("truncate table \"Hashes\"", Conn/*, Tran*/))
            {
                cmd.Transaction = Tran;
                cmd.CommandType = CommandType.Text;
                cmd.ExecuteNonQuery();
            }
        }

        public void Verify()
        {
            using (var cmd = new OracleCommand("SELECT * FROM \"Hashes\" WHERE \"hashMD5\" = :hashMD5", Conn/*, Tran*/))
            {
                cmd.CommandType = CommandType.Text;

                var param_key = new OracleParameter
                {
                    ParameterName = "hashMD5",
                    DbType = DbType.String,
                    Size = 32,
                    Value = "b25319faaaea0bf397b2bed872b78c45"
                };
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

        #region IDisposable Support

        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                    if (Conn != null)
                    {
                        if (Conn.State != ConnectionState.Closed)
                            Conn.Close();
                        Conn.Dispose();
                    }
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~OracleHasher() {
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

        #endregion
    }
}
