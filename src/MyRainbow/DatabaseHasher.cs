using System;
using System.Data;
using System.Data.SqlClient;

namespace MyRainbow
{
	internal class DatabaseHasher : IDisposable
	{
		//private readonly string _connectionString = "Server=chaos2;MultipleActiveResultSets=True;Initial Catalog=test;User ID=test;Password=XXXXXXXXX;";

		private SqlTransaction _tran;

		public SqlConnection Conn { get; set; }
		public SqlTransaction Tran { get; set; }

		public DatabaseHasher(SqlConnection conn, SqlTransaction tran = null)
		{
			Conn = conn;
			Tran = tran;
		}

		internal void Purge()
		{
			using (var cmd = new SqlCommand("truncate table hashes_md5", Conn, Tran))
			{
				cmd.CommandType = System.Data.CommandType.Text;
				cmd.ExecuteNonQuery();
			}
		}

		internal void EnsureExist()
		{
			string table_name = "hashes_md5";

			string cmd_text = $@"IF(NOT EXISTS(SELECT *
                     FROM INFORMATION_SCHEMA.TABLES
                     WHERE TABLE_SCHEMA = 'dbo'
                     AND  TABLE_NAME = '{table_name}'))
                BEGIN

               CREATE TABLE [dbo].[{table_name}](
                    [key] [varchar](200) NOT NULL,
                    [hash] [char](32) NOT NULL,
                 CONSTRAINT [PK_{table_name}] PRIMARY KEY CLUSTERED 
                (
                    [key] ASC
                )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
                 CONSTRAINT [IX_{table_name}] UNIQUE NONCLUSTERED 
                (
                    [hash] ASC
                )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
                ) ON [PRIMARY]
               
            END";

			using (var cmd = new SqlCommand(cmd_text, Conn, Tran))
			{
				cmd.CommandType = CommandType.Text;
				cmd.ExecuteNonQuery();
			}
		}

		internal void Insert(string key, string hash)
		{
			using (var cmd = new SqlCommand("insert into hashes_md5([key], hash) values(@key, @hash);", Conn, Tran))
			{
				cmd.CommandType = System.Data.CommandType.Text;

				var param_key = new SqlParameter("@key", System.Data.SqlDbType.VarChar, 200, key);
				param_key.Value = key;
				cmd.Parameters.Add(param_key);
				var hash_key = new SqlParameter("@hash", System.Data.SqlDbType.VarChar, 200, key);
				hash_key.Value = hash;
				cmd.Parameters.Add(hash_key);

				cmd.Prepare();
				cmd.ExecuteNonQuery();
			}
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
				// free managed resources
				//if (Conn != null)
				//{
				//	if (Conn.State != ConnectionState.Closed)
				//		Conn.Close();
				//	Conn.Dispose();
				//}
			}
			// free native resources if there are any.
		}
		#endregion Implementation
	}
}