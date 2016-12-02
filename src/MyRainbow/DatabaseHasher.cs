using System;
using System.Data;
using System.Data.SqlClient;

namespace MyRainbow
{
    internal class DatabaseHasher : IDisposable
    {
        const string _connectionString = "Server=chaos2;MultipleActiveResultSets=True;Initial Catalog=test;User ID=test;Password=XXXXXXXXXXXXXXXXXXX;";

        private SqlConnection _conn;

        public DatabaseHasher()
        {
            _conn = new SqlConnection(_connectionString);
            _conn.Open();
        }

        internal void Purge()
        {
            using (var cmd = new SqlCommand("truncate table hashes_md5", _conn))
            {
                cmd.CommandType = System.Data.CommandType.Text;
                cmd.ExecuteNonQuery();
            }
        }

        internal void EnsureExist()
        {

            /*using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();


//            IF (EXISTS (SELECT * 
//                 FROM INFORMATION_SCHEMA.TABLES 
//                 WHERE TABLE_SCHEMA = 'TheSchema' 
//                 AND  TABLE_NAME = 'TheTable'))
//BEGIN
//    --Do Stuff
//END




            }*/
        }

        internal void Insert(string key, string hash)
        {
            using (var cmd = new SqlCommand("insert into hashes_md5([key], hash) values(@key, @hash);", _conn))
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
                if (_conn != null)
                {
                    if (_conn.State != ConnectionState.Closed)
                        _conn.Close();
                    _conn.Dispose();
                }
            }
            // free native resources if there are any.
        }

    }
}