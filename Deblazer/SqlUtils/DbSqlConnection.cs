using System;
using System.Data.SqlClient;

namespace Dg.Deblazer.SqlUtils
{
    public class DbSqlConnection : IDisposable
    {
        public SqlConnection SqlConnection { get; private set; }

        public event Action OnDispose;

        // Fix me if you can
        // This field is true sometimes because we do not want to open or close another connection for AggregateUpdates (and possibly other stuff that is going on during SubmitChanges).
        // This is very leaky though, because sqlConnection will never get disposed.
        private bool helpIAmBeingHacked;

        public DbSqlConnection(string connectionString, SqlConnection existingSqlConnection)
        {
            if (existingSqlConnection != null)
            {
                SqlConnection = existingSqlConnection;
                helpIAmBeingHacked = true;
            }
            else
            {
                SqlConnection = new SqlConnection(connectionString);
                helpIAmBeingHacked = false;
            }
        }

        public void Open()
        {
            if (!helpIAmBeingHacked)
            {
                SqlConnection.Open();
            }
        }

        public void Dispose()
        {
            if (!helpIAmBeingHacked)
            {
                SqlConnection.Close();
                SqlConnection.Dispose();
                SqlConnection = null;
                OnDispose?.Invoke();
            }
        }
    }
}