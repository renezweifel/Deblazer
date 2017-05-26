using System;
using System.Data.SqlClient;
using System.Runtime.Serialization;
using Dg.Deblazer.SqlGeneration;

namespace Dg.Deblazer.Write
{
    [Serializable]
    public class DbSqlException : Exception
    {
        // Implement constructor for deserialization (Exception implements ISerializable)
        public DbSqlException()
        {
        }

        public DbSqlException(SerializationInfo info, StreamingContext context) : base(info, context) { }

        internal DbSqlException(string message, SqlException innerException, DbEntity failingEntity, SqlQueryType sqlQueryType)
            : this(message, innerException)
        {
            FailingEntity = failingEntity;
            SqlQueryType = sqlQueryType;
        }

        public DbSqlException(string message, SqlException innerException) : base(message, innerException)
        {
        }

        public SqlException SqlException
        {
            get
            {
                return InnerException as SqlException;
            }
        }

        public readonly DbEntity FailingEntity;
        internal readonly SqlQueryType SqlQueryType;
    }
}