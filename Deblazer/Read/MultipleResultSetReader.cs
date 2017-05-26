using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using Dg.Deblazer.Configuration;
using Dg.Deblazer.Internal;
using Dg.Deblazer.SqlUtils;
using Dg.Deblazer.Visitors;

namespace Dg.Deblazer.Read
{
    public class MultipleResultSetReader : IDisposable
    {
        private readonly DbSqlCommand sqlCommand;
        private readonly IDbInternal db;

        private SqlConnection sqlConnection;
        private SqlDataReader sqlDataReader;

        internal MultipleResultSetReader(DbSqlCommand sqlCommand, IDbInternal db)
        {
            this.sqlCommand = sqlCommand;
            this.db = db;
        }

        public bool SkipResultSet()
        {
            InitConnection();
            return sqlDataReader.NextResult();
        }

        public List<T> GetNextResultSet<T>()
        {
            if (!InitConnection())
            {
                if (!sqlDataReader.NextResult())
                {
                    throw new InvalidOperationException("There is no result set left");
                }
            }

            var fillVisitor = new FillVisitor(
                reader: sqlDataReader, 
                db: db,
                objectFillerFactory: new ObjectFillerFactory());
            var resultSet = new List<T>();

            var configuration = GlobalDbConfiguration.GetConfigurationOrEmpty(typeof(T));
            var entityFilter = configuration.EntityFilter;
            var queryLogger = GlobalDbConfiguration.QueryLogger;

            while (sqlDataReader.Read())
            {
                var entity = fillVisitor.Fill<T>();

                queryLogger.IncrementLoadedElementCount(increment: 1);

                if (entityFilter.DoReturnEntity(db.Settings, entity))
                {
                    EmitValueLoaded(entity);
                    resultSet.Add(entity);
                }
            }

            return resultSet;
        }

        private bool InitConnection()
        {
            if (sqlConnection == null)
            {
                sqlConnection = new SqlConnection(db.Settings.ConnectionString);
                sqlCommand.Connection = sqlConnection;
                sqlConnection.Open();

                sqlDataReader = sqlCommand.ExecuteReader(CommandBehavior.SequentialAccess);
                return true;
            }

            return false;
        }

        private void EmitValueLoaded(object entity)
        {
            var dbEntity = entity as DbEntity;
            if (dbEntity != null)
            {
                ((IDbInternal)db).TriggerEntitiesLoaded(new[] { dbEntity });
            }
        }

        public void Dispose()
        {
            if (sqlDataReader != null)
            {
                sqlDataReader.Dispose();
            }

            if (sqlCommand != null)
            {
                sqlCommand.Dispose();
            }

            if (sqlConnection != null)
            {
                sqlConnection.Dispose();
            }
        }
    }
}