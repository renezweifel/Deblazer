using Dg.Deblazer.Configuration;
using Dg.Deblazer.Extensions;
using Dg.Deblazer.Internal;
using Dg.Deblazer.Read;
using Dg.Deblazer.SqlUtils;
using Dg.Deblazer.Visitors;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace Dg.Deblazer.SqlGeneration
{
    public class QuerySql<TCurrent, TBack, TElement> : QuerySqlBase, IReadOnlyList<TElement>
    {
        internal QuerySql(BaseDb db, DbSqlCommand customSqlCommand)
        {
            this.customSqlCommand = customSqlCommand;
            Db = db;
        }

        internal QuerySql(BaseDb Db, string sql, IReadOnlyList<object> parameters)
        {
            sqlString = sql.Trim('\n', '\r', ' ', '\t');
            this.Db = Db;
            this.parameters = parameters;
        }

        // We don't want a internal default constructor
        private QuerySql()
        {
        }

        private IReadOnlyList<TElement> cachedElements { get; set; }
        private readonly object cachedElementsLock = new object();

        public string Sql
        {
            get
            {
                var sqlCommand = GetSqlCommand();
                QueryHelpers.AddSqlParameters(sqlCommand, parameters);
                return SqlCommands.GetSqlCommandText(sqlCommand.SqlCommand);
            }
        }

        public IEnumerator<TElement> GetEnumerator()
        {
            EnsureElementsAreLoaded();
            return cachedElements.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public TElement this[int index]
        {
            get
            {
                EnsureElementsAreLoaded();
                return cachedElements[index];
            }
        }

        public int Count
        {
            get
            {
                EnsureElementsAreLoaded();
                return cachedElements.Count;
            }
        }

        public event Action<DbEntity> ValueRemoved;

        public event Action<DbEntity> ValueLoadedBeforeRightsCheck;

        protected void EmitValueLoadedBeforeRightsCheck(DbEntity entity)
        {
            if (entity != null)
            {
                ValueLoadedBeforeRightsCheck?.Invoke(entity);
            }
        }

        protected override void EmitValueLoaded(DbEntity entity)
        {
            if (entity != null)
            {
                Db.TriggerEntitiesLoaded(new[] { entity });
            }
        }

        protected override void EmitValueRemoved(DbEntity entity)
        {
            if (entity != null)
            {
                ValueRemoved?.Invoke(entity);
            }
        }

        public static DbType TypeToDbType<TSystem>()
        {
            DbType dbt;
            try
            {
                dbt = (DbType)Enum.Parse(typeof(DbType), typeof(TSystem).Name);
            }
            catch
            {
                dbt = DbType.Object;
            }

            return dbt;
        }

        private void EnsureElementsAreLoaded()
        {
            if (cachedElements != null)
            {
                return;
            }

            lock (cachedElementsLock)
            {
                if (cachedElements != null)
                {
                    return;
                }

                var cachedTypedElements = new List<TElement>();
                List<(TElement, List<object>)> loadedData = SqlCommands.ExecuteSqlCommand(() =>
                {
                    using (var sqlConnection = Db.GetConnection())
                    {
                        using (var sqlCommand = GetSqlCommand())
                        {
                            QueryHelpers.AddSqlParameters(sqlCommand, parameters);

                            // AppContext.StartRequestTiming(sql, "OpenConnection");
                            sqlConnection.Open();
                            // AppContext.StopRequestTiming(sql, "OpenConnection");

                            sqlCommand.Connection = sqlConnection.SqlConnection;
                            // sqlCommand.Transaction = sqlConnection.SqlConnection.BeginTransaction(Db.Settings().IsolationLevel);

                            if (sqlCommand.CommandType == CommandType.StoredProcedure)
                            {
                                SqlParameter returnValue = SqlParameterUtils.Create("@ReturnValue", TypeToDbType<TElement>());
                                returnValue.Direction = ParameterDirection.ReturnValue;
                                sqlCommand.Parameters.Add(returnValue);
                                sqlCommand.ExecuteNonQuery();

                                TElement entity = (TElement)sqlCommand.Parameters["@ReturnValue"].Value;
                                cachedTypedElements.Add(entity);

                                sqlCommand.Parameters.Clear();
                                return new List<(TElement, List<object>)>();
                            }
                            else
                            {
                                // AppContext.StartRequestTiming(sql, "ExecuteReader");

                                var loadedDataFromReader = new List<(TElement, List<object>)>();
                                using (SqlDataReader reader = sqlCommand.ExecuteReader(CommandBehavior.SequentialAccess))
                                {
                                    // AppContext.StopRequestTiming(sql, "ExecuteReader");
                                    // AppContext.StartRequestTiming(sql, "FillVisitor");

                                    var fillVisitor = new FillVisitor(
                                        reader: reader, 
                                        db: Db,
                                        objectFillerFactory: new ObjectFillerFactory());
                                    // fillVisitor.SQL = sql;

                                    // AppContext.StopRequestTiming(sql, "FillVisitor");
                                    // AppContext.StartRequestTiming(sql, "Read");
                                    int k = 0;
                                    while (fillVisitor.Read())
                                    {
                                        // AppContext.StopRequestTiming(sql, "Read");
                                        // AppContext.StartRequestTiming(sql, "Fill", k > 0);

                                        TElement entity = QueryHelpers.Fill(default(TElement), fillVisitor);
                                        var dbEntity = entity as DbEntity;
                                        if (dbEntity != null)
                                        {
                                            ((IDbEntityInternal)dbEntity).SetAllowSettingColumns(false);
                                        }

                                        if (entity is IId)
                                        {
                                            entity = (TElement)Db.LoadedEntityCache.GetOrAdd(typeof(TElement), "Id", ((IId)entity).Id, entity);
                                        }

                                        // AppContext.StopRequestTiming(sql, "Fill");

                                        // First fetch all entities from the db and then return them
                                        // Otherwise we have problems, if someone calls AnyDb() first (AnyDb() just requests the first entity)
                                        // Anyway we improve performance if someone calls the query two times: The second time, we don't query the
                                        // database but return the cached elements

                                        List<object> subEntities = new List<object>();
                                        if (fillVisitor.HasNext && !fillVisitor.IsDBNull())
                                        {
                                            subEntities.OfType<IDbEntityInternal>().ForEach(e => e.SetAllowSettingColumns(true));
                                            // AppContext.StopRequestTiming(sql, "subEntities");
                                        }

                                        k++;
                                        loadedDataFromReader.Add((entity, subEntities));
                                        // CachedElements.Add(entity);
                                    }

                                    // AppContext.StopRequestTiming(sql, "Read");
                                }

                                sqlCommand.Parameters.Clear();
                                return loadedDataFromReader;
                            }
                        }
                    }
                });

                // Connect the items to one another (i.e. if Person.Customer was called, set Person.Customer to the recently loaded entity)
                // First emit EmitValueLoadedBeforeRightsCheck(dbEntity); for all entities before checking the rights, because if only do it within the while loop below just before Db.ReturnEntity for each dbEntity,
                // in GetOwnerMandatorId() of i.e. User (which is called from Db.ReturnEntity()), if we call Person.Customer.MandatorId, Customer is loaded from the DB, because Person.Customer was not set yet
                loadedData.Select(e => e.Item1).OfType<DbEntity>().ForEach(e => EmitValueLoadedBeforeRightsCheck(e));
                // Do this after the sql connection above has been closed
                // Otherwise we get this exception:
                // System.Reflection.TargetInvocationException: Exception has been thrown by the target of an invocation. ---> System.Transactions.TransactionAbortedException: The transaction has aborted. ---> System.Transactions.TransactionPromotionException: Failure while attempting to promote transaction. ---> System.Data.SqlClient.SqlException: There is already an open DataReader associated with this Command which must

                var entityFilter = GlobalDbConfiguration.GetConfigurationOrEmpty(typeof(TElement)).EntityFilter;
                for (int i = 0; i < loadedData.Count; i++)
                {
                    var entity = loadedData[i].Item1;
                    var joinedEntities = loadedData[i].Item2;

                    GlobalDbConfiguration.QueryLogger.IncrementLoadedElementCount(increment: 1);
                    // Count the joined entities too
                    GlobalDbConfiguration.QueryLogger.IncrementLoadedElementCount(increment: joinedEntities?.Count ?? 0);

                    var dbEntity = entity as DbEntity;
                    if (dbEntity != null)
                    {
                        ((IDbEntityInternal)dbEntity).SetAllowSettingColumns(allowSettingColumns: true);
                    }

                    if (entityFilter.DoReturnEntity(Db.Settings, entity, joinedEntities))
                    {
                        EmitValueLoaded(dbEntity);
                        cachedTypedElements.Add(entity);
                    }
                    else
                    {
                        EmitValueRemoved(dbEntity);
                    }
                }

                cachedElements = cachedTypedElements.AsReadOnly();
            }
        }
    }
}