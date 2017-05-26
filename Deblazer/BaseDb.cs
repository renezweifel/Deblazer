using Dg.Deblazer.Cache;
using Dg.Deblazer.Configuration;
using Dg.Deblazer.Extensions;
using Dg.Deblazer.Internal;
using Dg.Deblazer.Read;
using Dg.Deblazer.Settings;
using Dg.Deblazer.SqlGeneration;
using Dg.Deblazer.SqlUtils;
using Dg.Deblazer.Visitors;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Dg.Deblazer
{
    public abstract class BaseDb : IDbRead, IDbInternal, IDisposable
    {
        public static event Action<IDb> OnDbInitialized;

        protected readonly LoadedEntityCache loadedEntityCache;
        LoadedEntityCache IDbInternal.LoadedEntityCache => loadedEntityCache;

        public event Action<IReadOnlyList<DbEntity>> EntitiesLoaded;
        public event MixedDbEventHandler NotifyMixedDb;

        protected SqlConnection currentOpenConnection;

        private readonly IDbSettings settings;
        public IDbSettings Settings => settings;

        protected BaseDb(IDbSettings settings)
        {
            if (settings == null)
            {
                throw new ArgumentNullException(nameof(settings));
            }

            this.settings = settings;
            loadedEntityCache = new LoadedEntityCache(Settings);

            OnDbInitialized?.Invoke(this);
        }

        public IIdQuery<T> GetIIdTable<T>() where T : DbEntity, IId
        {
            var dbEntityQuery = new IIdQuery<T>(this);
            return dbEntityQuery;
        }

        public ILongIdQuery<T> GetILongIdTable<T>() where T : DbEntity, ILongId
        {
            var dbEntityQuery = new ILongIdQuery<T>(this);
            return dbEntityQuery;
        }

        public DbSqlConnection GetConnection() => new DbSqlConnection(Settings.ConnectionString, currentOpenConnection);

        protected virtual void OnEntitiesLoaded(IReadOnlyList<DbEntity> loadedEntities)
        {
            EntitiesLoaded?.Invoke(loadedEntities);
        }

        void IDbInternal.TriggerEntitiesLoaded(IReadOnlyList<DbEntity> entities)
        {
            OnEntitiesLoaded(entities);
        }

        private IReadOnlyList<object> Load(Type type, string sql, params object[] values)
        {
            List<object> entities = new List<object>();
            using (var sqlConnection = GetConnection())
            {
                using (var sqlCommand = new DbSqlCommand(sql))
                {
                    sqlCommand.Connection = sqlConnection.SqlConnection;

                    QueryHelpers.AddSqlParameters(sqlCommand, values);
                    sqlConnection.Open();

                    using (SqlDataReader reader = sqlCommand.ExecuteReader(CommandBehavior.SequentialAccess))
                    {
                        var fillVisitor = new FillVisitor(
                            reader: reader,
                            db: this,
                            objectFillerFactory: new ObjectFillerFactory());
                        while (fillVisitor.Read())
                        {
                            object entity = QueryHelpers.Fill(type, entity: null, fillVisitor: fillVisitor);
                            entities.Add(entity);
                        }
                    }
                }
            }

            var entitiesToReturn = new List<object>(entities.Count);

            var configuration = GlobalDbConfiguration.GetConfigurationOrEmpty(type);
            var entityFilter = configuration.EntityFilter;
            var queryLogger = GlobalDbConfiguration.QueryLogger;

            foreach (object entity in entities)
            {
                queryLogger.IncrementLoadedElementCount(increment: 1);
                if (entityFilter.DoReturnEntity(Settings, entity))
                {
                    entitiesToReturn.Add(entity);
                }
            }

            OnEntitiesLoaded(entitiesToReturn
                    .Where(e => e is DbEntity)
                    .Select(e => e as DbEntity)
                    .ToList());

            return entitiesToReturn;
        }

        QuerySql<T, T, T> IDbInternal.LoadBy<T>(string[] columns, IReadOnlyList<int> ids)
        {
            var helper = QueryHelpers.GetHelper(typeof(T));

            var sb = new StringBuilder();
            sb.Append("SELECT ");
            sb.AppendFormat(helper.ColumnsString, helper.FullTableName);
            sb.Append(" FROM ");
            sb.Append(helper.FullTableName);
            sb.Append(" WHERE ");

            int i = 0;
            foreach (var columnName in columns)
            {
                if (i > 0)
                {
                    sb.Append(" AND ");
                }
                sb.Append(columnName);
                sb.Append(" = @");
                sb.Append(i);
                sb.Append(" AND ");
                sb.Append(columnName);
                sb.Append(" IS NOT NULL");
                i++;
            }

            return Load<T>(sb.ToString(), ids.Select(id => (object)id).ToList());
        }

        public QuerySql<T, T, T> Load<T>(SqlCommand sqlCommand)
        {
            return Load<T>(new DbSqlCommand(sqlCommand));
        }

        public QuerySql<T, T, T> Load<T>(DbSqlCommand sqlCommand)
        {
            var querySql = new QuerySql<T, T, T>(this, sqlCommand);
            return querySql;
        }

        public QuerySql<T, T, T> Load<T>(string sql, IReadOnlyList<object> values)
        {
            var querySql = new QuerySql<T, T, T>(this, sql, values);
            return querySql;
        }

        public QuerySql<T, T, T> Load<T>(string sql, params object[] values)
        {
            var querySql = new QuerySql<T, T, T>(this, sql, values);
            return querySql;
        }

        public T SingleOrDefaultDb<T>(DbSqlCommand sqlCommand)
        {
            return Load<T>(sqlCommand).SingleOrDefault();
        }

        private IReadOnlyList<DbEntity> LoadWhere(Type type, string whereQuery, params object[] values)
        {
            string sql = string.Format("SELECT {0} FROM {1} WHERE {2}", QueryHelpers.ConcatColumnsInSelectStatement(type, -1), QueryHelpers.GetFullTableName(type), whereQuery);
            return Load(type, sql, values).Cast<DbEntity>().ToList();
        }

        private QuerySql<T, T, T> LoadWhere<T>(string whereQuery, params object[] values) where T : DbEntity, ILongId
        {
            string sql = string.Format("SELECT {0} FROM {1} WHERE {2}", QueryHelpers.ConcatColumnsInSelectStatement<T>(-1), QueryHelpers.GetFullTableName<T>(), whereQuery);
            return Load<T>(sql, values);
        }

        private TEntity SingleOrDefaultDb<TEntity>(int id) where TEntity : DbEntity, IId
        {
            TEntity result;
            if (!loadedEntityCache.TryGet("Id", id, out result))
            {
                result = LoadWhere<TEntity>("Id = @0", id).SingleOrDefault();
                return (TEntity)loadedEntityCache.GetOrAdd(typeof(TEntity), "Id", id, result);
            }

            return result;
        }
        private TEntity SingleOrDefaultDb<TEntity>(long id) where TEntity : DbEntity, ILongId
        {
            TEntity result;
            if (!loadedEntityCache.TryGet("Id", id, out result))
            {
                result = LoadWhere<TEntity>("Id = @0", id).SingleOrDefault();
                return (TEntity)loadedEntityCache.GetOrAdd(typeof(TEntity), "Id", id, result);
            }

            return result;
        }

        public TEntity SingleOrDefaultDb<TEntity>(Id<TEntity> id) where TEntity : DbEntity, IId
        {
            TEntity result;
            if (!loadedEntityCache.TryGet("Id", (int)id, out result))
            {
                result = LoadWhere<TEntity>("Id = @0", (int)id).SingleOrDefault();
                return (TEntity)loadedEntityCache.GetOrAdd(typeof(TEntity), "Id", (int)id, result);
            }

            return result;
        }

        public TEntity SingleOrDefaultDb<TEntity>(LongId<TEntity> id) where TEntity : DbEntity, ILongId
        {
            TEntity result;
            if (!loadedEntityCache.TryGet("Id", id.ToLong(), out result))
            {
                result = LoadWhere<TEntity>("Id = @0", id.ToLong()).SingleOrDefault();
                return (TEntity)loadedEntityCache.GetOrAdd(typeof(TEntity), "Id", id.ToLong(), result);
            }

            return result;
        }

        public DbEntity SingleOrDefaultDb(string typeName, int id)
        {
            return SingleOrDefaultDb(QueryHelpers.GetDbEntityType(typeName), id);
        }

        public DbEntity SingleOrDefaultDb(string typeName, long id)
        {
            return SingleOrDefaultDb(QueryHelpers.GetDbEntityType(typeName), id);
        }

        public DbEntity SingleOrDefaultDb(Type type, long id)
        {
            object result;
            if (!loadedEntityCache.TryGet(type, "Id", id, out result))
            {
                var list = LoadWhere(type, "Id = @0", id);
                if (list.Count > 0)
                {
                    result = loadedEntityCache.GetOrAdd(type, "Id", id, list[0]);
                }
            }

            return (DbEntity)result;
        }

        public TSelected SelectSingleDb<TSelected, TEntity>(Id<TEntity> entityId, Expression<Func<TEntity, TSelected>> selector) where TEntity : IId
        {
            var sql = GetSelectSingleSql(entityId, selector);
            return Load<TSelected>(sql).Single();
        }

        private static string GetSelectSingleSql<TSelected, TEntity>(Id<TEntity> entityId, Expression<Func<TEntity, TSelected>> selector) where TEntity : IId
        {
            var memberName = GetMemberName(selector);
            var whereQuery = $"Id = {entityId.ToInt()}";
            var sql = string.Format("SELECT {0} FROM {1} WHERE {2}", memberName, QueryHelpers.GetFullTableName<TEntity>(), whereQuery);
            return sql;
        }

        private static string GetMemberName<TSelected, TEntity>(Expression<Func<TEntity, TSelected>> selector) where TEntity : IId
        {
            var memberExpression = selector.Body as MemberExpression;
            if (memberExpression == null)
            {
                throw new ArgumentException($"{nameof(selector)} must be a {nameof(MemberExpression)}.", nameof(selector));
            }

            var memberName = memberExpression.Member.Name;
            return memberName;
        }

        public TSelected SelectSingleOrDefaultDb<TSelected, TEntity>(Id<TEntity> entityId, Expression<Func<TEntity, TSelected>> selector) where TEntity : IId
        {
            var sql = GetSelectSingleSql(entityId, selector);
            return Load<TSelected>(sql).SingleOrDefault();
        }

        public DbEntity SingleDb(Type type, long id)
        {
            object result;
            if (!loadedEntityCache.TryGet(type, "Id", id, out result))
            {
                var list = LoadWhere(type, "Id = @0", id);
                if (list.Count == 0)
                {
                    throw new InvalidOperationException("There is no " + type.Name + " with Id = " + id);
                }

                result = list[0];
                return (DbEntity)loadedEntityCache.GetOrAdd(type, "Id", id, result);
            }

            return (DbEntity)result;
        }

        // This method sounds like a nasty web service...
        private TEntity SinglePrivateDb<TEntity>(long id) where TEntity : DbEntity, ILongId
        {
            return (TEntity)SingleDb(typeof(TEntity), id);
        }

        public TEntity SingleDb<TEntity>(Id<TEntity> id) where TEntity : DbEntity, IId => SinglePrivateDb<TEntity>((long)id.ToInt());

        public TEntity SingleDb<TEntity>(LongId<TEntity> id) where TEntity : DbEntity, ILongId => SinglePrivateDb<TEntity>(id.ToLong());

        public IReadOnlyList<object> ManyDb(Type type, IReadOnlyCollection<int> ids)
        {
            return LoadWhere(type, "Id IN (SELECT Value FROM @0)", new object[] { ids });
        }

        public IReadOnlyList<object> ManyDb(Type type, IReadOnlyCollection<long> ids)
        {
            return LoadWhere(type, "Id IN (SELECT Value FROM @0)", new object[] { ids });
        }

        public IImmutableSet<TEntity> ManyDb<TEntity>(IReadOnlyCollection<Id<TEntity>> ids)
            where TEntity : DbEntity, IId
        {
            if (ids.NullOrNone())
            {
                return ImmutableHashSet<TEntity>.Empty;
            }

            return LoadWhere<TEntity>("Id IN (SELECT Value FROM @0)", new object[] { ids }).ToImmutableHashSet();
        }

        public IImmutableSet<TEntity> ManyDb<TEntity>(IReadOnlyCollection<LongId<TEntity>> ids)
            where TEntity : DbEntity, ILongId
        {
            if (ids.NullOrNone())
            {
                return ImmutableHashSet<TEntity>.Empty;
            }

            return LoadWhere<TEntity>("Id IN (SELECT Value FROM @0)", new object[] { ids }).ToImmutableHashSet();
        }

        MultipleResultSetReader IDbInternal.LoadMultipleResults(DbSqlCommand sqlCommand)
        {
            var multipleResults = new MultipleResultSetReader(sqlCommand, this);
            return multipleResults;
        }

        // Guess what, if you comment out IDisposable, everything compiles like a charm.
        public void Dispose()
        {
            currentOpenConnection?.Dispose();
        }

        public void RaiseNotifyMixedDb(MixedDbEventArgs mixedDbEventArgs)
        {
            NotifyMixedDb?.Invoke(this, mixedDbEventArgs);
        }
    }
}