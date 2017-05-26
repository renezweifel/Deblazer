using Dg.Deblazer.Settings;
using Dg.Deblazer.SqlGeneration;
using Dg.Deblazer.SqlUtils;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data.SqlClient;
using System.Linq.Expressions;

namespace Dg.Deblazer
{
    public interface IDb
    {
        IDbSettings Settings { get; }

        event Action<IReadOnlyList<DbEntity>> EntitiesLoaded;

        event MixedDbEventHandler NotifyMixedDb;

        IIdQuery<T> GetIIdTable<T>() where T : DbEntity, IId;
        ILongIdQuery<T> GetILongIdTable<T>() where T : DbEntity, ILongId;
        QuerySql<T, T, T> Load<T>(SqlCommand sqlCommand);
        QuerySql<T, T, T> Load<T>(DbSqlCommand sqlCommand);
        QuerySql<T, T, T> Load<T>(string sql, params object[] values);
        IReadOnlyList<object> ManyDb(Type type, IReadOnlyCollection<int> ids);
        IReadOnlyList<object> ManyDb(Type type, IReadOnlyCollection<long> ids);
        IImmutableSet<TEntity> ManyDb<TEntity>(IReadOnlyCollection<Id<TEntity>> ids) where TEntity : DbEntity, IId;
        IImmutableSet<TEntity> ManyDb<TEntity>(IReadOnlyCollection<LongId<TEntity>> ids) where TEntity : DbEntity, ILongId;
        DbEntity SingleDb(Type type, long id);
        TEntity SingleDb<TEntity>(Id<TEntity> id) where TEntity : DbEntity, IId;
        TEntity SingleDb<TEntity>(LongId<TEntity> id) where TEntity : DbEntity, ILongId;
        DbEntity SingleOrDefaultDb(Type type, long id);
        DbEntity SingleOrDefaultDb(string typeName, long id);
        TEntity SingleOrDefaultDb<TEntity>(Id<TEntity> id) where TEntity : DbEntity, IId;
        TEntity SingleOrDefaultDb<TEntity>(LongId<TEntity> id) where TEntity : DbEntity, ILongId;
        T SingleOrDefaultDb<T>(DbSqlCommand sqlCommand);
        TSelected SelectSingleOrDefaultDb<TSelected, TEntity>(Id<TEntity> entityId, Expression<Func<TEntity, TSelected>> selector) where TEntity : IId;
        TSelected SelectSingleDb<TSelected, TEntity>(Id<TEntity> entityId, Expression<Func<TEntity, TSelected>> selector) where TEntity : IId;
    }
}