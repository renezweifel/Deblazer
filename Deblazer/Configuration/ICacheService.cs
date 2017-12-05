using System;
using Dg.Deblazer.Cache;
using System.Data;

namespace Dg.Deblazer.Configuration
{
    public interface ICacheService
    {
        DbEntityRefCached<TEntity> GetDbEntityRefCached<TEntity>(long entityId) where TEntity : DbEntity;
        bool IsInitialUpdate(byte[] lastRowVersion);

        ICachedEntityList<TEntity> GetCachedEntities<TSource, TEntity>(
            Func<long?> getForeignKey)
            where TSource : DbEntity
            where TEntity : DbEntity;
    }
}