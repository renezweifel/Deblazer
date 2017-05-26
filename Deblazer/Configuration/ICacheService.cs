using System;
using System.Data.Linq;
using Dg.Deblazer.Cache;

namespace Dg.Deblazer.Configuration
{
    public interface ICacheService
    {
        DbEntityRefCached<TEntity> GetDbEntityRefCached<TEntity>(long entityId) where TEntity : DbEntity;
        bool IsInitialUpdate(Binary lastRowVersion);

        ICachedEntityList<TEntity> GetCachedEntities<TSource, TEntity>(
            Func<long?> getForeignKey)
            where TSource : DbEntity
            where TEntity : DbEntity;
    }
}