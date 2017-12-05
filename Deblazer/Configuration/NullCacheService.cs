using System;
using System.Data;
using Dg.Deblazer.Cache;

namespace Dg.Deblazer.Configuration
{
    internal class NullCacheService : ICacheService
    {
        public static readonly NullCacheService Instance = new NullCacheService();

        private NullCacheService()
        {
        }

        public ICachedEntityList<TEntity> GetCachedEntities<TSource, TEntity>(Func<long?> getForeignKey)
            where TSource : DbEntity
            where TEntity : DbEntity
        {
            throw new NotSupportedException("Cache is not supported with the current " + nameof(DbConfiguration));
        }

        public DbEntityRefCached<TEntity> GetDbEntityRefCached<TEntity>(long entityId) where TEntity : DbEntity
        {
            // We can't provide any meaningful logic here. Throw an exception to at least avoid a lot of wasted developer hours searching for bugs.
            throw new NotSupportedException("Cache is not supported with the current " + nameof(DbConfiguration));
        }

        public bool IsInitialUpdate(byte[] lastRowVersion)
        {
            return false;
        }
    }
}
