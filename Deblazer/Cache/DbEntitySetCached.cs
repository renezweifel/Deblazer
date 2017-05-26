using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using Dg.Deblazer.Api;
using Dg.Deblazer.Configuration;
using Dg.Deblazer.Internal;

namespace Dg.Deblazer.Cache
{
    public class DbEntitySetCached<TSource, TEntity> : IDbEntitySet<TEntity>, IDbEntitySetInternal
        where TSource : DbEntity
        where TEntity : DbEntity
    {
        private readonly Func<long?> getForeignKey;

        private ICachedEntityList<TEntity> cachedEntities;

        public DbEntitySetCached(Func<long?> getForeignKey)
        {
            this.getForeignKey = getForeignKey;
        }

        private IList<TEntity> Values
        {
            get
            {
                if (cachedEntities == null)
                {
                    var cacheService = GlobalDbConfiguration.GetConfigurationOrEmpty(typeof(TEntity)).CacheService;
                    cachedEntities = cacheService.GetCachedEntities<TSource, TEntity>(getForeignKey);
                    if (cachedEntities == null)
                    {
                        throw new InvalidOperationException($"{typeof(TSource).Name}.{typeof(TEntity).Name}s are not present in the cache");
                    }
                }

                // All implementations of ICachedEntityList<T> also implement IList<T> therefore the cast is safe. ICachedEntityList is covariant and thus
                // can not require IList to be implemented.
                return (IList<TEntity>)cachedEntities;
            }
        }

        public long? ForeignKey => getForeignKey();

        public IEnumerable<TEntity> ValuesNoLoad => cachedEntities;

        public IEnumerator<TEntity> GetEnumerator() => Values.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public void Add(TEntity entity)
        {
            throw new NotSupportedException();
        }

        public void AddRange(IEnumerable<TEntity> entities)
        {
            throw new NotSupportedException();
        }

        public int IndexOf(TEntity item) => Values.IndexOf(item);

        public void Insert(int index, TEntity item)
        {
            throw new NotSupportedException();
        }

        public void RemoveAt(int index)
        {
            throw new NotSupportedException();
        }

        public TEntity this[int index]
        {
            get { return Values[index]; }

            set { throw new NotSupportedException(); }
        }

        public void Clear()
        {
            throw new NotSupportedException();
        }

        public bool Contains(TEntity item) => Values.Contains(item);

        public void CopyTo(TEntity[] array, int arrayIndex) => Values.CopyTo(array, arrayIndex);

        public int Count => Values.Count;

        public bool IsReadOnly => true;

        public bool Remove(TEntity item)
        {
            throw new NotSupportedException();
        }

        public void Attach(IEnumerable<TEntity> entities)
        {
            throw new NotSupportedException();
        }

        IReadOnlyList<DbEntity> IDbEntitySetInternal.EntitiesInternal => cachedEntities;

        bool IDbEntitySetInternal.IsForeignKey => false;

        void IDbEntitySetInternal.SetDb(BaseDb db) { }

        void IDbEntitySetInternal.DisableLazyLoadChildren() { }

        void IDbEntitySetInternal.EnableLoadingChildrenFromCache() { }

        void IDbEntitySetInternal.MakeReadOnly() { }

        bool IRaiseDbSubmitEvent.RaiseBeforeInsertEvent() => false;

        IImmutableSet<DbEntity> IRaiseDbSubmitEvent.RaiseAfterInsertEvent() => ImmutableHashSet<DbEntity>.Empty;

        bool IRaiseDbSubmitEvent.RaiseBeforeDeleteEvent() => false;

        bool IRaiseDbSubmitEvent.RaiseBeforeUpdateEvent() => false;

        bool IRaiseDbSubmitEvent.RaiseAfterDeleteEvent() => false;

        bool IRaiseDbSubmitEvent.RaiseOnSubmitTransactionAbortedEvent() => false;
    }
}