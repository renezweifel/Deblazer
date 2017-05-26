using System;
using System.Collections.Concurrent;
using Dg.Deblazer.Settings;

namespace Dg.Deblazer.Cache
{
    public class LoadedEntityCache
    {
        private readonly IDbSettings settings;

        // Should be ConcurrentDictionary<TypeAndMemberName, ConcurrentSet<object>> but there is no ConcurrentSet in the BCL
        private readonly ConcurrentDictionary<Type, ConcurrentDictionary<IdAndMemberName, object>> cache;

        public LoadedEntityCache(IDbSettings settings)
        {
            this.settings = settings;
            this.cache = new ConcurrentDictionary<Type, ConcurrentDictionary<IdAndMemberName, object>>();
        }

        public bool TryGet<TValue>(string memberName, long id, out TValue value)
        {
            object valueObj;
            bool success = TryGet(typeof(TValue), memberName, id, out valueObj);
            value = (TValue)valueObj;
            return success;
        }

        public bool TryGet(Type type, string memberName, long id, out object value)
        {
            return TryGet(type, new IdAndMemberName(id, memberName), out value);
        }

        public bool TryGet<TValue>(string memberName, string referenceKey, out TValue value)
        {
            object valueObj;
            bool success = TryGet(typeof(TValue), memberName, referenceKey, out valueObj);
            value = (TValue)valueObj;
            return success;
        }

        public bool TryGet(Type type, string memberName, string referenceKey, out object value)
        {
            return TryGet(type, new IdAndMemberName(referenceKey, memberName), out value);
        }

        private bool TryGet(Type type, IdAndMemberName idAndMemberName, out object value)
        {
            if (!settings.EnableCache && !ReturnPreviouslyLoadedEntity)
            {
                value = null;
                return false;
            }

            ConcurrentDictionary<IdAndMemberName, object> entitiesByIdAndMemberName;
            if (cache.TryGetValue(type, out entitiesByIdAndMemberName))
            {
                return entitiesByIdAndMemberName.TryGetValue(idAndMemberName, out value);
            }

            value = null;
            return false;
        }

        public TEntity GetOrAdd<TEntity>(string memberName, long id, TEntity entity)
            where TEntity : DbEntity
        {
            return (TEntity)GetOrAdd(typeof(TEntity), memberName, id, entity);
        }

        public object GetOrAdd(Type type, string memberName, long id, object entity)
        {
            return GetOrAdd(type, new IdAndMemberName(id, memberName), entity);
        }

        public TEntity GetOrAdd<TEntity>(string memberName, string referenceKey, TEntity entity)
            where TEntity : DbEntity
        {
            return (TEntity)GetOrAdd(typeof(TEntity), memberName, referenceKey, entity);
        }

        public object GetOrAdd(Type type, string memberName, string referenceKey, object entity)
        {
            return GetOrAdd(type, new IdAndMemberName(referenceKey, memberName), entity);
        }

        private object GetOrAdd(Type type, IdAndMemberName idAndMemberName, object entity)
        {
            if (!settings.EnableCache && !ReturnPreviouslyLoadedEntity)
            {
                return entity;
            }

            var entitiesByIdAndMemberName = cache.GetOrAdd(type, t => new ConcurrentDictionary<IdAndMemberName, object>(concurrencyLevel: 2, capacity: 1));
            object entityFromCache = entitiesByIdAndMemberName.GetOrAdd(idAndMemberName, k => entity);

            return ReturnPreviouslyLoadedEntity ? entityFromCache : entity;
        }

        public bool TryRemoveFromCache(Type type)
        {
            ConcurrentDictionary<IdAndMemberName, object> removed;
            return cache.TryRemove(type, out removed);
        }

        public void ClearCache()
        {
            cache.Clear();
        }

        private bool ReturnPreviouslyLoadedEntity => settings is WriteDbSettings ? ((WriteDbSettings)settings).ReturnPreviouslyLoadedEntity_Obsolete : false;

        private struct IdAndMemberName : IEquatable<IdAndMemberName>
        {
            public readonly long Id;
            public readonly string ReferenceKey;
            public readonly string MemberName;

            public IdAndMemberName(long id, string memberName)
            {
                this.Id = id;
                this.ReferenceKey = null;
                this.MemberName = memberName;
            }

            public IdAndMemberName(string referenceKey, string memberName)
            {
                this.ReferenceKey = referenceKey;
                this.MemberName = memberName;
                this.Id = default(int);
            }

            public override int GetHashCode()
            {
                return Id.GetHashCode()
                    ^ (ReferenceKey != null ? ReferenceKey.GetHashCode() : 0)
                    ^ (MemberName != null ? MemberName.GetHashCode() : 0);
            }

            public override bool Equals(object obj)
            {
                var idAndMemberName = obj as IdAndMemberName?;
                if (idAndMemberName == null)
                {
                    return false;
                }

                return Equals(this, idAndMemberName.Value);
            }

            public bool Equals(IdAndMemberName other)
            {
                return Equals(this, other);
            }

            public static bool Equals(IdAndMemberName first, IdAndMemberName second)
            {
                return first.Id == second.Id && first.ReferenceKey == second.ReferenceKey && first.MemberName == second.MemberName;
            }
        }
    }
}