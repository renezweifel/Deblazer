using Dg.Deblazer;
using Dg.Deblazer.Extensions;
using JetBrains.Annotations;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace System
{
    public static class IIdExtensions
    {
        /// <summary>
        /// OBSOLETE: Use iid?.Id instead
        /// </summary>
        [CanBeNull]
        public static int? GetIdOrNull([CanBeNull]this IId iid)
        {
            return iid?.Id;
        }

        // ENG-222 -> remove in long term
        public static IReadOnlyList<int> GetIdsAsList(this IEnumerable<IId> iids)
        {
            return iids
                .Where(i => i != null)
                .Select(i => i.Id)
                .ToList();
        }

        

        public static Dictionary<int, TEntity> ToDictionaryById<TEntity>(this IEnumerable<TEntity> source, int? initCapacity = null) where TEntity : class, IId
        {
            var dictionary = new Dictionary<int, TEntity>(initCapacity ?? source.GetCapacityOrZero());
            source.ForEach(e => dictionary.Add(e.Id, e));
            return dictionary;
        }

        public static Id<TEntity> GetTypedId<TEntity>([NotNull]this TEntity iid) where TEntity : class, IId
        {
            return ToTypedId<TEntity>(iid.Id);
        }

        public static IReadOnlyList<Id<TEntity>> GetTypedIdsAsList<TEntity>(this IEnumerable<TEntity> iids) where TEntity : class, IId
        {
            return iids
                .Where(i => i != null)
                .Select(GetTypedId)
                .ToList();
        }

        public static IImmutableSet<Id<TEntity>> GetTypedIdsAsSet<TEntity>(this IEnumerable<TEntity> iids) where TEntity : class, IId
        {
            return iids
                .Where(i => i != null)
                .Select(GetTypedId)
                .ToImmutableHashSet();
        }

        public static Dictionary<Id<TEntity>, TEntity> ToDictionaryByTypedId<TEntity>(this IEnumerable<TEntity> source, int? initCapacity = null) where TEntity : class, IId
        {
            var dictionary = new Dictionary<Id<TEntity>, TEntity>(initCapacity ?? source.GetCapacityOrZero());
            source.ForEach(e => dictionary.Add(GetTypedId(e), e));
            return dictionary;
        }

        public static Id<TIId> ToTypedId<TIId>(this int id) where TIId : class, IId
        {
            return new Id<TIId>(id);
        }
    }
}