using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace Dg.Deblazer.Extensions
{
    public static class LinqExtensionsId
    {
        public static IReadOnlyList<Id<TIId>> ToTypedIdsList<TIId>(this IEnumerable<int> ids) where TIId : IId
        {
            return ids.Select(id => new Id<TIId>(id)).ToList();
        }

        public static IImmutableSet<Id<TIId>> ToTypedIdsSet<TIId>(this IEnumerable<int> ids) where TIId : IId
        {
            return ids.Select(id => new Id<TIId>(id)).ToImmutableHashSet();
        }

        public static IEnumerable<int> Except<TIId>(this IEnumerable<int> ids, IEnumerable<Id<TIId>> otherIds) where TIId : IId
        {
            return ids.Except(otherIds.CastToInt());
        }

        public static void AddRange<TIId>(this List<int> ids, IEnumerable<Id<TIId>> otherIds) where TIId : IId
        {
            ids.AddRange(otherIds.CastToInt());
        }

        public static IImmutableSet<int> ToIntReadOnlySet<TIId>(this IEnumerable<Id<TIId>> ids) where TIId : IId
        {
            return ids.CastToInt().ToImmutableHashSet();
        }

        public static List<int> ToIntList<TIId>(this IEnumerable<Id<TIId>> ids) where TIId : IId
        {
            return ids.CastToInt().ToList();
        }

        public static int[] ToIntArray<TIId>(this IEnumerable<Id<TIId>> ids) where TIId : IId
        {
            return ids.CastToInt().ToArray();
        }

        public static HashSet<int> ToIntDgHashSet<TIId>(this IEnumerable<Id<TIId>> ids) where TIId : IId
        {
            return new HashSet<int>(ids.CastToInt());
        }

        public static IEnumerable<int> CastToInt<TIId>(this IEnumerable<Id<TIId>> ids) where TIId : IId
        {
            return ids.Select(id => (int)id);
        }

        internal static void ForEach<T>(this IEnumerable<T> sequence, Action<T> action)
        {
            if (action == null)
            {
                throw new ArgumentNullException("action");
            }

            foreach (T item in sequence)
            {
                action(item);
            }
        }

        /// <summary>
        /// Returns if source is not null and source.Any()
        /// </summary>
        public static bool NotNullAndAny<T>(this IEnumerable<T> source)
        {
            return source != null && source.Any();
        }

        /// <summary>
        /// Returns if source is not null and source.Any()
        /// </summary>
        public static bool NotNullAndAny<T>(this IEnumerable<T> source, Func<T, bool> func)
        {
            return source != null && source.Any(func);
        }

        /// <summary>
        /// Returns if source is null or source.None() (== !source.NotNullAndAny())
        /// </summary>
        public static bool NullOrNone<T>(this IEnumerable<T> source)
        {
            return !NotNullAndAny(source);
        }

        /// <summary>
        /// Returns if source is null or source.None() (== !source.NotNullAndAny())
        /// </summary>
        public static bool NullOrNone<T>(this IEnumerable<T> source, Func<T, bool> func)
        {
            return !NotNullAndAny(source, func);
        }

        /// <summary>
        /// Splits the sequence in multiple batches. Each batch is buffered in a list so the batch can be iterated
        /// multiple times.
        /// </summary>
        internal static IEnumerable<IReadOnlyList<T>> Batch<T>(this IEnumerable<T> source, int batchSize)
        {
            using (var enumerator = source.GetEnumerator())
            {
                while (enumerator.MoveNext())
                {
                    yield return YieldBatchElements(enumerator, batchSize).ToList();
                }
            }
        }

        private static IEnumerable<T> YieldBatchElements<T>(
            IEnumerator<T> source,
            int batchSize)
        {
            yield return source.Current;
            int i = 1;
            while (i < batchSize && source.MoveNext())
            {
                i += 1;
                yield return source.Current;
            }
        }

        public static int GetCapacityOrZero<TSource>(this IEnumerable<TSource> source)
        {
            if (source is ICollection<TSource>)
            {
                return ((ICollection<TSource>)source).Count;
            }

            if (source is IReadOnlyCollection<TSource>)
            {
                return ((IReadOnlyCollection<TSource>)source).Count;
            }

            return 0;
        }
    }
}