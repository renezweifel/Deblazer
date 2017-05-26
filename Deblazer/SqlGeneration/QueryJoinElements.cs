using System.Collections.Generic;
using System.Linq;
using Dg.Deblazer.Extensions;

namespace Dg.Deblazer.SqlGeneration
{
    public class QueryJoinElements<K, TOriginal, TValue> : Query<K, TOriginal, JoinElements<TValue>, JoinElementsWrapper<TValue>, QueryJoinElements<K, TOriginal, TValue>>
        where K : QueryBase
        where TOriginal : DbEntity, ILongId
    {
        private static readonly JoinElementsWrapper<TValue> joinElementsWrapper = new JoinElementsWrapper<TValue>();

        internal QueryJoinElements(IDb db, IEnumerable<TValue> elements)
            : base(db)
        {
            SetJoinValue(elements);
        }

        protected virtual void SetJoinValue(IEnumerable<TValue> elements)
        {
            if (typeof(int) == typeof(TValue)
                || typeof(int?) == typeof(TValue)
                || typeof(string) == typeof(TValue))
            {
                joinValue = elements;
            }
            else if (typeof(IConvertibleToInt32).IsAssignableFrom(typeof(TValue)))
            {
                joinValue = elements.Cast<IConvertibleToInt32>().Select(i => i.ConvertToInt32());
            }
            else
            {
                joinValue = string.Join(",",  elements.Select(e => e.ToString()));
            }
        }

        protected override JoinElementsWrapper<TValue> GetWrapper() => joinElementsWrapper;
    }
}