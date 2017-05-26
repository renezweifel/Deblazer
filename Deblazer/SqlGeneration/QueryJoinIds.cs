using System;
using System.Collections.Generic;
using System.Linq;

namespace Dg.Deblazer.SqlGeneration
{
    internal class QueryJoinIds<K, TOriginal, V> : QueryJoinElements<K, TOriginal, Id<V>>
        where K : QueryBase
        where TOriginal : DbEntity, ILongId
        where V : IId
    {
        internal QueryJoinIds(IDb db, IEnumerable<Id<V>> ids)
            : base(db, ids)
        {
        }

        protected override void SetJoinValue(IEnumerable<Id<V>> elements)
        {
            joinValue = elements.Select(e => (int)e);
        }
    }

    internal class QueryJoinLongIds<K, TOriginal, V> : QueryJoinElements<K, TOriginal, LongId<V>>
        where K : QueryBase
        where TOriginal : DbEntity, ILongId
        where V : ILongId
    {
        internal QueryJoinLongIds(IDb db, IEnumerable<LongId<V>> ids)
            : base(db, ids)
        {
        }

        protected override void SetJoinValue(IEnumerable<LongId<V>> elements)
        {
            joinValue = elements.Select(e => e.ToLong());
        }
    }
}