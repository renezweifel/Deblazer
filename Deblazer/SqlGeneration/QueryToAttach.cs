using System;
using System.Collections.Generic;

namespace Dg.Deblazer.SqlGeneration
{
    public class QueryToAttach
    {
        public QueryBase QueryBase;
        public Func<QueryWrapper, IReadOnlyList<long>, QueryEl> QueryFilter;
        internal Func<DbEntity, long> GroupingKey;

        public Action<DbEntity, IReadOnlyList<DbEntity>> AttachEntitiesAction { get; internal set; }
    }
}