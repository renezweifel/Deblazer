using System;
using System.Collections.Generic;
using Dg.Deblazer.SqlGeneration;

namespace Dg.Deblazer.Internal
{
    internal interface IQueryInternal<TBack, TOriginal, TCurrent, TWrapper, TQuery>
        where TBack : QueryBase
        where TOriginal : DbEntity, ILongId
        where TCurrent : IQueryReturnType
        where TWrapper : QueryWrapper
        where TQuery : Query<TBack, TOriginal, TCurrent, TWrapper, TQuery>
    {
        IDbInternal Db { get; }
        int GlobalJoinCount { get; }

        QueryJoinElements<TQuery, TOriginal, Id<TOther>> JoinIds<TOther>(
            JoinType joinType,
            string memberName,
            IEnumerable<Id<TOther>> ids)
            where TOther : DbEntity, IId;

        QueryJoinElements<TQuery, TOriginal, LongId<TOther>> JoinIds<TOther>(
            JoinType joinType,
            string memberName,
            IEnumerable<LongId<TOther>> ids)
            where TOther : DbEntity, ILongId;

        QueryJoinElements<TQuery, TOriginal, TValue> Join<TValue>(
            JoinType joinType,
            string memberName,
            QueryJoinElements<TQuery, TOriginal, TValue> joinedQuery);

        TWrapper GetWrapper();
    }
}
