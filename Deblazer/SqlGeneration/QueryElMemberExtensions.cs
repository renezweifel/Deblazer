using System.Collections.Generic;
using System.Linq;

namespace Dg.Deblazer.SqlGeneration
{
    public static class QueryElMemberExtensions
    {
        public static QueryEl In<T>(this QueryElMember<T?> queryElMember, IEnumerable<T> values) where T : struct
        {
            return queryElMember.In(values?.Cast<T?>());
        }

        public static QueryElOperator<int> Minus(this QueryElMemberStruct<int> minuend, QueryElMemberStruct<int> subtrahend)
        {
            return new QueryElOperator<int>("({0} - {1})", minuend, subtrahend);
        }

        public static QueryElOperator<int> Plus(this QueryElMemberStruct<int> firstSummand, QueryElMemberStruct<int> secondSummand)
        {
            return new QueryElOperator<int>("({0} + {1})", firstSummand, secondSummand);
        }
    }
}