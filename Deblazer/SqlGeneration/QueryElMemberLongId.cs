using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace Dg.Deblazer.SqlGeneration
{
    public class QueryElMemberLongId<TEntity> : QueryElMember<LongId<TEntity>> where TEntity : ILongId
    {
        public QueryElMemberLongId(string c) : base(c)
        {
        }

        public override string GetSql(SqlParameterCollection parameters, int joinCount)
        {
            return ((QueryElMember<long>)this).GetSql(parameters, joinCount);
        }

        public override bool Equals(object obj)
        {
            //
            // See the full list of guidelines at
            //   http://go.microsoft.com/fwlink/?LinkID=85237
            // and also the guidance for operator== at
            //   http://go.microsoft.com/fwlink/?LinkId=85238
            //
            throw new NotImplementedException();
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public static QueryElOperator<LongId<TEntity>> operator ==(QueryElMemberLongId<TEntity> x, long y)
        {
            return new QueryElOperator<LongId<TEntity>>("({0}={1})", x, QueryConversionHelper.ConvertParameter(y));
        }

        public static QueryElOperator<LongId<TEntity>> operator !=(QueryElMemberLongId<TEntity> x, long y)
        {
            return new QueryElOperator<LongId<TEntity>>("({0}<>{1})", x, QueryConversionHelper.ConvertParameter(y));
        }

        public static QueryElOperator<LongId<TEntity>> operator ==(QueryElMemberLongId<TEntity> x, LongId<TEntity>? y)
        {
            return y.HasValue ? x == y.Value.ToLong() : new QueryElOperator<LongId<TEntity>>("({0} IS NULL)", x);
        }

        public static QueryElOperator<LongId<TEntity>> operator !=(QueryElMemberLongId<TEntity> x, LongId<TEntity>? y)
        {
            return y.HasValue ? x != y.Value.ToLong() : new QueryElOperator<LongId<TEntity>>("({0} IS NOT NULL)", x);
        }

        public QueryEl In(params long[] values)
        {
            if (values == null
                || values.Length == 0)
            {
                // Always false
                return new QueryElBool(false);
            }

            return In(values.Select(id => id as long?));
        }

        public QueryEl In(IEnumerable<long?> values)
        {
            if (values == null
                || values.Count() == 0)
            {
                // Always false
                return new QueryElBool(false);
            }

            return In(values.Where(v => v.HasValue).Select(v => new LongId<TEntity>(v.Value)));
        }

        public QueryEl In(IEnumerable<long> ids)
        {
            if (ids == null
                || ids.Count() == 0)
            {
                // Always false
                return new QueryElBool(false);
            }

            return In(ids.Select(i => new LongId<TEntity>(i)));
        }

        public static QueryElOperator<LongId<TEntity>> operator <(QueryElMemberLongId<TEntity> x, long y)
        {
            return new QueryElOperator<LongId<TEntity>>("({0}<{1})", x, QueryConversionHelper.ConvertParameter(y));
        }

        public static QueryElOperator<LongId<TEntity>> operator <=(QueryElMemberLongId<TEntity> x, long y)
        {
            return new QueryElOperator<LongId<TEntity>>("({0}<={1})", x, QueryConversionHelper.ConvertParameter(y));
        }

        public static QueryElOperator<LongId<TEntity>> operator >(QueryElMemberLongId<TEntity> x, long y)
        {
            return new QueryElOperator<LongId<TEntity>>("({0}>{1})", x, QueryConversionHelper.ConvertParameter(y));
        }

        public static QueryElOperator<LongId<TEntity>> operator >=(QueryElMemberLongId<TEntity> x, long y)
        {
            return new QueryElOperator<LongId<TEntity>>("({0}>={1})", x, QueryConversionHelper.ConvertParameter(y));
        }

        public static QueryElOperator<LongId<TEntity>> operator <(QueryElMemberLongId<TEntity> x, LongId<TEntity>? y)
        {
            return y.HasValue ? x < y.Value.ToLong() : new QueryElOperator<LongId<TEntity>>("({0} < NULL)", x);
        }

        public static QueryElOperator<LongId<TEntity>> operator <=(QueryElMemberLongId<TEntity> x, LongId<TEntity>? y)
        {
            return y.HasValue ? x <= y.Value.ToLong() : new QueryElOperator<LongId<TEntity>>("({0} <= NULL)", x);
        }

        public static QueryElOperator<LongId<TEntity>> operator >(QueryElMemberLongId<TEntity> x, LongId<TEntity>? y)
        {
            return y.HasValue ? x > y.Value.ToLong() : new QueryElOperator<LongId<TEntity>>("({0} > NULL)", x);
        }

        public static QueryElOperator<LongId<TEntity>> operator >=(QueryElMemberLongId<TEntity> x, LongId<TEntity>? y)
        {
            return y.HasValue ? x >= y.Value.ToLong() : new QueryElOperator<LongId<TEntity>>("({0} >= NULL)", x);
        }

        public static implicit operator QueryElMember<long>(QueryElMemberLongId<TEntity> x)
        {
            return new QueryElMember<long>(x.MemberName);
        }
    }
}