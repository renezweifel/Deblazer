using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace Dg.Deblazer.SqlGeneration
{
    public class QueryElMemberId<TEntity> : QueryElMember<Id<TEntity>> where TEntity : IId
    {
        public QueryElMemberId(string c) : base(c)
        {
        }

        public override string GetSql(SqlParameterCollection parameters, int joinCount)
        {
            return ((QueryElMember<int>)this).GetSql(parameters, joinCount);
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

        public static QueryElOperator<Id<TEntity>> operator ==(QueryElMemberId<TEntity> x, int y)
        {
            return new QueryElOperator<Id<TEntity>>("({0}={1})", x, QueryConversionHelper.ConvertParameter(y));
        }

        public static QueryElOperator<Id<TEntity>> operator !=(QueryElMemberId<TEntity> x, int y)
        {
            return new QueryElOperator<Id<TEntity>>("({0}<>{1})", x, QueryConversionHelper.ConvertParameter(y));
        }

        public static QueryElOperator<Id<TEntity>> operator ==(QueryElMemberId<TEntity> x, Id<TEntity>? y)
        {
            return y.HasValue ? x == (int)y : new QueryElOperator<Id<TEntity>>("({0} IS NULL)", x);
        }

        public static QueryElOperator<Id<TEntity>> operator !=(QueryElMemberId<TEntity> x, Id<TEntity>? y)
        {
            return y.HasValue ? x != (int)y : new QueryElOperator<Id<TEntity>>("({0} IS NOT NULL)", x);
        }

        public QueryEl In(params int[] values)
        {
            if (values == null
                || values.Length == 0)
            {
                // Always false
                return new QueryElBool(false);
            }

            return In(values.Select(id => new Id<TEntity>?(id)));
        }

        public QueryEl In(IEnumerable<int?> values)
        {
            if (values == null
                || values.Count() == 0)
            {
                // Always false
                return new QueryElBool(false);
            }

            return In(values.Where(v => v.HasValue).Select(v => (Id<TEntity>)v.Value));
        }

        public QueryEl In(IEnumerable<int> ids)
        {
            if (ids == null
                || ids.Count() == 0)
            {
                // Always false
                return new QueryElBool(false);
            }

            return In(ids.Select(i => (Id<TEntity>)i));
        }

        public static QueryElOperator<Id<TEntity>> operator <(QueryElMemberId<TEntity> x, int y)
        {
            return new QueryElOperator<Id<TEntity>>("({0}<{1})", x, QueryConversionHelper.ConvertParameter(y));
        }

        public static QueryElOperator<Id<TEntity>> operator <=(QueryElMemberId<TEntity> x, int y)
        {
            return new QueryElOperator<Id<TEntity>>("({0}<={1})", x, QueryConversionHelper.ConvertParameter(y));
        }

        public static QueryElOperator<Id<TEntity>> operator >(QueryElMemberId<TEntity> x, int y)
        {
            return new QueryElOperator<Id<TEntity>>("({0}>{1})", x, QueryConversionHelper.ConvertParameter(y));
        }

        public static QueryElOperator<Id<TEntity>> operator >=(QueryElMemberId<TEntity> x, int y)
        {
            return new QueryElOperator<Id<TEntity>>("({0}>={1})", x, QueryConversionHelper.ConvertParameter(y));
        }

        public static QueryElOperator<Id<TEntity>> operator <(QueryElMemberId<TEntity> x, Id<TEntity>? y)
        {
            return y.HasValue ? x < (int)y : new QueryElOperator<Id<TEntity>>("({0} < NULL)", x);
        }

        public static QueryElOperator<Id<TEntity>> operator <=(QueryElMemberId<TEntity> x, Id<TEntity>? y)
        {
            return y.HasValue ? x <= (int)y : new QueryElOperator<Id<TEntity>>("({0} <= NULL)", x);
        }

        public static QueryElOperator<Id<TEntity>> operator >(QueryElMemberId<TEntity> x, Id<TEntity>? y)
        {
            return y.HasValue ? x > (int)y : new QueryElOperator<Id<TEntity>>("({0} > NULL)", x);
        }

        public static QueryElOperator<Id<TEntity>> operator >=(QueryElMemberId<TEntity> x, Id<TEntity>? y)
        {
            return y.HasValue ? x >= (int)y : new QueryElOperator<Id<TEntity>>("({0} >= NULL)", x);
        }

        public static implicit operator QueryElMember<int>(QueryElMemberId<TEntity> x)
        {
            return new QueryElMember<int>(x.MemberName);
        }
    }
}