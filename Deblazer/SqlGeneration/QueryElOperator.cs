using System;
using System.Data.SqlClient;

namespace Dg.Deblazer.SqlGeneration
{
    public class QueryElOperator<T> : QueryEl
    {
        private readonly QueryEl[] queryEls;
        private readonly string sql;

        public QueryElOperator(string sql, params QueryEl[] operands)
        {
            this.sql = sql;
            queryEls = operands;
        }

        public override string GetSql(SqlParameterCollection parameters, int joinCount)
        {
            string[] strs = new string[queryEls.Length];
            for (int i = 0; i < queryEls.Length; i++)
            {
                strs[i] = queryEls[i].GetSql(parameters, joinCount);
            }

            return string.Format(sql, strs);
        }

        public static QueryElOperator<T> operator ==(QueryElOperator<T> x, T y)
        {
            return y != null ? new QueryElOperator<T>("({0}={1})", x, QueryConversionHelper.ConvertParameter(y)) : new QueryElOperator<T>("({0} IS NULL)", x);
        }

        public static QueryElOperator<T> operator !=(QueryElOperator<T> x, T y)
        {
            return y != null ? new QueryElOperator<T>("({0}<>{1})", x, QueryConversionHelper.ConvertParameter(y)) : new QueryElOperator<T>("({0} IS NULL)", x);
        }

        public static QueryElOperator<T> operator *(QueryElOperator<T> x, QueryElOperator<T> y)
        {
            return new QueryElOperator<T>("({0}*{1})", x, y);
        }

        public override bool Equals(object obj)
        {
            throw new NotSupportedException();
        }

        public override int GetHashCode()
        {
            throw new NotSupportedException();
        }
    }
}