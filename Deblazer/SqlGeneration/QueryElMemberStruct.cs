using System;

namespace Dg.Deblazer.SqlGeneration
{
    public class QueryElMemberStruct<T> : QueryElMember<T> where T : struct
    {
        public QueryElMemberStruct(string c)
            : base(c)
        {
        }

        public static QueryElOperator<T> operator ==(QueryElMemberStruct<T> x, T? y)
        {
            return y != null ? new QueryElOperator<T>("({0}={1})", x, QueryConversionHelper.ConvertParameter(y)) : new QueryElOperator<T>("({0} IS NULL)", x);
        }

        public static QueryElOperator<T> operator !=(QueryElMemberStruct<T> x, T? y)
        {
            return y != null ? new QueryElOperator<T>("({0}<>{1})", x, QueryConversionHelper.ConvertParameter(y)) : new QueryElOperator<T>("({0} IS NOT NULL)", x);
        }

        public static QueryEl operator >(QueryElMemberStruct<T> x, T? y)
        {
            return y != null ? new QueryElOperator<T>("({0}>{1})", x, QueryConversionHelper.ConvertParameter(y)) : new QueryElOperator<T>("({0}>NULL)", x);
        }

        public static QueryEl operator <(QueryElMemberStruct<T> x, T? y)
        {
            return y != null ? new QueryElOperator<T>("({0}<{1})", x, QueryConversionHelper.ConvertParameter(y)) : new QueryElOperator<T>("({0}<NULL)", x);
        }

        public static QueryEl operator >=(QueryElMemberStruct<T> x, T? y)
        {
            return y != null ? new QueryElOperator<T>("({0}>={1})", x, QueryConversionHelper.ConvertParameter(y)) : new QueryElOperator<T>("({0}>=NULL)", x);
        }

        public static QueryEl operator <=(QueryElMemberStruct<T> x, T? y)
        {
            return y != null ? new QueryElOperator<T>("({0}<={1})", x, QueryConversionHelper.ConvertParameter(y)) : new QueryElOperator<T>("({0}<=NULL)", x);
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
    }
}