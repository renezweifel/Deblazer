using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using Dg.Deblazer.Utils;
using Dg.Deblazer.Extensions;

namespace Dg.Deblazer.SqlGeneration
{
    public class QueryElMember<T> : QueryEl
    {
        public readonly string MemberName;
        private readonly string sql;

        public QueryElMember(string c)
        {
            MemberName = c;
            sql = "[t{0}].[" + c + "]";
        }

        public override string GetSql(SqlParameterCollection parameters, int joinCount)
        {
            var str = string.Format(sql, joinCount);

            return str;
        }

        public override string ToString()
        {
            return GetSql(parameters: null, joinCount: -1);
        }

        public static QueryElOperator<T> operator ==(QueryElMember<T> x, T y)
        {
            return y != null ? new QueryElOperator<T>("({0}={1} AND {0} IS NOT NULL)", x, QueryConversionHelper.ConvertParameter(y)) : new QueryElOperator<T>("({0} IS NULL)", x);
        }

        public static QueryElOperator<T> operator !=(QueryElMember<T> x, T y)
        {
            return y != null ? new QueryElOperator<T>("({0}<>{1})", x, QueryConversionHelper.ConvertParameter(y)) : new QueryElOperator<T>("({0} IS NOT NULL)", x);
        }

        public QueryElOperator<T> Equals(QueryElMember<T> y)
        {
            return new QueryElOperator<T>("({0}={1})", this, QueryConversionHelper.ConvertParameter(y));
        }

        public QueryElOperator<T> Equals(QueryElOperator<T> y)
        {
            return new QueryElOperator<T>("({0}={1})", this, QueryConversionHelper.ConvertParameter(y));
        }

        public static QueryElOperator<T> operator >=(QueryElMember<T> x, QueryElOperator<T> y)
        {
            return new QueryElOperator<T>("({0}>={1})", x, QueryConversionHelper.ConvertParameter(y));
        }

        public static QueryElOperator<T> operator <=(QueryElMember<T> x, QueryElOperator<T> y)
        {
            return new QueryElOperator<T>("({0}<={1})", x, QueryConversionHelper.ConvertParameter(y));
        }

        public static QueryElOperator<T> operator <(QueryElMember<T> x, QueryElOperator<T> y)
        {
            return new QueryElOperator<T>("({0}<{1})", x, QueryConversionHelper.ConvertParameter(y));
        }

        public static QueryElOperator<T> operator >(QueryElMember<T> x, QueryElOperator<T> y)
        {
            return new QueryElOperator<T>("({0}>{1})", x, QueryConversionHelper.ConvertParameter(y));
        }

        public QueryElOperator<T> NotEquals(QueryElMember<T> y)
        {
            return new QueryElOperator<T>("({0}<>{1})", this, QueryConversionHelper.ConvertParameter(y));
        }

        public QueryElOperator<T> NotEquals(QueryElOperator<T> y)
        {
            return new QueryElOperator<T>("({0}<>{1})", this, QueryConversionHelper.ConvertParameter(y));
        }

        public static QueryElOperator<T> operator >(QueryElMember<T> x, T y)
        {
            return GetFalseQueryElOperatorOrNull(y) ?? new QueryElOperator<T>("({0}>{1})", x, QueryConversionHelper.ConvertParameter(y));
        }

        private static QueryElOperator<T> GetFalseQueryElOperatorOrNull(T y)
        {
            if (y == null)
            {
                // Can never be true
                return new QueryElOperator<T>("(0 = 1)");
            }

            return null;
        }

        public static QueryElOperator<T> operator >(QueryElMember<T> x, QueryElMember<T> y)
        {
            return new QueryElOperator<T>("({0}>{1})", x, QueryConversionHelper.ConvertParameter(y));
        }

        public static QueryElOperator<T> operator <(QueryElMember<T> x, T y)
        {
            return GetFalseQueryElOperatorOrNull(y) ?? new QueryElOperator<T>("({0}<{1})", x, QueryConversionHelper.ConvertParameter(y));
        }

        public static QueryElOperator<T> operator <(QueryElMember<T> x, QueryElMember<T> y)
        {
            return new QueryElOperator<T>("({0}<{1})", x, QueryConversionHelper.ConvertParameter(y));
        }

        public static QueryElOperator<T> operator >=(QueryElMember<T> x, T y)
        {
            return GetFalseQueryElOperatorOrNull(y) ?? new QueryElOperator<T>("({0}>={1})", x, QueryConversionHelper.ConvertParameter(y));
        }

        public static QueryElOperator<T> operator >=(QueryElMember<T> x, QueryElMember<T> y)
        {
            return new QueryElOperator<T>("({0}>={1})", x, QueryConversionHelper.ConvertParameter(y));
        }

        public static QueryElOperator<T> operator <=(QueryElMember<T> x, T y)
        {
            return GetFalseQueryElOperatorOrNull(y) ?? new QueryElOperator<T>("({0}<={1})", x, QueryConversionHelper.ConvertParameter(y));
        }

        public static QueryElOperator<T> operator <=(QueryElMember<T> x, QueryElMember<T> y)
        {
            return new QueryElOperator<T>("({0}<={1})", x, QueryConversionHelper.ConvertParameter(y));
        }

        public static QueryElOperator<T> operator +(QueryElMember<T> x, T y)
        {
            return new QueryElOperator<T>("({0}+{1})", x, QueryConversionHelper.ConvertParameter(y));
        }

        public static QueryElOperator<T> operator -(QueryElMember<T> x, T y)
        {
            return new QueryElOperator<T>("({0}-{1})", x, QueryConversionHelper.ConvertParameter(y));
        }

        public static QueryElOperator<T> operator *(QueryElMember<T> x, T y)
        {
            return new QueryElOperator<T>("({0}*{1})", x, QueryConversionHelper.ConvertParameter(y));
        }

        public static QueryElOperator<T> operator /(QueryElMember<T> x, T y)
        {
            return new QueryElOperator<T>("({0}/{1})", x, QueryConversionHelper.ConvertParameter(y));
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

        // Do not delete (It's a trap) This will break the build. Listen to MC Hammer.
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public QueryElOperator<T> Contains(string value)
        {
            return new QueryElOperator<T>("({0} LIKE {1})", this, new QueryElParameter<string>("%" + value + "%"));
        }

        public QueryElOperator<T> FullTextContainsWordsStartingWith(string value)
        {
            return new QueryElOperator<T>("CONTAINS({0}, {1})", this, new QueryElParameter<string>(FullTextUtils.GetFullTextStartsWithQuery(value)));
        }

        public QueryElOperator<T> FullTextContainsWords(string value)
        {
            return new QueryElOperator<T>("CONTAINS({0}, {1})", this, new QueryElParameter<string>(FullTextUtils.GetFullTextContainsExactWordWithQuery(value)));
        }

        public QueryElOperator<T> Contains(T value, char escapeCharacter)
        {
            return new QueryElOperator<T>(
                "({0} LIKE {1} ESCAPE {2})",
                this,
                new QueryElParameter<string>("%" + value + "%"),
                new QueryElParameter<char>(escapeCharacter));
        }

        public QueryElOperator<T> Matches(T regex)
        {
            return new QueryElOperator<T>("({0} LIKE {1})", this, new QueryElParameter<T>(regex));
        }

        // public QueryElBinaryOperator Matches(string regex, char escapeCharacter)
        // {
        //    return new QueryElBinaryOperator("({0} LIKE {1} ESCAPE {2})", this, new QueryElParameter<string>(regex), escapeCharacter);
        // }

        public QueryElOperator<T> EndsWith(T value)
        {
            return new QueryElOperator<T>("({0} LIKE {1})", this, new QueryElParameter<string>("%" + value));
        }

        public QueryElOperator<T> IfNull(T alternativeValue)
        {
            return new QueryElOperator<T>("ISNULL({0}, {1})", this, new QueryElParameter<T>(alternativeValue));
        }

        public QueryElOperator<T> IfNull(QueryElMember<T> alternativeMember)
        {
            return new QueryElOperator<T>("ISNULL({0}, {1})", this, alternativeMember);
        }

        public QueryElOperator<T> IfFalse(T comparisonValue, T fallbackValue)
        {
            return new QueryElOperator<T>(
                "(CASE WHEN {0} = {1} THEN {0} ELSE {2} END)",
                this,
                new QueryElParameter<T>(comparisonValue),
                new QueryElParameter<T>(fallbackValue));
        }

        public QueryElOperator<T> IfElse<TOtherElement>(T comparisonValue, QueryElMember<TOtherElement> whenTrueEl, TOtherElement whenFalseValue)
        {
            if (comparisonValue == null)
            {
                return new QueryElOperator<T>("(CASE WHEN {0} IS NULL THEN {1} ELSE {2} END)",
                    this,
                    whenTrueEl,
                    new QueryElParameter<TOtherElement>(whenFalseValue));
            }

            return new QueryElOperator<T>(
                "(CASE WHEN {0} = {1} THEN {2} ELSE {3} END)",
                this,
                new QueryElParameter<T>(comparisonValue),
                whenTrueEl,
                new QueryElParameter<TOtherElement>(whenFalseValue));
        }

        public QueryElOperator<T> IfElse<TOtherElement>(T comparisonValue, TOtherElement whenTrueValue, TOtherElement whenFalseValue)
        {
            if (comparisonValue == null)
            {
                return new QueryElOperator<T>("(CASE WHEN {0} IS NULL THEN {1} ELSE {2} END)",
                    this,
                    new QueryElParameter<TOtherElement>(whenTrueValue),
                    new QueryElParameter<TOtherElement>(whenFalseValue));
            }

            return new QueryElOperator<T>(
                "(CASE WHEN {0} = {1} THEN {2} ELSE {3} END)",
                this,
                new QueryElParameter<T>(comparisonValue),
                new QueryElParameter<TOtherElement>(whenTrueValue),
                new QueryElParameter<TOtherElement>(whenFalseValue));
        }

        public QueryElOperator<T> StartsWith(T value)
        {
            return new QueryElOperator<T>("({0} LIKE {1})", this, new QueryElParameter<string>(value + "%"));
        }

        public QueryEl In(params T[] values)
        {
            return In((IEnumerable<T>)values);
        }

        public QueryEl In<T2>(IEnumerable<T2?> values) where T2 : struct, T
        {
            return In(values.Where(v => v.HasValue).Select(v => (T)v.Value));
        }

        public QueryEl In(IEnumerable<T> values)
        {
            var valuesList = values as ICollection<T>;
            if (valuesList == null && values != null)
            {
                valuesList = values.ToList();
            }

            if (valuesList == null
                || valuesList.Count == 0)
            {
                // Always false
                return new QueryElBool(false);
            }

            if (valuesList.Count == 1)
            {
                // If the SQL-Server knows there is only a single value, it does a much better job at optimizing the query
                // Do not generate an In-Statement in this case.
                return new QueryElOperator<T>("({0}={1})", this, QueryConversionHelper.ConvertParameter(valuesList.Single()));
            }

            if (IdTypeExtension.IsIdType<T>() || IdTypeExtension.IsNullableIdType<T>())
            {
                var intsList = valuesList
                    .Where(v => v != null)
                    .Select(v => ((IConvertibleToInt32)v).ConvertToInt32())
                    // 2TK BDA initCapacity: valuesList.Count
                    .ToList();
                return new QueryElSetOperator<int>("({0} IN (SELECT Value FROM {1}))", this, intsList);
            }

            if (LongIdTypeExtension.IsLongIdType<T>() || LongIdTypeExtension.IsNullableLongIdType<T>())
            {
                var longsList = valuesList
                    .Where(v => v != null)
                    .Select(v => ((IConvertibleToInt64)v).ConvertToInt64())
                    // 2TK BDA initCapacity: valuesList.Count
                    .ToList();
                return new QueryElSetOperator<long>("({0} IN (SELECT Value FROM {1}))", this, longsList);
            }

            if (typeof(T) == typeof(string) || typeof(T) == typeof(int) || typeof(T) == typeof(int?))
            {
                return new QueryElSetOperator<T>("({0} IN (SELECT Value FROM {1}))", this, valuesList);
            }

            var vals = valuesList.JoinString(",", onlyNonEmptyValues: true);
            if (string.IsNullOrEmpty(vals))
            {
                return new QueryElBool(false);
            }

            return new QueryElFormat("({0} IN ({1}))", this, vals);
        }

        public QueryEl Between(T lowerBound, T upperBound)
        {
            return new QueryElOperator<T>(
                "({0} BETWEEN ({1}) AND ({2}))",
                this,
                QueryConversionHelper.ConvertParameter(lowerBound),
                QueryConversionHelper.ConvertParameter(upperBound));
        }
    }
}