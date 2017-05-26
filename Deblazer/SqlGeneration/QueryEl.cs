using System;
using System.Data.SqlClient;

namespace Dg.Deblazer.SqlGeneration
{
    /// <summary>
    /// Overloading operators && and ||, see http://steve.emxsoftware.com/NET/Overloading+the++and++operators
    /// The operation x && y is evaluated as T.false(x) ? x : T.&(x, y), where T.false(x) is an invocation of the operator false declared in T, and T.&(x, y) is an invocation of the selected operator &. In other words, x is first evaluated and operator false is invoked on the result to determine if x is definitely false. Then, if x is definitely false, the result of the operation is the value previously computed for x. Otherwise, y is evaluated, and the selected operator & is invoked on the value previously computed for x and the value computed for y to produce the result of the operation.
    /// The operation x || y is evaluated as T.true(x) ? x : T.|(x, y), where T.true(x) is an invocation of the operator true declared in T, and T.|(x, y) is an invocation of the selected operator |. In other words, x is first evaluated and operator true is invoked on the result to determine if x is definitely true. Then, if x is definitely true, the result of the operation is the value previously computed for x. Otherwise, y is evaluated, and the selected operator | is invoked on the value previously computed for x and the value computed for y to produce the result of the operation.
    /// </summary>
    public abstract class QueryEl
    {
        public abstract string GetSql(SqlParameterCollection parameters, int joinCount);

        public static QueryEl operator !(QueryEl x)
        {
            return new QueryElOperator<bool>("(NOT {0})", QueryConversionHelper.ConvertMember(x));
        }

        // Do not delete this operator. It is needed to evaluate &&.
        public static QueryEl operator &(QueryEl x, QueryEl y)
        {
            return new QueryElOperator<bool>("(({0}) AND ({1}))", QueryConversionHelper.ConvertMember(x), QueryConversionHelper.ConvertMember(y));
        }

        // Do not delete this operator. It is needed to evaluate ||.
        public static QueryEl operator |(QueryEl x, QueryEl y)
        {
            return new QueryElOperator<bool>("(({0}) OR ({1}))", QueryConversionHelper.ConvertMember(x), QueryConversionHelper.ConvertMember(y));
        }

        // Do not delete this operator. It is needed to evaluate || and &&.
        public static bool operator true(QueryEl criteria)
        {
            return false;
        }

        // Do not delete this operator. It is needed to evaluate || and &&.
        public static bool operator false(QueryEl criteria)
        {
            return false;
        }
    }
}