using System.Data.SqlClient;

namespace Dg.Deblazer.SqlGeneration
{
    internal class QueryElBool : QueryElOperator<bool>
    {
        public QueryElBool(bool b)
            : base("{0}={1}", new QueryElParameter<bool>(b), new QueryElParameter<bool>(true))
        {
            IsTrue = b;
        }

        public bool IsTrue { get; private set; }

        public override string GetSql(SqlParameterCollection parameters, int joinCount)
        {
            return IsTrue ? "1=1" : "1=0";
        }

        public static implicit operator bool(QueryElBool x) // db.Customers().WhereDb(c => true || c.Id == 200365)
        {
            return x.IsTrue;
        }
    }
}