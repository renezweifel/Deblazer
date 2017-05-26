using System.Data.SqlClient;

namespace Dg.Deblazer.SqlGeneration
{
    internal class QueryElFormat : QueryEl
    {
        private readonly object[] args;
        private readonly QueryEl el1;
        private readonly string sql;

        public QueryElFormat(string sql, QueryEl el1, params object[] values)
        {
            args = values;
            this.sql = sql;
            this.el1 = el1;
        }

        public override string GetSql(SqlParameterCollection parameters, int joinCount)
        {
            object[] paramStrings = new object[args.Length + 1];
            paramStrings[0] = el1.GetSql(parameters, joinCount);
            for (int i = 0; i < args.Length; i++)
            {
                paramStrings[i + 1] = args[i];
            }

            return string.Format(sql, paramStrings);
        }
    }
}