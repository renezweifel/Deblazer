using System.Data.SqlClient;

namespace Dg.Deblazer.SqlGeneration
{
    internal class QueryElLiteral : QueryEl
    {
        private readonly string sql;
        private readonly object[] values;

        public QueryElLiteral(string sql, params object[] values)
        {
            this.values = values;
            this.sql = sql;
        }

        public override string GetSql(SqlParameterCollection parameters, int joinCount)
        {
            string[] paramStrings = new string[values.Length];
            for (int i = 0; i < values.Length; i++)
            {
                paramStrings[i] = "@" + parameters.Count;
                QueryHelpers.AddSqlParameter(parameters, paramStrings[i], values[i]);
            }

            return string.Format(sql, paramStrings);
        }
    }
}