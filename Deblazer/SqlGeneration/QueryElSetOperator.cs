using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Dg.Deblazer.SqlUtils;

namespace Dg.Deblazer.SqlGeneration
{
    internal class QueryElSetOperator<T> : QueryEl
    {
        private readonly QueryEl[] queryEls;
        private readonly string sql;
        private readonly IEnumerable<T> values;

        internal QueryElSetOperator(string sql, QueryEl el1, IEnumerable<T> values)
        {
            this.values = values;
            this.sql = sql;
            queryEls = new[] { el1 };
        }

        public override string GetSql(SqlParameterCollection parameters, int joinCount)
        {
            StringBuilder stringBuilder = new StringBuilder();

            if (typeof(T) == typeof(int))
            {
                string paramName = "@" + parameters.Count;
                QueryHelpers.AddSqlParameter(parameters, paramName, SqlParameterUtils.Create(paramName, values.OfType<int>()));
                stringBuilder.Append(paramName);
            }
            else if (typeof(T) == typeof(int?))
            {
                string paramName = "@" + parameters.Count;
                QueryHelpers.AddSqlParameter(parameters, paramName, SqlParameterUtils.Create(paramName, values.OfType<int?>()));
                stringBuilder.Append(paramName);
            }
            else if (typeof(T) == typeof(long))
            {
                string paramName = "@" + parameters.Count;
                QueryHelpers.AddSqlParameter(parameters, paramName, SqlParameterUtils.Create(paramName, values.OfType<long>()));
                stringBuilder.Append(paramName);
            }
            else if (typeof(T) == typeof(long?))
            {
                string paramName = "@" + parameters.Count;
                QueryHelpers.AddSqlParameter(parameters, paramName, SqlParameterUtils.Create(paramName, values.OfType<long?>()));
                stringBuilder.Append(paramName);
            }
            else if (typeof(T) == typeof(string))
            {
                string paramName = "@" + parameters.Count;
                QueryHelpers.AddSqlParameter(parameters, paramName, SqlParameterUtils.Create(paramName, values.OfType<string>()));
                stringBuilder.Append(paramName);
            }
            else
            {
                int i = 0;
                foreach (T value in values)
                {
                    if (!value.Equals(default(T)))
                    {
                        if (i > 0)
                        {
                            stringBuilder.Append(", ");
                        }

                        string paramName = "@" + parameters.Count;
                        stringBuilder.Append(paramName);
                        QueryHelpers.AddSqlParameter(parameters, paramName, value);
                        i++;
                    }
                }
            }

            string str1 = queryEls[0].GetSql(parameters, joinCount);
            return string.Format(sql, str1, stringBuilder);
        }
    }
}