using System.Data.SqlClient;

namespace Dg.Deblazer.SqlGeneration
{
    internal class QueryElParameter<T> : QueryEl
    {
        private readonly T value;

        public QueryElParameter(T value)
        {
            this.value = value;
        }

        public override string GetSql(SqlParameterCollection parameters, int joinCount)
        {
            var booleanValue = value as bool?;
            if (booleanValue != null)
            {
                // To help the query optimizer to create separate query plans for the true and false case we hard code boolean values
                return booleanValue.Value ? "1" : "0";
            }

            int? intValue;
            object sqlValue;
            if (value == null)
            {
                intValue = null;
                sqlValue = null;
            }
            else if (value is IConvertibleToInt32)
            {
                intValue = ((IConvertibleToInt32)value).ConvertToInt32();
                sqlValue = intValue;
            }
            else
            {
                intValue = value as int?;
                sqlValue = value;
            }

            if (intValue == 406802) // DbEntityIds.MandatorIds.Galaxus
            {
                // Since we had problems with queries for the mandator Galaxus (which typically returns a lot more rows that queries for other customers),
                // we hard code the mandatorId to let the query optimizer create a separate query plan for the mandator galaxus
                return intValue.Value.ToString();
            }

            string rawSql = "@" + parameters.Count;
            QueryHelpers.AddSqlParameter(parameters, rawSql, sqlValue);
            return rawSql;
        }
    }
}