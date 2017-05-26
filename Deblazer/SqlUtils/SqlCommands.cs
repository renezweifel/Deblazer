using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Threading;
using Dg.Deblazer.Extensions;

namespace Dg.Deblazer.SqlUtils
{
    /// <summary>
    /// Contains Methods to execute sql commands with retry and a method to get the sql string for a command.
    /// (and also this class violates SRP)
    /// </summary>
    internal class SqlCommands
    {
        private const string integerParameterTable = "dbo.IntTableParameter";
        private const string stringParameterTable = "dbo.StringTableParameter";

        /// <summary>
        /// HINT: Prefer the other overload which takes a function as parameter and returns a result. The goal is to avoid to capture a closure for better performance
        /// </summary>
        public static void ExecuteSqlCommand(Action sqlCode)
        {
            int retryCount = 3;
            while (retryCount > 0)
            {
                try
                {
                    sqlCode();
                    return;
                }
                catch (SqlException exception) when (exception.Number == 1205 && retryCount > 1)
                {
                    // a sql exception that is a deadlock
                    Thread.Sleep(new Random().Next(70) + 130);
                    retryCount--;
                }
            }
        }

        public static TResult ExecuteSqlCommand<TResult>(Func<TResult> sqlCode)
        {
            int retryCount = 3;
            while (retryCount > 0)
            {
                try
                {
                    return sqlCode();
                }
                catch (SqlException exception) when (exception.Number == 1205 && retryCount > 1)
                {
                    // a sql exception that is a deadlock
                    Thread.Sleep(new Random().Next(70) + 130);
                    retryCount--;
                }
            }

            throw new InvalidOperationException("This exception is unreachable because we throw at last before last retry");
        }

        public static string GetSqlCommandText(SqlCommand sqlCommand)
        {
            var sqlParameters = sqlCommand.Parameters.OfType<SqlParameter>()
                // Sort @12 after @2 not before
                .OrderByDescending(p => p.ParameterName.Length)
                .ThenBy(p => p.ParameterName)
                .ToList();

            string paramterDeclaration = GetParameterDeclarations(sqlParameters);
            var sql = new StringBuilder(paramterDeclaration)
                .AppendLine()
                .Append(sqlCommand.CommandText);

            return sql.ToString();
        }

        private static string GetParameterDeclarations(IReadOnlyList<SqlParameter> sqlParameters)
        {
            if (sqlParameters.NullOrNone())
            {
                return "";
            }

            var parameterDeclarationSql = new StringBuilder();

            foreach (SqlParameter sqlParameter in sqlParameters)
            {
                parameterDeclarationSql.Append("declare ")
                    .Append(sqlParameter.ParameterName)
                    .Append(" ")
                    .AppendLine(GetDataType(sqlParameter))
                    .AppendLine(GetValueSetter(sqlParameter));
            }

            return parameterDeclarationSql.ToString();
        }

        private static string GetDataType(SqlParameter sqlParameter)
        {
            var value = sqlParameter.Value;
            var typeName = sqlParameter.TypeName;
            if (value == null || Convert.IsDBNull(value))
            {
                return typeName;
            }

            var valueString = value as string;
            if (valueString != null)
            {
                var sb = new StringBuilder("nvarchar(")
                    .Append(valueString.Length.ToString())
                    .Append(")");

                return sb.ToString();
            }

            if (value is int)
            {
                return "int";
            }

            if (value is DateTime)
            {
                return "datetime";
            }

            if (value is Date)
            {
                return "date";
            }

            if (value is bool)
            {
                return "bit";
            }

            if (value is decimal)
            {
                var precision = ((SqlDecimal)sqlParameter.SqlValue).Precision;
                var scale = ((SqlDecimal)sqlParameter.SqlValue).Scale;
                return $"decimal({precision},{scale})";
            }

            if (value is DataTable)
            {
                var str = ((DataTable)value).Rows.Count > 0 && ((DataTable)value).Rows[0][0] is int
                    ? integerParameterTable
                    : stringParameterTable;

                return str;
            }

            return typeName;
        }

        private static string GetValueSetter(SqlParameter sqlParameter)
        {
            var value = sqlParameter.Value;
            var paramterName = sqlParameter.ParameterName;
            if (value is DataTable)
            {
                var allRows = ((DataTable)value).Rows.Cast<DataRow>();

                var insertSql = new StringBuilder();

                int maxRowsAllowedForInsert = 1000;
                foreach (var rowsBatch in allRows.Batch(maxRowsAllowedForInsert))
                {
                    insertSql
                        .Append("insert into ")
                        .Append(paramterName)
                        .Append(" values ");
                    var values = rowsBatch.Select(dr => "(" + dr[0] + ")");
                    insertSql.Append(string.Join(", ", values));
                    insertSql.AppendLine();
                }


                return insertSql.ToString();
            }

            var setSql = new StringBuilder("set ")
                .Append(paramterName)
                .Append(" = ");

            if (value == null || Convert.IsDBNull(value))
            {
                setSql.Append("NULL");

                return setSql.ToString();
            }
            else if (value is string)
            {
                setSql
                    .Append("'")
                    .Append(value)
                    .Append("'");

                return setSql.ToString();
            }
            else if (value is DateTime)
            {
                setSql
                    .Append("'")
                    .Append(((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss:fff"))
                    .Append("'");

                return setSql.ToString();
            }
            else if (value is Date)
            {
                setSql
                    .Append("'")
                    .Append(((Date)value).ToString("yyyy-MM-dd"))
                    .Append("'");

                return setSql.ToString();
            }
            else if (value is bool)
            {
                setSql.Append((bool)value ? "1" : "0");
                return setSql.ToString();
            }

            setSql.Append(value);

            return setSql.ToString();
        }
    }
}