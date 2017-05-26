using devinite.DbLayer.Configuration;
using devinite.SqlServerLibrary;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;

namespace devinite.DbLayer
{
    public class SqlCommands
    {
        public static readonly TimeSpan CommandTimeoutDefault = TimeSpan.FromSeconds(60);

       

        private const string integerParameterTable = "dbo.IntTableParameter";
        private const string stringParameterTable = "dbo.StringTableParameter";
        
        public static object SelectSingleValue(string sqlCommand, params SqlParameter[] parameters)
        {
            using (var command = new SqlCommand(sqlCommand))
            {
                foreach (var param in parameters)
                {
                    command.Parameters.Add(param);
                }

                return SelectSingleValue(command);
            }
        }

        public static T SelectSingleValue<T>(string sqlCommand)
        {
            return DataRowExtensionsCustom.Get<T>(SelectSingleValue(sqlCommand, new SqlParameter[0]));
        }

        public static T SelectSingleValue<T>(string sqlCommand, params SqlParameter[] parameters)
        {
            return DataRowExtensionsCustom.Get<T>(SelectSingleValue(sqlCommand, parameters));
        }

        public static T SelectSingleValue<T>(string sqlCommand, params object[] parameters)
        {
            return DataRowExtensionsCustom.Get<T>(SelectSingleValue(sqlCommand, parameters.Select((p, i) => new SqlParameter("@" + i, p)).ToArray()));
        }

        public static List<T> SelectSingleValues<T>(string sqlCommand, params object[] parameters)
        {
            DataTable dt = SelectMultipleRows(sqlCommand, parameters);
            List<T> list = new List<T>();
            foreach (DataRow dr in dt.Rows)
            {
                list.Add(DataRowExtensionsCustom.Get<T>(dr[0]));
            }

            return list;
        }

        public static List<T> SelectSingleValues<T>(SqlCommand sqlCommand)
        {
            DataTable dt = SelectMultipleRows(sqlCommand);
            List<T> list = new List<T>();
            foreach (DataRow dr in dt.Rows)
            {
                list.Add(DataRowExtensionsCustom.Get<T>(dr[0]));
            }

            return list;
        }

        public static T SelectSingleValue<T>(SqlCommand sqlCommand)
        {
            return DataRowExtensionsCustom.Get<T>(SelectSingleValue(sqlCommand));
        }

        public static object SelectSingleValue(SqlCommand sqlCommand, IsolationLevel isolationLevel)
        {
            if (sqlCommand.Connection == null)
            {
                return ExecuteSqlCommand(() =>
                {
                    using (SqlConnection sqlConnection = new SqlConnection(GlobalDbConfiguration.ConnectionString))
                    {
                        sqlCommand.Connection = sqlConnection;
                        sqlConnection.Open();
                        return sqlCommand.ExecuteScalar();
                    }
                });
            }
            else
            {
                // Just execute the query, don't close the connection
                return sqlCommand.ExecuteScalar();
            }
        }

        public static object SelectSingleValue(SqlCommand sqlCommand)
        {
            return SelectSingleValue(sqlCommand, IsolationLevel.ReadCommitted);
        }

        public static object SelectSingleValue(string selectCommand)
        {
            using (SqlCommand sqlCommand = new SqlCommand())
            {
                sqlCommand.CommandText = selectCommand;
                return SelectSingleValue(sqlCommand);
            }
        }

        public static DataRow SelectSingleRow(SqlConnection connection, string selectCommand, params SqlParameter[] parameters)
        {
            using (var command = new SqlCommand(selectCommand, connection))
            {
                foreach (var param in parameters)
                {
                    command.Parameters.Add(param);
                }

                return SelectSingleRow(command);
            }
        }

        public static DataRow SelectSingleRow(string selectCommand, params SqlParameter[] parameters)
        {
            using (var command = new SqlCommand(selectCommand))
            {
                foreach (var param in parameters)
                {
                    command.Parameters.Add(param);
                }

                return SelectSingleRow(command);
            }
        }

        [CanBeNull]
        public static DataRow SelectSingleRow(string selectCommand)
        {
            DataRowCollection drc = SelectMultipleRows(selectCommand).Rows;
            if (drc.Count > 0)
            {
                return drc[0];
            }

            return null;
        }

        public static DataRow SelectSingleRow(SqlCommand sqlCommand)
        {
            DataRowCollection drc = SelectMultipleRows(sqlCommand).Rows;
            if (drc.Count > 0)
            {
                return drc[0];
            }

            return null;
        }

        public static DataTable SelectMultipleRows(string selectCommand, params object[] commandParameters)
        {
            using (SqlCommand sqlCommand = new SqlCommand())
            {
                AddParametersToCommand(sqlCommand, commandParameters);
                sqlCommand.CommandText = selectCommand;
                return SelectMultipleRows(sqlCommand);
            }
        }

        public static DataTable SelectMultipleRows(string selectCommand, params SqlParameter[] parameters)
        {
            return SelectMultipleRows(GlobalDbConfiguration.ConnectionString, selectCommand, parameters);
        }

        public static DataTable SelectMultipleRows(SqlConnection sqlConnection, string selectCommand, params SqlParameter[] parameters)
        {
            return ExecuteSqlCommand(() =>
            {
                using (var command = new SqlCommand(selectCommand) { Connection = sqlConnection })
                {
                    foreach (var param in parameters)
                    {
                        command.Parameters.Add(param);
                    }

                    return SelectMultipleRows(command);
                }
            });
        }

        public static DataTable SelectMultipleRows(string connectionString, string selectCommand, params SqlParameter[] parameters)
        {
            return SelectMultipleRows(new SqlConnection(connectionString), selectCommand, parameters);
        }

        public static DataTable SelectMultipleRows(string selectCommand)
        {
            using (SqlCommand sqlCommand = new SqlCommand())
            {
                sqlCommand.CommandText = selectCommand;
                return SelectMultipleRows(sqlCommand);
            }
        }

        public static DataTable SelectMultipleRows(SqlCommand sqlCommand)
        {
            if (sqlCommand.Connection == null)
            {
                return ExecuteSqlCommand(() =>
                {
                    using (SqlConnection sqlVerbindung = new SqlConnection(GlobalDbConfiguration.ConnectionString))
                    {
                        sqlCommand.Connection = sqlVerbindung;
                        SqlDataAdapter da = new SqlDataAdapter(sqlCommand);

                        sqlVerbindung.Open();

                        var dataTable = new DataTable();
                        da.Fill(dataTable);
                        return dataTable;
                    }
                });
            }
            else
            {
                SqlDataAdapter da = new SqlDataAdapter(sqlCommand);
                var dataTable = new DataTable();
                da.Fill(dataTable);
                return dataTable;
            }
        }

        public static int ExecuteNonQuery(string commandText, params object[] commandParameters)
        {
            using (SqlCommand sqlCommand = new SqlCommand())
            {
                AddParametersToCommand(sqlCommand, commandParameters);
                sqlCommand.CommandText = commandText;
                return ExecuteNonQuery(sqlCommand);
            }
        }

        public static void AddParametersToCommand(SqlCommand sqlCommand, params object[] commandParameters)
        {
            for (int i = 0; i < commandParameters.Length; i++)
            {
                if (commandParameters[i] is Date?)
                {
                    sqlCommand.Parameters.AddWithValue("@" + i, (DateTime?)(Date?)commandParameters[i] ?? (object)DBNull.Value);
                }
                else
                {
                    sqlCommand.Parameters.AddWithValue("@" + i, commandParameters[i]);
                }
            }
        }

        public static int ExecuteNonQuery(SqlCommand sqlCommand)
        {
            if (sqlCommand.Connection == null)
            {                
                return ExecuteSqlCommand(() =>
                {
                    using (SqlConnection sqlConnection = new SqlConnection(GlobalDbConfiguration.ConnectionString))
                    {
                        sqlCommand.Connection = sqlConnection;
                        sqlConnection.Open();
                        return sqlCommand.ExecuteNonQuery();
                    }
                });
            }
            else
            {
                return sqlCommand.ExecuteNonQuery();
            }
        }

        public static int ExecuteInsert(string selectCommand)
        {
            using (SqlCommand sqlCommand = new SqlCommand())
            {
                sqlCommand.CommandText = selectCommand;
                return ExecuteInsert(sqlCommand);
            }
        }

        public static int ExecuteInsert(SqlCommand sqlCommand)
        {
            sqlCommand.CommandText += ";SELECT SCOPE_IDENTITY();";

            if (sqlCommand.Connection == null)
            {
                return ExecuteSqlCommand(() =>
                {
                    using (SqlConnection sqlVerbindung = new SqlConnection(GlobalDbConfiguration.ConnectionString))
                    {
                        sqlVerbindung.Open();
                        sqlCommand.Connection = sqlVerbindung;
                        var execResult = sqlCommand.ExecuteScalar();
                        if (!(execResult is DBNull))
                        {
                            return Convert.ToInt32(execResult);
                        }
                        else
                        {
                            return -1;
                        }
                    }
                });
            }
            else
            {
                var execResult = sqlCommand.ExecuteScalar();
                if (!(execResult is DBNull))
                {
                    return Convert.ToInt32(execResult);
                }
                else
                {
                    return -1;
                }
            }
        }

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
                .OrderByDescending(p => p.ParameterName.Length)
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
                var insertSql = new StringBuilder("insert into ")
                    .Append(paramterName)
                    .Append(" values ");
                var values = ((DataTable)value).Rows.Cast<DataRow>().Select(dr => "(" + dr[0] + ")");
                insertSql.Append(string.Join(", ", values));

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