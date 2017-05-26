using Dg.Deblazer.Configuration;
using Dg.Deblazer.Extensions;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;

namespace Dg.Deblazer.SqlUtils
{
    /// <summary>
    /// Wrapper around System.Data.SqlClient to enable logging and time measurement
    /// </summary>
    public class DbSqlCommand : IDisposable
    {
        private readonly SqlCommand sqlCommand;

        public SqlCommand SqlCommand { get { return this.sqlCommand; } }

        public DbSqlCommand()
            : this(new SqlCommand())
        {
        }

        public DbSqlCommand(string cmdText)
            : this(new SqlCommand(cmdText))
        {
        }

        public DbSqlCommand(string cmdText, SqlConnection connection)
            : this(new SqlCommand(cmdText, connection))
        {
        }

        public DbSqlCommand(string cmdText, SqlConnection connection, SqlTransaction transaction)
            : this(new SqlCommand(cmdText, connection, transaction))
        {
        }

        public DbSqlCommand(SqlCommand sqlCommand)
        {
            if (sqlCommand == null)
                throw new ArgumentNullException("sqlCommand");

            this.sqlCommand = sqlCommand;
        }

        public string CommandText
        {
            get { return this.sqlCommand.CommandText; }

            set { this.sqlCommand.CommandText = value; }
        }

        /// <summary>
        /// Gets or sets the wait time before terminating the attempt to execute a command
        /// and generating an error.
        /// The time in seconds to wait for the command to execute. The default is 30 seconds.
        /// </summary>
        public int CommandTimeout
        {
            get { return this.sqlCommand.CommandTimeout; }

            set { this.sqlCommand.CommandTimeout = value; }
        }

        public CommandType CommandType
        {
            get { return this.sqlCommand.CommandType; }

            set { this.sqlCommand.CommandType = value; }
        }

        public SqlConnection Connection
        {
            get { return this.sqlCommand.Connection; }

            set { this.sqlCommand.Connection = value; }
        }

        public SqlParameterCollection Parameters
        {
            get { return this.sqlCommand.Parameters; }
        }

        public int ExecuteNonQuery()
        {
            using (QueryTime.Measure(this))
            {
                return this.sqlCommand.ExecuteNonQuery();
            }
        }

        public SqlDataReader ExecuteReader()
        {
            using (QueryTime.Measure(this))
            {
                return this.sqlCommand.ExecuteReader();
            }
        }

        public SqlDataReader ExecuteReader(CommandBehavior behavior)
        {
            using (QueryTime.Measure(this))
            {
                return this.sqlCommand.ExecuteReader(behavior);
            }
        }

        public T SelectSingleValue<T>()
        {
            if (SqlCommand.Connection == null)
            {
                throw new InvalidOperationException("SqlCommand.Connection is not set.");
            }

            using (QueryTime.Measure(this))
            {
                var value = sqlCommand.ExecuteScalar();
                if (value == null)
                {
                    return default(T);
                }

                // Special case for Id<> and LongId<>
                var targetConvertType = typeof(T);
                if (IdTypeExtension.IsIdType(targetConvertType))
                {
                    var constructor = targetConvertType.GetConstructor(new[] { typeof(int) });
                    if (constructor == null)
                    {
                        throw new InvalidCastException("The Id<> type has no constructor taking int as a parameter.");
                    }

                    return (T)constructor.Invoke(new object[] { Convert.ToInt32(value) });
                }
                else if (LongIdTypeExtension.IsLongIdType(targetConvertType))
                {
                    var constructor = targetConvertType.GetConstructor(new[] { typeof(long) });
                    if (constructor == null)
                    {
                        throw new InvalidCastException("The LongId<> type has no constructor taking long as a parameter.");
                    }

                    return (T)constructor.Invoke(new object[] { Convert.ToInt64(value) });
                }

                // Just execute the query, don't close the connection
                return DataRowExtensionsCustom.Get<T>(value);
            }
        }

        public static int ExecuteNonQueryBatch(IReadOnlyList<DbSqlCommand> sqlCommands, SqlConnection sqlConnection)
        {
            if (sqlCommands == null)
            {
                throw new ArgumentNullException(nameof(sqlCommands));
            }
            if (sqlConnection == null)
            {
                throw new ArgumentNullException(nameof(sqlConnection));
            }
            if (sqlCommands.Count == 0)
            {
                return 0;
            }

            var connectionsCount = sqlCommands.Count(c => c.Connection != null && c.Connection != sqlConnection);
            if (connectionsCount > 1)
            {
                throw new InvalidOperationException("All commands of a batch must use the same connection.");
            }

            var commandSet = new SqlCommandSet();
            commandSet.CommandTimeout = (int)TimeSpan.FromMinutes(5).TotalSeconds;

            foreach (var sqlCommand in sqlCommands)
            {
                commandSet.Append(sqlCommand.SqlCommand);
            }

            using (QueryTime.MeasureBatch(commandSet))
            {
                return SqlCommands.ExecuteSqlCommand(() =>
                {
                    bool wasOpened = false;
                    if (sqlConnection.State != ConnectionState.Open)
                    {
                        sqlConnection.Open();
                        wasOpened = true;
                    }

                    try
                    {
                        commandSet.Connection = sqlConnection;
                        var changesCount = commandSet.ExecuteNonQuery();
                        return changesCount;
                    }
                    finally
                    {
                        if (wasOpened)
                        {
                            sqlConnection.Close();
                        }
                    }
                });
            }
        }

        public void Dispose()
        {
            sqlCommand.Dispose();
        }

        private struct QueryTime : IDisposable
        {
            private string sql;
            private Stopwatch stopwatch;

            public static QueryTime Measure(DbSqlCommand sqlCommand)
            {
                if (GlobalDbConfiguration.QueryLogger.DoWriteLog)
                {
                    return new QueryTime
                    {
                        sql = SqlCommands.GetSqlCommandText(sqlCommand.SqlCommand),
                        stopwatch = Stopwatch.StartNew()
                    };
                }
                else
                {
                    return new QueryTime()
                    {
                        stopwatch = Stopwatch.StartNew()
                    };
                }
            }

            public static QueryTime MeasureBatch(SqlCommandSet commandSet)
            {
                if (GlobalDbConfiguration.QueryLogger.DoWriteLog)
                {
                    return new QueryTime
                    {
                        sql = SqlCommands.GetSqlCommandText(commandSet.BatchCommand),
                        stopwatch = Stopwatch.StartNew()
                    };
                }
                else
                {
                    return new QueryTime()
                    {
                        stopwatch = Stopwatch.StartNew()
                    };
                }
            }

            public void Dispose()
            {
                stopwatch.Stop();

                if (GlobalDbConfiguration.QueryLogger.DoWriteLog)
                {
                    GlobalDbConfiguration.QueryLogger.WriteLog(this.sql, stopwatch.ElapsedMilliseconds);
                }

                GlobalDbConfiguration.QueryLogger.IncrementQueryCountAndTime((int)stopwatch.ElapsedMilliseconds);
            }
        }
    }
}