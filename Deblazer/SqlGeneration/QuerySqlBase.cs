using System.Collections.Generic;
using Dg.Deblazer.Internal;
using Dg.Deblazer.SqlUtils;

namespace Dg.Deblazer.SqlGeneration
{
    public abstract class QuerySqlBase
    {
        internal IDbInternal Db;
        protected DbSqlCommand customSqlCommand;
        protected IReadOnlyList<object> parameters;
        protected string sqlString;

        protected abstract void EmitValueLoaded(DbEntity entity);
        protected abstract void EmitValueRemoved(DbEntity entity);

        protected DbSqlCommand GetSqlCommand()
        {
            if (customSqlCommand != null)
            {
                return customSqlCommand;
            }

            var sqlCommand = new DbSqlCommand
            {
                CommandTimeout = (int)Db.Settings.CommandTimeout.TotalSeconds,
                CommandText = sqlString
            };

            return sqlCommand;
        }

    }
}