using System;
using System.Data.SqlClient;
using Dg.Deblazer.SqlGeneration;
using Dg.Deblazer.Visitors;

namespace Dg.Deblazer.Read
{
    public interface IHelper<TEntity> : IHelper
    {
        TEntity Fill(TEntity entity, FillVisitor fillVisitor);

        void FillInsertCommand(SqlCommand sqlCommand, TEntity entity);

        void ExecuteInsertCommand(SqlCommand sqlCommand, TEntity entity);
        QueryWrapper Wrapper { get; }
    }

    public interface IHelper
    {
        string[] ColumnsInSelectStatement { get; }

        string[] ColumnsInInsertStatement { get; }

        string CreateTempTableCommand { get; }

        string ColumnsString { get; }

        string FullTableName { get; }

        Type DbType { get; }

        object Fill(object entity, FillVisitor fillVisitor);

        void FillInsertCommand(SqlCommand sqlCommand, object entity);

        void ExecuteInsertCommand(SqlCommand sqlCommand, object entity);
    }
}
