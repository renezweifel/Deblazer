using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using Dg.Deblazer.Internal;
using Dg.Deblazer.Visitors;
using Dg.Deblazer.Write;

namespace Dg.Deblazer.SqlGeneration
{
    public abstract class QueryHelper<TElement> where TElement : DbEntity, new()
    {
        private string columnsString;

        public abstract string[] ColumnsInSelectStatement { get; }

        public abstract string[] ColumnsInInsertStatement { get; }

        public abstract string FullTableName { get; }

        public abstract string CreateTempTableCommand { get; }

        public string ColumnsString
        {
            get
            {
                if (columnsString == null)
                {
                    columnsString = string.Join(", ", ColumnsInSelectStatement);
                }

                return columnsString;
            }
        }

        public Type DbType
        {
            get { return typeof(TElement); }
        }

        public abstract bool IsForeignKeyTo(Type other);

        internal IEnumerable<TElement> SelectBy(IDbWrite db, string column, object value)
        {
            return db.Load<TElement>("SELECT " + string.Format(ColumnsString, FullTableName) + " FROM " + FullTableName + " WHERE " + column + " = @0", value);
        }

        public object Fill(object _obj, FillVisitor fillVisitor)
        {
            return Fill(_obj as TElement, fillVisitor);
        }

        public abstract void FillInsertCommand(SqlCommand sqlCommand, TElement entity);

        public void FillInsertCommand(SqlCommand sqlCommand, object entity)
        {
            FillInsertCommand(sqlCommand, (TElement)entity);
        }

        public abstract void ExecuteInsertCommand(SqlCommand sqlCommand, TElement entity);

        public void ExecuteInsertCommand(SqlCommand sqlCommand, object entity)
        {
            ExecuteInsertCommand(sqlCommand, (TElement)entity);
        }

        public TElement Fill(TElement entity, FillVisitor fillVisitor)
        {
            if (fillVisitor.IsDBNull())
            {
                fillVisitor.Skip(ColumnsInSelectStatement.Length);
            }
            else
            {
                if (entity == null)
                {
                    entity = new TElement();
                }

                ((IDbEntityInternal)entity).ModifyInternalState(fillVisitor);
            }

            return entity;
        }
    }
}