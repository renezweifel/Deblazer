using System.Collections.Generic;

namespace Dg.Deblazer.Visitors
{
    public interface IUpdateVisitor
    {
        void SetCurrentEntity(DbEntity entity);
        void AddUpdatedValue(string columnName, string columnDbDataTypeName, object value);
        IReadOnlyList<DbEntity> UpdateSet { get; }
        bool DoReset { get; }
        string[] ColumnsToReset { get; }
    }
}