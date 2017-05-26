using System;
using Dg.Deblazer.Visitors;

namespace Dg.Deblazer.Write
{
    public interface IWriteDefaultUpdateValues
    {
        void SetDefaultValues(IUpdateVisitor visitor);
        void AddCustomValueSetter<TEntity, TColumn>(
            string dbColumnName, 
            string dbDateType, 
            Action<TEntity, TColumn> setterFunction, 
            Func<TColumn> valueFunction);
    }
}