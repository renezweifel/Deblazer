using System;
using System.Collections.Generic;
using Dg.Deblazer.ContextValues.DgSpecific;

namespace Dg.Deblazer.Write
{
    public interface IWriteDefaultInsertValues
    {
        void SetDefaultValues(IEnumerable<DbEntity> entities);
        void AddCustomValueSetter<T>(Action<T> action);
    }
}