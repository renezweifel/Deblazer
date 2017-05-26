using System;
using System.Collections.Generic;
using System.Linq;
using Dg.Deblazer.ContextValues;
using Dg.Deblazer.ContextValues.DgSpecific;
using Dg.Deblazer.Extensions;

namespace Dg.Deblazer.Write
{
    class WriteDefaultInsertValues : IWriteDefaultInsertValues
    {
        private readonly List<Delegate> _actions = new List<Delegate>();

        public void AddCustomValueSetter<T>(Action<T> action)
        {
            _actions.Add(action);
        }

        public void SetDefaultValues(IEnumerable<DbEntity> entities)
        {
            // Only set the insert and update date if it isn't set yet
            entities.OfType<IInsertDate>().Where(e => e.InsertDate == default(DateTime)).ForEach(e => e.InsertDate = DateTime.Now);
            entities.OfType<IUpdateDate>().Where(e => e.UpdateDate == default(DateTime)).ForEach(e => e.UpdateDate = DateTime.Now);

            foreach (var action in _actions)
            {
                entities.ForEach(e => action.Method.Invoke(action.Target, parameters: new[] { e }));
            }
        }
    }
}