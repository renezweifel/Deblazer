using System;
using System.Collections.Generic;
using System.Linq;
using Dg.Deblazer.Visitors;

namespace Dg.Deblazer.Write
{
    internal class WriteDefaultUpdateValues : IWriteDefaultUpdateValues
    {
        private readonly Dictionary<Type, (string dbColumnName, string dbDateType, Delegate setterFunction, Delegate valueFunction)> _actionsByType = new Dictionary<Type, (string dbColumnName, string dbDateType, Delegate setterFunction, Delegate valueFunction)>();

        public void AddCustomValueSetter<TEntity, TColumn>(string dbColumnName, string dbDateType, Action<TEntity, TColumn> setterFunction, Func<TColumn> valueFunction)
        {
            _actionsByType.Add(typeof(TEntity), (dbColumnName, dbDateType, setterFunction, valueFunction));
        }

        public void SetDefaultValues(IUpdateVisitor visitor)
        {
            foreach (var actionByType in _actionsByType)
            {
                var action = actionByType.Value;

                foreach (var entity in visitor.UpdateSet.Where(e => actionByType.Key.IsAssignableFrom(e.GetType())))
                {
                    visitor.SetCurrentEntity(entity);

                    var valueEvaluated = action.valueFunction.Method.Invoke(action.valueFunction.Target, parameters: null);

                    visitor.AddUpdatedValue(action.dbColumnName, action.dbDateType, valueEvaluated);
                    action.setterFunction.Method.Invoke(action.setterFunction.Target, parameters: new[] { valueEvaluated });
                }
            }
        }
    }
}