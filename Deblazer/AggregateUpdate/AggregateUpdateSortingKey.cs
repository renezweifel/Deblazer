using System;
using System.Linq;

namespace Dg.Deblazer.AggregateUpdate
{
    public struct AggregateUpdateSortingKey
    {
        public readonly int ExecutionOrderNumber;
        public readonly int UniqueKey;
        public readonly AggregateUpdateSortingKey[] UpdatersThatMustRunBeforeThisUpdater;

        public AggregateUpdateSortingKey(int executionOrderNumber, int uniqueKey, params AggregateUpdateSortingKey[] updatersThatMustRunBeforeThisUpdater)
        {
            ExecutionOrderNumber = executionOrderNumber;
            UniqueKey = uniqueKey;
            if (updatersThatMustRunBeforeThisUpdater?.Any(u => u.ExecutionOrderNumber == 0 && u.UniqueKey == 0) == true)
            {
                throw new InvalidOperationException($"{nameof(AggregateUpdateSortingKey)} with {nameof(uniqueKey)} {uniqueKey} has a dependency"
                    + $"to an invalid {nameof(AggregateUpdateSortingKey)}. Probably you are referring to a public static readonly instance "
                    + "which is not yet initialized due to wrong ordering.");
            }
            UpdatersThatMustRunBeforeThisUpdater = updatersThatMustRunBeforeThisUpdater;
        }
    }
}