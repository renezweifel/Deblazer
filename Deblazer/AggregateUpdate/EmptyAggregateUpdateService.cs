using System;
using System.Collections.Generic;
using Dg.Deblazer.ContextValues;
using Dg.Deblazer.ContextValues.DgSpecific;
using Dg.Deblazer.Write;

namespace Dg.Deblazer.AggregateUpdate
{
    public class EmptyAggregateUpdateService : IAggregateUpdateService
    {
        public static readonly EmptyAggregateUpdateService Instance = new EmptyAggregateUpdateService();

        private EmptyAggregateUpdateService()
        {
        }

        public void AddAggregateUpdateToProcess(IAggregateUpdate aggregateUpdate, Action<DbEntity> insertEntity)
        {
            // Do nothing
        }

        public IReadOnlyList<IAggregateUpdate> GetAggregateUpdates(IDbWrite db)
        {
            return Array.Empty<IAggregateUpdate>();
        }
    }
}
