using System;
using System.Collections.Generic;
using Dg.Deblazer.ContextValues;
using Dg.Deblazer.ContextValues.DgSpecific;
using Dg.Deblazer.Write;

namespace Dg.Deblazer.AggregateUpdate
{
    public interface IAggregateUpdateService
    {
        IReadOnlyList<IAggregateUpdate> GetAggregateUpdates(IDbWrite db);
        void AddAggregateUpdateToProcess(IAggregateUpdate aggregateUpdate, Action<DbEntity> insertEntity);
    }
}