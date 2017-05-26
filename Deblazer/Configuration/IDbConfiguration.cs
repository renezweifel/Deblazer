using System;
using Dg.Deblazer.AggregateUpdate;
using Dg.Deblazer.ContextValues.DgSpecific;
using Dg.Deblazer.Validation;

namespace Dg.Deblazer.Configuration
{
    public interface IDbConfiguration
    {
        IDbEntityValidator Validator { get; }

        IEntityFilter EntityFilter { get; }

        IAggregateUpdateService AggregateUpdateService { get; }

        ICacheService CacheService { get; }

        Func<BaseDb> GetDbForEvilLazyLoad { get; }
    }
}