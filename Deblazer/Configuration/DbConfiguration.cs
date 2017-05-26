using Dg.Deblazer.AggregateUpdate;
using Dg.Deblazer.ContextValues.DgSpecific;
using Dg.Deblazer.Validation;
using JetBrains.Annotations;
using System;

namespace Dg.Deblazer.Configuration
{
    internal class DbConfiguration : IDbConfiguration
    {
        public DbConfiguration(IDbEntityValidator validation,
            IEntityFilter entityFilter,
            IAggregateUpdateService aggregateUpdateService,
            ICacheService cacheService,
            [CanBeNull] Func<BaseDb> getDbForEvilLazyLoad)
        {
            Validator = validation;
            EntityFilter = entityFilter;
            AggregateUpdateService = aggregateUpdateService;
            CacheService = cacheService;
            GetDbForEvilLazyLoad = getDbForEvilLazyLoad;
        }

        public IDbEntityValidator Validator { get; }

        public IAggregateUpdateService AggregateUpdateService { get; }

        public IEntityFilter EntityFilter { get; }

        public ICacheService CacheService { get; }

        [CanBeNull]
        public Func<BaseDb> GetDbForEvilLazyLoad { get; }
    }
}