using Dg.Deblazer.AggregateUpdate;
using Dg.Deblazer.ContextValues.DgSpecific;
using Dg.Deblazer.Validation;
using System;
using System.Collections.Generic;

namespace Dg.Deblazer.Configuration
{
    public static class GlobalDbConfiguration
    {
        private static readonly Dictionary<string, IDbConfiguration> configurationByEntityNamespace = new Dictionary<string, IDbConfiguration>();

        private static readonly IDbConfiguration emptyConfiguration = new DbConfiguration(
            validation: EmptyValidator.Instance,
            entityFilter: EmptyEntityFilter.Instance,
            aggregateUpdateService: EmptyAggregateUpdateService.Instance,
            cacheService: NullCacheService.Instance,
            getDbForEvilLazyLoad: null);

        public static IDbConfiguration GetConfigurationOrEmpty(Type entityType)
        {
            if (entityType == null)
            {
                return emptyConfiguration;
            }

            IDbConfiguration configuration;
            if (configurationByEntityNamespace.TryGetValue(entityType.Namespace, out configuration))
            {
                return configuration;
            }

            return emptyConfiguration;
        }

        public static void AddConfiguration(
            string entityNameSpace,
            IDbEntityValidator validation = null,
            IEntityFilter entityFilter = null,
            IAggregateUpdateService aggregateUpdateService = null,
            ICacheService cacheService = null,
            Func<BaseDb> getDbForEvilLazyLoad = null)
        {
            if (configurationByEntityNamespace.ContainsKey(entityNameSpace))
            {
                throw new InvalidOperationException($"There is already a configuration for '{entityNameSpace}' Changing the configuration is not allowed");
            }

            var configuration = new DbConfiguration(
                validation: validation ?? EmptyValidator.Instance,
                entityFilter: entityFilter ?? EmptyEntityFilter.Instance,
                aggregateUpdateService: aggregateUpdateService ?? EmptyAggregateUpdateService.Instance,
                cacheService: cacheService ?? NullCacheService.Instance,
                getDbForEvilLazyLoad: getDbForEvilLazyLoad
                );

            configurationByEntityNamespace[entityNameSpace] = configuration;
        }

        private static IQueryLoggingHandler queryLoggingHandler = NullQueryLoggingHandler.Instance;

        public static IQueryLoggingHandler QueryLogging
        {
            get
            {
                return queryLoggingHandler;
            }
            set
            {
                if (value == null)
                {
                    throw new InvalidOperationException(nameof(QueryLogging) + " cannot be set to NULL. Use " + nameof(NullQueryLoggingHandler) + "." + nameof(NullQueryLoggingHandler.Instance) + " instad");
                }
                queryLoggingHandler = value;
            }
        }

        public static IQueryLogger QueryLogger
        {
            get
            {
                return QueryLogging.QueryLogger;
            }
        }

        public static ISubmitChangesConfigurationHandler SubmitChangesConfiguration { get; set; } = DefaultSubmitChangesConfigurationHandler.Instance;

        private class OverwriteConfigurationDisposable : IDisposable
        {
            private readonly Dictionary<string, IDbConfiguration> configurationByEntityNamespace;
            private readonly string entityNameSpace;
            private readonly IDbConfiguration previousDbConfiguration;

            public OverwriteConfigurationDisposable(Dictionary<string, IDbConfiguration> configurationByEntityNamespace, string entityNameSpace, IDbConfiguration configuration)
            {
                this.configurationByEntityNamespace = configurationByEntityNamespace;
                this.entityNameSpace = entityNameSpace;
                previousDbConfiguration = configurationByEntityNamespace[entityNameSpace];
                configurationByEntityNamespace[entityNameSpace] = configuration;
            }

            public void Dispose()
            {
                configurationByEntityNamespace[entityNameSpace] = previousDbConfiguration;
            }
        }
    }
}