using System;
using System.Collections.Concurrent;
using System.Data.SqlClient;
using Dg.Deblazer.ContextValues;
using Dg.Deblazer.ContextValues.DgSpecific;
using Dg.Deblazer.Read;

namespace Dg.Deblazer.Settings
{
    // 2MN TC Hm... Wenn wir eine neue Settings-Klasse erstellen, gibt es eine Exception... Das verstösst mMn gegen das Open/Closed-Prinzip.
    // Ist natürlich sowieso unschön, dass die Settings unterschiedliche Settings haben und trotzdem das selbe Interface implementieren.
    // Am besten lassen wir es erstmal so und ich erfasse eine Story, damit der DbLayer ein bisschen aufgeräumt wird. Die ganzen Vererbungen im Deblazer sind abgründig grauenvoll... *kotz*
    public static class DbSettingsExtensions
    {
        private static ConcurrentDictionary<string, string> databaseNameUnescapedByConnectionString = new ConcurrentDictionary<string, string>(concurrencyLevel: 1, capacity: 0);

        public static string GetDatabaseName(this IDbSettings dbSettings) => GetDatabaseNameUnescaped(dbSettings.ConnectionString);

        public static string GetDatabaseName(string connectionString)
        {
            var databaseNameUnescaped = GetDatabaseNameUnescaped(connectionString);
            return "[" + databaseNameUnescaped + "]";
        }
        public static string GetDatabaseNameUnescaped(this IDbSettings dbSettings) => GetDatabaseNameUnescaped(dbSettings.ConnectionString);

        public static string GetDatabaseNameUnescaped(string connectionString)
        {
            var databaseName = databaseNameUnescapedByConnectionString.GetOrAdd(
                connectionString,
                valueFactory: connString => new SqlConnectionStringBuilder(connString).InitialCatalog);
            return databaseName;
        }

        public static IDisposable BeginScope(this IDbSettings settings,
            TimeSpan? commandTimeout = null,
            bool? checkUserRights = null,
            bool? enableCache = null,
            bool? withNoLock = null,
            bool? allowLoadingBinaryData = null,
            bool? showEntitiesFromOtherMandators = null)
        {
            if (settings is ReadDbSettings)
            {
                return new ReadSettingsScope(
                    parentSettings: (ReadDbSettings)settings,
                    commandTimeout: commandTimeout,
                    checkUserRights: checkUserRights,
                    enableCache: enableCache,
                    withNoLock: withNoLock,
                    allowLoadingBinaryData: allowLoadingBinaryData,
                    showEntitiesFromOtherMandators: showEntitiesFromOtherMandators);
            }
            else if (settings is WriteDbSettings)
            {
                return new WriteSettingsScope(
                    parentSettings: (WriteDbSettings)settings,
                    commandTimeout: commandTimeout,
                    checkUserRights: checkUserRights,
                    enableCache: enableCache,
                    withNoLock: withNoLock,
                    allowLoadingBinaryData: allowLoadingBinaryData,
                    showEntitiesFromOtherMandators: showEntitiesFromOtherMandators,
                    returnPreviouslyLoadedEntity_Obsolete: null);
            }
            else
            {
                throw new NotSupportedException("This implementation of IDbSettings is not supported in BeginScope");
            }

        }

        public static IDisposable BeginScope(
            this WriteDbSettings settings,
            TimeSpan? commandTimeout = null,
            bool? checkUserRights = null,
            bool? enableCache = null,
            bool? withNoLock = null,
            bool? allowLoadingBinaryData = null,
            bool? showEntitiesFromOtherMandators = null,
            bool? returnPreviouslyLoadedEntity_Obsolete = null)
        {
            return new WriteSettingsScope(
                parentSettings: settings,
                commandTimeout: commandTimeout,
                checkUserRights: checkUserRights,
                enableCache: enableCache,
                withNoLock: withNoLock,
                allowLoadingBinaryData: allowLoadingBinaryData,
                showEntitiesFromOtherMandators: showEntitiesFromOtherMandators,
                returnPreviouslyLoadedEntity_Obsolete: returnPreviouslyLoadedEntity_Obsolete);
        }

        private class ReadSettingsScope : IDisposable
        {
            private ReadDbSettingsDataSet parentSettingsDataSet;
            private ReadDbSettings scopeSettings;

            public ReadSettingsScope(
                ReadDbSettings parentSettings,
                TimeSpan? commandTimeout,
                bool? checkUserRights,
                bool? enableCache,
                bool? withNoLock,
                bool? allowLoadingBinaryData,
                bool? showEntitiesFromOtherMandators)
            {
                parentSettingsDataSet = new ReadDbSettingsDataSet(
                    commandTimeout: parentSettings.CommandTimeout,
                    checkUserRights: parentSettings.CheckUserRights,
                    enableCache: parentSettings.EnableCache,
                    withNoLock: parentSettings.WithNoLock,
                    allowLoadingBinaryData: parentSettings.AllowLoadingBinaryData,
                    showEntitiesFromOtherMandators: parentSettings.ShowEntitiesFromOtherMandators);

                parentSettings.CheckUserRights = checkUserRights ?? parentSettings.CheckUserRights;
                parentSettings.CommandTimeout = commandTimeout ?? parentSettings.CommandTimeout;
                parentSettings.EnableCache = enableCache ?? parentSettings.EnableCache;
                parentSettings.WithNoLock = withNoLock ?? parentSettings.WithNoLock;
                parentSettings.AllowLoadingBinaryData = allowLoadingBinaryData ?? parentSettings.AllowLoadingBinaryData;
                parentSettings.ShowEntitiesFromOtherMandators = showEntitiesFromOtherMandators ?? parentSettings.ShowEntitiesFromOtherMandators;

                this.scopeSettings = parentSettings;
            }

            public void Dispose()
            {
                scopeSettings.CommandTimeout = parentSettingsDataSet.CommandTimeout;
                scopeSettings.CheckUserRights = parentSettingsDataSet.CheckUserRights;
                scopeSettings.EnableCache = parentSettingsDataSet.EnableCache;
                scopeSettings.WithNoLock = parentSettingsDataSet.WithNoLock;
                scopeSettings.AllowLoadingBinaryData = parentSettingsDataSet.AllowLoadingBinaryData;
                scopeSettings.ShowEntitiesFromOtherMandators = parentSettingsDataSet.ShowEntitiesFromOtherMandators;
            }
        }

        private class WriteSettingsScope : IDisposable
        {
            private WriteDbSettingsDataSet parentSettingsDataSet;
            private WriteDbSettings scopeSettings;

            public WriteSettingsScope(
                WriteDbSettings parentSettings,
                TimeSpan? commandTimeout,
                bool? checkUserRights,
                bool? enableCache,
                bool? withNoLock,
                bool? allowLoadingBinaryData,
                bool? showEntitiesFromOtherMandators,
                bool? returnPreviouslyLoadedEntity_Obsolete)
            {
                parentSettingsDataSet = new WriteDbSettingsDataSet(
                    commandTimeout: parentSettings.CommandTimeout,
                    checkUserRights: parentSettings.CheckUserRights,
                    enableCache: parentSettings.EnableCache,
                    withNoLock: parentSettings.WithNoLock,
                    allowLoadingBinaryData: parentSettings.AllowLoadingBinaryData,
                    showEntitiesFromOtherMandators: parentSettings.ShowEntitiesFromOtherMandators,
                    returnPreviouslyLoadedEntity_Obsolete: parentSettings.ReturnPreviouslyLoadedEntity_Obsolete);

                parentSettings.CheckUserRights = checkUserRights ?? parentSettings.CheckUserRights;
                parentSettings.CommandTimeout = commandTimeout ?? parentSettings.CommandTimeout;
                parentSettings.EnableCache = enableCache ?? parentSettings.EnableCache;
                parentSettings.WithNoLock = withNoLock ?? parentSettings.WithNoLock;
                parentSettings.AllowLoadingBinaryData = allowLoadingBinaryData ?? parentSettings.AllowLoadingBinaryData;
                parentSettings.ShowEntitiesFromOtherMandators = showEntitiesFromOtherMandators ?? parentSettings.ShowEntitiesFromOtherMandators;
                parentSettings.ReturnPreviouslyLoadedEntity_Obsolete = returnPreviouslyLoadedEntity_Obsolete ?? parentSettings.ReturnPreviouslyLoadedEntity_Obsolete;

                scopeSettings = parentSettings;
            }

            public void Dispose()
            {
                scopeSettings.CommandTimeout = parentSettingsDataSet.CommandTimeout;
                scopeSettings.CheckUserRights = parentSettingsDataSet.CheckUserRights;
                scopeSettings.EnableCache = parentSettingsDataSet.EnableCache;
                scopeSettings.WithNoLock = parentSettingsDataSet.WithNoLock;
                scopeSettings.AllowLoadingBinaryData = parentSettingsDataSet.AllowLoadingBinaryData;
                scopeSettings.ShowEntitiesFromOtherMandators = parentSettingsDataSet.ShowEntitiesFromOtherMandators;
                scopeSettings.ReturnPreviouslyLoadedEntity_Obsolete = parentSettingsDataSet.ReturnPreviouslyLoadedEntity_Obsolete;
            }
        }

        private struct ReadDbSettingsDataSet
        {
            public readonly TimeSpan CommandTimeout;
            public readonly bool CheckUserRights;
            public readonly bool EnableCache;
            public readonly bool WithNoLock;
            public readonly bool AllowLoadingBinaryData;
            public readonly bool ShowEntitiesFromOtherMandators;

            public ReadDbSettingsDataSet(
                TimeSpan commandTimeout,
                bool checkUserRights,
                bool enableCache,
                bool withNoLock,
                bool allowLoadingBinaryData,
                bool showEntitiesFromOtherMandators)
            {
                this.CommandTimeout = commandTimeout;
                this.CheckUserRights = checkUserRights;
                this.EnableCache = enableCache;
                this.WithNoLock = withNoLock;
                this.AllowLoadingBinaryData = allowLoadingBinaryData;
                this.ShowEntitiesFromOtherMandators = showEntitiesFromOtherMandators;
            }
        }

        private struct WriteDbSettingsDataSet
        {
            public readonly TimeSpan CommandTimeout;
            public readonly bool CheckUserRights;
            public readonly bool EnableCache;
            public readonly bool WithNoLock;
            public readonly bool AllowLoadingBinaryData;
            public readonly bool ShowEntitiesFromOtherMandators;
            public readonly bool ReturnPreviouslyLoadedEntity_Obsolete;

            public WriteDbSettingsDataSet(
                TimeSpan commandTimeout,
                bool checkUserRights,
                bool enableCache,
                bool withNoLock,
                bool allowLoadingBinaryData,
                bool showEntitiesFromOtherMandators,
                bool returnPreviouslyLoadedEntity_Obsolete)
            {
                this.CommandTimeout = commandTimeout;
                this.CheckUserRights = checkUserRights;
                this.EnableCache = enableCache;
                this.WithNoLock = withNoLock;
                this.AllowLoadingBinaryData = allowLoadingBinaryData;
                this.ShowEntitiesFromOtherMandators = showEntitiesFromOtherMandators;
                this.ReturnPreviouslyLoadedEntity_Obsolete = returnPreviouslyLoadedEntity_Obsolete;
            }
        }
    }
}