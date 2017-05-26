using Dg.Deblazer.AggregateUpdate;
using Dg.Deblazer.ContextValues.DgSpecific;
using Dg.Deblazer.Read;
using System;

namespace Dg.Deblazer.Settings
{
    public class WriteDbSettings : IDbSettings
    {
        /// <summary>
        /// This Property leads to weird behavior and should not be used. See https://jira.devinite.com/browse/ARH-139
        /// </summary>
        internal bool ReturnPreviouslyLoadedEntity_Obsolete { get; set; }

        public static bool UseSqlBulkCopyForUpdates_Experimental { get; set; }

        public bool CheckUserRights { get; internal set; }

        public bool AllowSubmitChangesOnDeviniteAndLocalRequest { get; set; }

        public TimeSpan CommandTimeout { get; internal set; }

        /// <summary>
        /// Use the optional parameter "transactionTimeout" in db.SubmitChanges() instead
        /// </summary>
        public TimeSpan SubmitChangesTimeout_Obsolete { get; set; }

        public virtual string ConnectionString { get; set; }

        public bool WithNoLock { get; internal set; }

        /// <summary>
        /// Use the optional parameter "aggregateUpdateMode" in db.SubmitChanges() instead
        /// </summary>
        public AggregateUpdateProcessingMode AggregateUpdateMode_Obsolete { get; set; }

        private bool supportSubmitChanges;

        /// <summary>
        /// OBSOLETE: Use ReadDb instead
        /// Set this to false in order to optimize memory usage and disable the loadedSet. Otherwise all items loaded from the same db are retained in its loadedSet and may not get garbage collected even if we remove them from the cache.
        /// </summary>
        /// <returns></returns>
        public bool SupportSubmitChanges_Obsolete
        {
            get { return supportSubmitChanges; }

            set
            {
                supportSubmitChanges = value;
                if (!value) { EnableCache = false; }
            }
        }

        public bool ShowEntitiesFromOtherMandators { get; internal set; }

        public bool EnableCache { get; internal set; }
        public void SetEnableCache_DoNotUse(bool value) => EnableCache = value;

        public bool AllowLoadingBinaryData { get; internal set; }

        /// <summary>
        /// This setting should not be set directly before and after SubmitChanges. Instead use the following pattern:
        /// <code>using(WebSubmitChangesConfigurationHandler.Instance.AllowSubmitChangesForSpecialCases(db)) { ... }</code>
        /// That way, the setting is set to true before and false after automatically.
        /// Alternatively you can add the Attribute [AllowSubmitChangesInGetRequest] to the action.
        /// </summary>
        public bool AllowSubmitChangesForSpecialCase { get; set; }

        private const int DefaultTransactionTimeoutMinutes = 1;

        public WriteDbSettings(string connectionString, bool allowSubmitChangesForSpecialCase = false)
        {
            ConnectionString = connectionString;
            ReturnPreviouslyLoadedEntity_Obsolete = false;
            EnableCache = true;
            ShowEntitiesFromOtherMandators = false;
            CheckUserRights = true;
            AllowSubmitChangesOnDeviniteAndLocalRequest = false;
            CommandTimeout = ReadDbSettings.CommandTimeoutDefault;
            SubmitChangesTimeout_Obsolete = TimeSpan.FromMinutes(DefaultTransactionTimeoutMinutes);
            WithNoLock = false;
            SupportSubmitChanges_Obsolete = true;
            AllowLoadingBinaryData = false;
            // 2TK BDA InSubmitChangesIfBelowChunkSize sollte ersetzt werden mit einem Eintrag von aussen. Ausserdem ist das AggregateUpdateMode_Obsolete --> obsoletes ausbauen
            AggregateUpdateMode_Obsolete = AggregateUpdateProcessingMode.InSubmitChanges;
            AllowSubmitChangesForSpecialCase = allowSubmitChangesForSpecialCase;
        }

        public WriteDbSettings Clone()
        {
            return new WriteDbSettings(connectionString: this.ConnectionString)
            {
                EnableCache = this.EnableCache,
                ShowEntitiesFromOtherMandators = this.ShowEntitiesFromOtherMandators,
                CheckUserRights = this.CheckUserRights,
                AllowSubmitChangesOnDeviniteAndLocalRequest = this.AllowSubmitChangesOnDeviniteAndLocalRequest,
                CommandTimeout = this.CommandTimeout,
                SubmitChangesTimeout_Obsolete = this.SubmitChangesTimeout_Obsolete,
                SupportSubmitChanges_Obsolete = this.SupportSubmitChanges_Obsolete,
                WithNoLock = this.WithNoLock,
                AllowLoadingBinaryData = this.AllowLoadingBinaryData,
                AggregateUpdateMode_Obsolete = this.AggregateUpdateMode_Obsolete,
                AllowSubmitChangesForSpecialCase = this.AllowSubmitChangesForSpecialCase,
              ReturnPreviouslyLoadedEntity_Obsolete = this.ReturnPreviouslyLoadedEntity_Obsolete
            };
        }
    }
}