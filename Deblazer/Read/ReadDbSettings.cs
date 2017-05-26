using System;
using Dg.Deblazer.Settings;

namespace Dg.Deblazer.Read
{
    public class ReadDbSettings : IDbSettings
    {
        public static readonly TimeSpan CommandTimeoutDefault = TimeSpan.FromSeconds(60);

        public bool AllowLoadingBinaryData { get; internal set; }
        public bool CheckUserRights { get; internal set; }
        public TimeSpan CommandTimeout { get; set; }
        public virtual string ConnectionString { get; }
        public bool EnableCache { get; internal set; }

        public bool ShowEntitiesFromOtherMandators { get; set; }
        public bool WithNoLock { get; internal set; }

        public ReadDbSettings(
            string connectionString,
            bool checkUserRights = true)
        {
            ConnectionString = connectionString;
            CheckUserRights = checkUserRights;
            ShowEntitiesFromOtherMandators = false;
            EnableCache = true;
            WithNoLock = false;
            CommandTimeout = CommandTimeoutDefault;
            AllowLoadingBinaryData = false;
        }
    }
}