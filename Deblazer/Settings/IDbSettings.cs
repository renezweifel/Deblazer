using System;

namespace Dg.Deblazer.Settings
{
    public interface IDbSettings
    {
        // 2TC TC Dieses Setting sollte nicht hier sein. Ergibt semantisch keinerlei Sinn im Deblazer.
        bool ShowEntitiesFromOtherMandators { get; }
        bool CheckUserRights { get; }
        bool EnableCache { get; }
        string ConnectionString { get; }
        TimeSpan CommandTimeout { get; }
        bool AllowLoadingBinaryData { get; }
        bool WithNoLock { get; }
    }
}