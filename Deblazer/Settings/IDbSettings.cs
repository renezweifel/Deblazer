using System;

namespace Dg.Deblazer.Settings
{
    public interface IDbSettings
    {
        bool CheckUserRights { get; }
        bool EnableCache { get; }
        string ConnectionString { get; }
        TimeSpan CommandTimeout { get; }
        bool AllowLoadingBinaryData { get; }
        bool WithNoLock { get; }
    }
}