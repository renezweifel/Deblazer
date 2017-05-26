using System;

namespace Dg.Deblazer.Configuration
{
    public interface IQueryLogger
    {
        bool DoWriteLog { get; }
        void WriteLog(string message, long? elapsedMilliseconds = null);
        void FlushLog();
        void IncrementQueryCountAndTime(int elapsedMilliseconds);
        int GetQueryElapsedMilliseconds();
        int GetQueryCount();
        void IncrementLoadedElementCount(int increment = 1);
        int GetLoadedElementCount();
        void LogTransaction(TimeSpan transactionDuration);
        int GetDbTransactionMaxElapsedMilliseconds();
        int GetDbTransactionTotalElapsedMilliseconds();
        int GetDbTransactionCount();
        void ResetQueryCount();
    }
}
