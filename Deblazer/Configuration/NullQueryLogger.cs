using System;

namespace Dg.Deblazer.Configuration
{
    internal class NullQueryLogger : IQueryLogger

    {
        public static readonly NullQueryLogger Instance = new NullQueryLogger();

        private NullQueryLogger()
        {
        }

        public bool DoWriteLog => false;

        public void FlushLog()
        {
        }

        public int GetDbTransactionCount() => 0;

        public int GetDbTransactionMaxElapsedMilliseconds() => 0;

        public int GetDbTransactionTotalElapsedMilliseconds() => 0;

        public int GetLoadedElementCount() => 0;

        public int GetQueryCount() => 0;

        public int GetQueryElapsedMilliseconds() => 0;

        public void IncrementLoadedElementCount(int increment = 1)
        {
        }

        public void IncrementQueryCountAndTime(int elapsedMilliseconds)
        {
        }

        public void LogTransaction(TimeSpan transactionDuration)
        {
        }

        public void ResetQueryCount()
        {
        }

        public void WriteLog(string message, long? elapsedMilliseconds = default(long?))
        {
        }
    }
}
