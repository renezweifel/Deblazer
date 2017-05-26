namespace Dg.Deblazer.Configuration
{
    internal class NullQueryLoggingHandler : IQueryLoggingHandler
    {
        public static readonly NullQueryLoggingHandler Instance = new NullQueryLoggingHandler();

        private NullQueryLoggingHandler()
        {
        }

        public IQueryLogger QueryLogger => NullQueryLogger.Instance;
    }
}