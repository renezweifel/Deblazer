using JetBrains.Annotations;

namespace Dg.Deblazer.Configuration
{
    public interface IQueryLoggingHandler
    {
        [NotNull]
        IQueryLogger QueryLogger { get; }
    }
}
