using Dg.Deblazer.Internal;

namespace Dg.Deblazer.Visitors
{
    public class ConvertToReadOnlyVisitor : DbEntityVisitorBase
    {
        internal override void ProcessSingleEntity(IDbEntityInternal entity) => entity.MakeReadOnly();

        internal override void ProcessSingleEntity(IDbEntityRefInternal entity) => entity.MakeReadOnly();

        internal override void ProcessSingleEntity(IDbEntitySetInternal entity) => entity.MakeReadOnly();
    }
}