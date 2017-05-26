using Dg.Deblazer.Internal;

namespace Dg.Deblazer.Visitors
{
    public class DoNotLazyLoadChildrenVisitor : DbEntityVisitorBase
    {
        internal override void ProcessSingleEntity(IDbEntityInternal entity) => entity.DisableLazyLoadChildren();

        internal override void ProcessSingleEntity(IDbEntityRefInternal entity) => entity.DisableLazyLoadChildren();

        internal override void ProcessSingleEntity(IDbEntitySetInternal entity) => entity.DisableLazyLoadChildren();
    }
}