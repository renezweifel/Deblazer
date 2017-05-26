
using Dg.Deblazer.Internal;

namespace Dg.Deblazer.Visitors
{
    public class GetChildrenFromCacheVisitor : DbEntityVisitorBase
    {
        internal override void ProcessSingleEntity(IDbEntityInternal entity) => entity.EnableLoadingChildrenFromCache();

        internal override void ProcessSingleEntity(IDbEntityRefInternal entity) => entity.EnableLoadingChildrenFromCache();

        internal override void ProcessSingleEntity(IDbEntitySetInternal entity) => entity.EnableLoadingChildrenFromCache();
    }
}