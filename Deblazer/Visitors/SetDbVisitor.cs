using Dg.Deblazer.Internal;

namespace Dg.Deblazer.Visitors
{
    internal class SetDbVisitor : DbEntityVisitorBase
    {
        private readonly BaseDb db;

        internal SetDbVisitor(BaseDb db)
        {
            this.db = db;
        }

        internal override void ProcessSingleEntity(IDbEntityInternal entity) => entity.SetDb(db);

        internal override void ProcessSingleEntity(IDbEntityRefInternal entity) => entity.SetDb(db);

        internal override void ProcessSingleEntity(IDbEntitySetInternal entity) => entity.SetDb(db);
    }
}