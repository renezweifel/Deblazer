using System.Collections.Generic;
using Dg.Deblazer.Settings;

namespace Dg.Deblazer.Configuration
{
    internal class EmptyEntityFilter : IEntityFilter
    {
        public static readonly EmptyEntityFilter Instance = new EmptyEntityFilter();

        private EmptyEntityFilter()
        {
        }

        public bool DoReturnEntity(IDbSettings dbSettings, object entity, IReadOnlyList<object> joinedEntities = null)
        {
            return true;
        }

        public void PreventInvalidOperations(IDbSettings dbSettings, IReadOnlyList<DbEntity> toDelete, IEnumerable<DbEntity> toInsert, IReadOnlyList<DbEntity> toUpdate)
        {
            // Nothing to do in EmptyEntityFilter
        }
    }
}