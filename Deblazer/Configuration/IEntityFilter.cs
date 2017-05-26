using System.Collections.Generic;
using Dg.Deblazer.Settings;

namespace Dg.Deblazer.Configuration
{
    public interface IEntityFilter
    {
        void PreventInvalidOperations(IDbSettings dbSettings, IReadOnlyList<DbEntity> toDelete, IEnumerable<DbEntity> toInsert, IReadOnlyList<DbEntity> toUpdate);

        bool DoReturnEntity(IDbSettings dbSettings, object entity, IReadOnlyList<object> joinedEntities = null);
    }
}