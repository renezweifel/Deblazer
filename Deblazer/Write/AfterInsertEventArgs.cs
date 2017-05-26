using System.Collections.Generic;
using System.Collections.Immutable;

namespace Dg.Deblazer.Write
{
    public class AfterInsertEventArgs
    {
        private readonly HashSet<DbEntity> entitiesToUpdate = new HashSet<DbEntity>();

        public void AddEntityToUpdate(DbEntity entity)
        {
            entitiesToUpdate.Add(entity);
        }

        public IImmutableSet<DbEntity> GetEntitiesToUpdate()
        {
            return entitiesToUpdate.ToImmutableHashSet();
        }
    }
}