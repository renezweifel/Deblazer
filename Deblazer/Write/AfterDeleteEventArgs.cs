using System.Collections.Generic;

namespace Dg.Deblazer.Write
{
    public class AfterDeleteEventArgs
    {
        internal readonly List<DbEntity> EntitiesToInsert = new List<DbEntity>();

        public void AddEntityToInsert(DbEntity entity)
        {
            EntitiesToInsert.Add(entity);
        }
    }
}
