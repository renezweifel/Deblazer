using System;
using System.Collections.Generic;
using System.Linq;
using Dg.Deblazer.Write;
using Dg.Deblazer.Extensions;

namespace Dg.Deblazer.Validation
{
    public abstract class ValidatorBase<T> : IDbEntityValidator where T : DbEntity
    {
        public IReadOnlyDictionary<DbEntity, IReadOnlyList<ValidationError>> ValidateForDelete(IReadOnlyList<DbEntity> entities)
        {
            IDbWrite writeDb = null;
            foreach (var entity in entities)
            {
                var db = entity?.DbInternal;
                if (db == null)
                {
                    throw new InvalidOperationException("The Db instance is NULL which is not allowed. You are probably trying to delete an Entity which is not yet in the DB and therefore can not be deleted.");
                }

                var currentWriteDb = db as IDbWrite;
                if (currentWriteDb == null)
                {
                    throw new InvalidOperationException("You are trying to validate for delete with a Db which does not support writes. You propably did not intend this.");
                }

                if (writeDb != null && currentWriteDb != writeDb)
                {
                    throw new InvalidOperationException("ValidateForDelete was called with entities from different Dbs. This is not valid because they are going to be submitted with the same Db.");
                }
                writeDb = writeDb ?? currentWriteDb;
            }

            return ValidateForDeleteGeneric(writeDb, entities.Where(e => e is T).Select(e => (T)e).ToList());
        }

        protected abstract IReadOnlyDictionary<DbEntity, IReadOnlyList<ValidationError>> ValidateForDeleteGeneric(IDbWrite writeDb, IReadOnlyList<T> entities);

        public IReadOnlyDictionary<DbEntity, IReadOnlyList<ValidationError>> ValidateForInsertOrUpdate(IReadOnlyList<DbEntity> entities)
        {
            return ValidateForInsertOrUpdateGeneric(entities.Where(e => e is T).Select(e => (T)e).ToList());
        }

        protected abstract IReadOnlyDictionary<DbEntity, IReadOnlyList<ValidationError>> ValidateForInsertOrUpdateGeneric(IReadOnlyList<T> entities);
    }
}
