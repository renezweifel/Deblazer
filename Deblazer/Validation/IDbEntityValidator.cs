using System.Collections.Generic;

namespace Dg.Deblazer.Validation
{
    public interface IDbEntityValidator
    {
        IReadOnlyDictionary<DbEntity, IReadOnlyList<ValidationError>> ValidateForDelete(IReadOnlyList<DbEntity> entities);
        IReadOnlyDictionary<DbEntity, IReadOnlyList<ValidationError>> ValidateForInsertOrUpdate(IReadOnlyList<DbEntity> entities);
    }
}