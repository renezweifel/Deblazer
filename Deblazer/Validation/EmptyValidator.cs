using System.Collections.Generic;

namespace Dg.Deblazer.Validation
{
    public class EmptyValidator : IDbEntityValidator
    {
        public static readonly EmptyValidator Instance = new EmptyValidator();

        // Singleton
        private EmptyValidator()
        {

        }

        private readonly IReadOnlyDictionary<DbEntity, IReadOnlyList<ValidationError>> emptyDictionary = new Dictionary<DbEntity, IReadOnlyList<ValidationError>>();

        public IReadOnlyDictionary<DbEntity, IReadOnlyList<ValidationError>> ValidateForDelete(IReadOnlyList<DbEntity> entities)
        {
            return emptyDictionary;
        }

        public IReadOnlyDictionary<DbEntity, IReadOnlyList<ValidationError>> ValidateForInsertOrUpdate(IReadOnlyList<DbEntity> entities)
        {
            return emptyDictionary;
        }
    }
}