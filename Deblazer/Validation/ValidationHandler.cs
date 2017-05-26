using System.Collections.Generic;
using System.Linq;
using Dg.Deblazer.Extensions;

namespace Dg.Deblazer.Validation
{
    public class ValidationHandler : IDbEntityValidator
    {
        private readonly IDbEntityValidatorFactory factory;

        public ValidationHandler(IDbEntityValidatorFactory factory)
        {
            this.factory = factory;
        }

        public IReadOnlyDictionary<DbEntity, IReadOnlyList<ValidationError>> ValidateForDelete(IReadOnlyList<DbEntity> entities)
        {
            var validationErrors = new Dictionary<DbEntity, IReadOnlyList<ValidationError>>();
            var entitiesByType = entities.ToLookup(e => e.GetType());
            foreach (var entitiesAndType in entitiesByType)
            {
                var validator = factory.GetValidator(entitiesAndType.Key);
                validator.ValidateForDelete(entitiesAndType.ToList()).ForEach(kvp => validationErrors.Add(kvp.Key, kvp.Value));
            }

            return validationErrors;
        }

        public IReadOnlyDictionary<DbEntity, IReadOnlyList<ValidationError>> ValidateForInsertOrUpdate(IReadOnlyList<DbEntity> entities)
        {
            var validationErrors = new Dictionary<DbEntity, IReadOnlyList<ValidationError>>();
            var entitiesByType = entities.ToLookup(e => e.GetType());
            foreach (var entitiesAndType in entitiesByType)
            {
                var validator = factory.GetValidator(entitiesAndType.Key);
                validator.ValidateForInsertOrUpdate(entitiesAndType.ToList()).ForEach(kvp => validationErrors.Add(kvp.Key, kvp.Value));
            }

            return validationErrors;
        }
    }
}
