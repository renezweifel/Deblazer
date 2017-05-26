using System.Collections.Generic;
using System.Linq;
using Dg.Deblazer.Extensions;

namespace Dg.Deblazer.Validation
{
    public class CombinedValidator : IDbEntityValidator
    {
        public readonly IReadOnlyList<IDbEntityValidator> Validators;

        public CombinedValidator(IReadOnlyList<IDbEntityValidator> validators)
        {
            Validators = validators?.ToList() ?? new List<IDbEntityValidator>();
        }

        public IReadOnlyDictionary<DbEntity, IReadOnlyList<ValidationError>> ValidateForDelete(IReadOnlyList<DbEntity> entities)
        {
            var validationErrors = new Dictionary<DbEntity, IReadOnlyList<ValidationError>>();
            foreach (var validator in Validators)
            {
                validator.ValidateForDelete(entities).ForEach(kvp => Update(kvp, validationErrors));
            }

            return validationErrors;
        }

        public IReadOnlyDictionary<DbEntity, IReadOnlyList<ValidationError>> ValidateForInsertOrUpdate(IReadOnlyList<DbEntity> entities)
        {
            var validationErrors = new Dictionary<DbEntity, IReadOnlyList<ValidationError>>();
            foreach (var validator in Validators)
            {
                validator.ValidateForInsertOrUpdate(entities).ForEach(kvp => Update(kvp, validationErrors));
            }

            return validationErrors;
        }

        private static void Update(KeyValuePair<DbEntity, IReadOnlyList<ValidationError>> kvp, Dictionary<DbEntity, IReadOnlyList<ValidationError>> validationErrors)
        {
            IReadOnlyList<ValidationError> existingErrors;
            if (!validationErrors.TryGetValue(kvp.Key, out existingErrors))
            {
                validationErrors.Add(kvp.Key, kvp.Value);
            }
            else
            {
                validationErrors[kvp.Key] = existingErrors.Concat(kvp.Value).ToList();
            }
        }
    }
}
