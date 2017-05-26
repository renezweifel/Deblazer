using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace Dg.Deblazer.Validation
{
    /// <summary>
    /// Provides an interface for all classes that may be validated. They must implement the method Validate() in order
    /// to populate the validationErrors.
    /// </summary>
    [Serializable]
    public abstract class ValidationBase : IValidatableObject
    {
        [NonSerialized]
        private ValidationErrorContainer errors;
        [NonSerialized]
        private bool hasBeenValidated;

        public ValidationErrorContainer Errors
        {
            get
            {
                if (errors == null)
                {
                    errors = new ValidationErrorContainer(this);
                }

                return errors;
            }
        }

        public List<ValidationError> ValidationErrors
        {
            get { return Validate(GetType().Name); }
        }

        public bool HasValidationErrors
        {
            get { return ValidationErrors.Count > 0; }
        }

        IEnumerable<ValidationResult> IValidatableObject.Validate(ValidationContext validationContext)
        {
            foreach (var validationError in ValidationErrors)
            {
                yield return new ValidationResult(
                    validationError.ErrorMessage,
                    new[] { validationError.PropertyName }
                    );
            }
        }

        public List<ValidationError> Validate(string formElementName)
        {
            return Validate(new[] { formElementName });
        }

        public List<ValidationError> Validate(IEnumerable<string> memberNames)
        {
            if (!hasBeenValidated)
            {
                hasBeenValidated = true;

                Errors.SetParentPropertyNames(memberNames);
                if (DoValidateForDelete())
                {
                    ValidateForDelete();
                }
                else
                {
                    Validate();
                }

                Errors.ClearParentPropertyNames();
            }

            return Errors.ValidationErrors;
        }

        public virtual bool DoValidateForDelete()
        {
            return false;
        }

        protected virtual void ValidateAutoForInsertOrUpdate()
        {
        }

        protected virtual void Validate()
        {
            ValidateAutoForInsertOrUpdate();
        }

        protected virtual void ValidateForDelete()
        {
        }

        protected void AddErrorForInsertOrUpdate(ValidationBase validationObject, string memberName)
        {
            if (validationObject != null)
            {
                var memberNames = new List<string>(Errors.ParentPropertyNames);
                if (!string.IsNullOrEmpty(memberName))
                {
                    memberNames.Add(memberName);
                }

                var localValidationErrors = validationObject.Validate(memberNames);
                if (localValidationErrors != null && localValidationErrors.Count > 0)
                {
                    Errors.ValidationErrors.AddRange(localValidationErrors);
                }
            }
        }
    }
}