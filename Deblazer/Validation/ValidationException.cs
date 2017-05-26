using System;
using System.Linq;
using Dg.Deblazer.Utils;
using Dg.Deblazer.Extensions;

namespace Dg.Deblazer.Validation
{
    /// <summary>
    /// The exception that is thrown when validation has failed.
    /// </summary>
    [Serializable]
    public class ValidationException : ApplicationException
    {
        /// <summary>
        /// A list of ValidationError objects which describe any validation rules that were broken.
        /// </summary>
        public readonly ValidationBase ValidatedObject;

        // public ValidationException(string message, ValidationException other) : this(other.ValidatedObject)
        // {
        // }

        /// <summary>
        /// Initializes a new instance of the ValidationException class.
        /// </summary>
        /// <param name="validatedObject">The validated object containing the list of ValidationErrors that contributed to the exception.</param>
        public ValidationException(ValidationBase validatedObject)
            : base($@"Exception in validation of {validatedObject.GetType().FullName}
({ObjectUtils.PropertiesToString(validatedObject)}):
{Environment.NewLine}                
{string.Join(Environment.NewLine, validatedObject.ValidationErrors.Select(
    e => (e.Source != null && e.Source != validatedObject ? (e.Source.GetType().FullName + ": "): "") + GetErrorMessage(e)))}")
        {
            ValidatedObject = validatedObject;
        }

        private static string GetErrorMessage(ValidationError validationError)
        {
            if (validationError.StringFormatObjects.NullOrNone())
            {
                return validationError.ErrorMessage;
            }

            var fortmattableString = validationError.ErrorMessage.Replace('[', '{').Replace(']', '}');
            return String.Format(fortmattableString, validationError.StringFormatObjects.Select(o => o.Parameter).ToArray());
        }
    }
}