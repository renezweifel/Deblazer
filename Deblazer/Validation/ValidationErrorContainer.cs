using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Dg.Deblazer.Utils;
using Dg.Deblazer.Extensions;

namespace Dg.Deblazer.Validation
{
    [Serializable]
    public sealed class ValidationErrorContainer
    {
        private readonly object parent;
        [NonSerialized]
        private readonly List<string> parentPropertyNames = new List<string>();
        [NonSerialized]
        private List<ValidationError> validationErrors;

        public ValidationErrorContainer(object parent)
        {
            this.parent = parent;
        }

        public List<string> ParentPropertyNames
        {
            get { return parentPropertyNames; }
        }

        public string ParentPropertyFullName => string.Join(".", parentPropertyNames);

        public List<ValidationError> ValidationErrors
        {
            get
            {
                if (validationErrors == null)
                {
                    validationErrors = new List<ValidationError>();
                }

                return validationErrors;
            }
        }

        public bool HasValidationErrors
        {
            get { return ValidationErrors.Count > 0; }
        }

        internal void SetParentPropertyNames(IEnumerable<string> parentPropertyNames)
        {
            ClearParentPropertyNames();
            this.parentPropertyNames.AddRange(parentPropertyNames);
        }

        public void ClearParentPropertyNames()
        {
            parentPropertyNames.Clear();
        }

        public static string GetMemberName<T>(Expression<Func<T>> expression)
        {
            var memberNames = expression.GetMemberNames(false);
            return memberNames.LastOrDefault() ?? string.Empty;
            //var prefix = new StringBuilder();
            //for (int i = 0; i < memberNames.Count; i++)
            //{
            //    if (i > 0)
            //    {
            //        prefix.Append(".");
            //    }

            //    prefix.Append(memberNames[i]);
            //}

            //return prefix.ToString();

        }

        public void AddError<T>(Expression<Func<T>> expression, object value, string message, params TranslationParameter[] stringFormatObjects)
        {
            AddErrorWithValue(expression, value, message, stringFormatObjects);
        }

        public void AddError(string memberName, object value, string message, params TranslationParameter[] stringFormatObjects)
        {
            ValidationErrors.Add(new ValidationError(message, memberName, ParentPropertyFullName, value, parent, stringFormatObjects: stringFormatObjects));
        }

        public void AddErrors(ValidationErrorContainer errors)
        {
            AddErrors(errors.ValidationErrors);
        }

        public void AddErrors(ValidationBase item)
        {
            if (item != null)
            {
                AddErrors(item.ValidationErrors);
            }
        }

        public void AddErrors(List<ValidationError> errors)
        {
            ValidationErrors.AddRange(errors.Where(v => !ValidationErrors.Any(e => e.ErrorMessage == v.ErrorMessage)));
        }

        public void AddErrors<TItem>(IEnumerable<TItem> items) where TItem : ValidationBase
        {
            foreach (var item in items)
            {
                AddErrors(item);
            }
        }

        public void AddError(ValidationError validationError)
        {
            ValidationErrors.Add(validationError);
        }

        public void AddCustomError(string message, params TranslationParameter[] stringFormatObjects)
        {
            ValidationErrors.Add(new ValidationError(message, null, ParentPropertyFullName, null, parent, stringFormatObjects));
        }

        public void AddErrorWithValue<T>(
            Expression<Func<T>> expression,
            object attemptedValue,
            string message,
            params TranslationParameter[] stringFormatObjects)
        {
            string memberName = GetMemberName(expression);
            ValidationErrors.Add(new ValidationError(message, memberName, ParentPropertyFullName, attemptedValue, parent, stringFormatObjects));
        }

        public void AddErrorWithValue(
            string memberName,
            object attemptedValue,
            string message,
            params TranslationParameter[] stringFormatObjects)
        {
            ValidationErrors.Add(new ValidationError(message, memberName, ParentPropertyFullName, attemptedValue, parent, stringFormatObjects));
        }
    }
}