using System;

namespace Dg.Deblazer.Validation
{
    public class ValidationError
    {
        public ValidationError(
            string errorMessage,
            string propertyName = null,
            string parentPropertyName = null,
            object propertyValue = null,
            object source = null,
            params TranslationParameter[] stringFormatObjects)
        {
            ParentPropertyName = parentPropertyName;
            PropertyName = propertyName;
            PropertyValue = propertyValue;
            ErrorMessage = errorMessage;
            Source = source;
            StringFormatObjects = stringFormatObjects;
        }

        public TranslationParameter[] StringFormatObjects { get; }

        public object Source { get; }

        public string ParentPropertyName { get; }

        public string PropertyName { get; }

        public string PropertyFullName
        {
            get
            {
                return string.IsNullOrEmpty(ParentPropertyName)
                    ? PropertyName
                    : ParentPropertyName + "." + PropertyName;
            }
        }

        public object PropertyValue { get; }

        public string PropertyValueString
        {
            get
            {
                if (PropertyValue != null)
                {
                    if (PropertyValue is DateTime || PropertyValue is DateTime?)
                    {
                        return ((DateTime)PropertyValue).ToShortDateString();
                    }

                    return PropertyValue.ToString();
                }

                return string.Empty;
            }
        }

        public string ErrorMessage { get; set; }

        public override string ToString()
        {
            return ErrorMessage;
        }
    }
}