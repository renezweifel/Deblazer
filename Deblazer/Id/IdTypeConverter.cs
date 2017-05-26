using System;
using System.ComponentModel;
using System.Globalization;

namespace Dg.Deblazer
{
    public class IdTypeConverter : TypeConverter
    {
        private readonly Type convertedType;
        public IdTypeConverter(Type type)
        {
            convertedType = type;
        }

        public override bool CanConvertFrom(
            ITypeDescriptorContext context,
            Type sourceType)
        {
            if (sourceType == typeof(string))
            {
                return true;
            }

            return base.CanConvertFrom(context, sourceType);
        }

        public override object ConvertFrom(ITypeDescriptorContext context, CultureInfo culture, object value)
        {
            if (value is string)
            {
                var intvalue = int.Parse((string)value);
                return Activator.CreateInstance(convertedType, intvalue);
            }

            return base.ConvertFrom(context, culture, value);
        }
    }
}
