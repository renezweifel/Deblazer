using System.ComponentModel;
using System.Globalization;

namespace System
{
    public class DateConverter : TypeConverter
    {
        private static readonly DateTimeConverter dateTimeConverter = new DateTimeConverter();

        public override bool CanConvertFrom(ITypeDescriptorContext context, Type sourceType)
        {
            return dateTimeConverter.CanConvertFrom(context, sourceType);
        }

        public override bool CanConvertTo(ITypeDescriptorContext context, Type destinationType)
        {
            return dateTimeConverter.CanConvertTo(context, destinationType);
        }

        public override object ConvertFrom(ITypeDescriptorContext context, CultureInfo culture, object value)
        {
            return (Date)(DateTime)dateTimeConverter.ConvertFrom(context, culture, value);
        }

        public override object ConvertTo(ITypeDescriptorContext context, CultureInfo culture, object value, Type destinationType)
        {
            return dateTimeConverter.ConvertTo(context, culture, (DateTime)(Date)value, destinationType);
        }
    }
}