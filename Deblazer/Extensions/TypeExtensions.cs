using JetBrains.Annotations;
using System;
using System.ComponentModel;

namespace Dg.Deblazer.Extensions
{
    public static class TypeExtensions
    {
        [CanBeNull]
        public static object Convert(this object sourceValue, Type targetType)
        {
            if (sourceValue == null)
            {
                return null;
            }

            var conv = TypeDescriptor.GetConverter(targetType);
            if (conv.CanConvertFrom(sourceValue.GetType()))
            {
                return conv.ConvertFrom(sourceValue);
            }

            if (conv.CanConvertFrom(typeof(string)))
            {
                return conv.ConvertFrom(sourceValue.ToString());
            }

            return System.Convert.ChangeType(sourceValue, targetType);
        }
    }
}