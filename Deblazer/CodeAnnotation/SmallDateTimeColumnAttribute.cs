using System;

namespace Dg.Deblazer.CodeAnnotation
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public sealed class SmallDateTimeColumnAttribute : Attribute
    {
        public static readonly DateTime MinValue = new DateTime(1900, 1, 1);
        public static readonly DateTime MaxValue = new DateTime(2079, 6, 6);
    }
}