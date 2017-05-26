using System;

namespace Dg.Deblazer.CodeAnnotation
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public sealed class StringColumnAttribute : Attribute
    {
        public int MaxLength { get; private set; }

        public bool CanBeNull { get; private set; }

        public StringColumnAttribute(int maxLength, bool canBeNull)
        {
            MaxLength = maxLength;
            CanBeNull = canBeNull;
        }
    }
}