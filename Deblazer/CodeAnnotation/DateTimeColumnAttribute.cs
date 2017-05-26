using System;

namespace Dg.Deblazer.CodeAnnotation
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public sealed class DateTimeColumnAttribute : Attribute
    {
    }
}