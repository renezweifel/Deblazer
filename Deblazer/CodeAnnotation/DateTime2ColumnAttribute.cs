using System;

namespace Dg.Deblazer.CodeAnnotation
{
    public sealed class DateTime2ColumnAttribute : Attribute
    {
        public int Precision { get; private set; }

        public DateTime2ColumnAttribute(int precision)
        {
            Precision = precision;
        }
    }
}