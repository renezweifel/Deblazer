using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dg.Deblazer.Test
{
    internal class MockRecordField
    {
        public readonly object Value;
        public readonly string DataTypeName;
        public readonly string FieldName;
        public readonly Type Type;

        public MockRecordField(object value, string dataTypeName, string fieldName, Type type)
        {
            this.Value = value;
            this.DataTypeName = dataTypeName;
            this.FieldName = fieldName;
            this.Type = type;
        }

        public MockRecordField(object value, string fieldName, Type type)
        {
            this.Value = value;
            this.DataTypeName = type.Name;
            this.FieldName = fieldName;
            this.Type = type;
        }
    }
}
