using System;
using System.Collections.Generic;
using System.Data;

namespace Dg.Deblazer.Test
{
    internal class MockDataReader : IDataRecord
    {
        object IDataRecord.this[string name]
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        object IDataRecord.this[int i]
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        int IDataRecord.FieldCount
        {
            get
            {
                return mockRecordFields.Count;
            }
        }

        private readonly IReadOnlyList<MockRecordField> mockRecordFields;

        public MockDataReader(IReadOnlyList<MockRecordField> recordFields)
        {
            mockRecordFields = recordFields;
        }

        bool IDataRecord.GetBoolean(int i)
        {
            return (bool)mockRecordFields[i].Value;
        }

        byte IDataRecord.GetByte(int i)
        {
            return (byte)mockRecordFields[i].Value;
        }

        long IDataRecord.GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            return (long)mockRecordFields[i].Value;
        }

        char IDataRecord.GetChar(int i)
        {
            return (char)mockRecordFields[i].Value;
        }

        long IDataRecord.GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            return (long)mockRecordFields[i].Value;
        }

        IDataReader IDataRecord.GetData(int i)
        {
            return (IDataReader)mockRecordFields[i].Value;
        }

        string IDataRecord.GetDataTypeName(int i)
        {
            return mockRecordFields[i].DataTypeName;
        }

        DateTime IDataRecord.GetDateTime(int i)
        {
            if (mockRecordFields[i].DataTypeName == "date")
            {
                return (DateTime)(Date)mockRecordFields[i].Value;
            }
            return (DateTime)mockRecordFields[i].Value;
        }

        decimal IDataRecord.GetDecimal(int i)
        {
            return (decimal)mockRecordFields[i].Value;
        }

        double IDataRecord.GetDouble(int i)
        {
            return (double)mockRecordFields[i].Value;
        }

        Type IDataRecord.GetFieldType(int i)
        {
            return mockRecordFields[i].Type;
        }

        float IDataRecord.GetFloat(int i)
        {
            return (float)mockRecordFields[i].Value;
        }

        Guid IDataRecord.GetGuid(int i)
        {
            return (Guid)mockRecordFields[i].Value;
        }

        short IDataRecord.GetInt16(int i)
        {
            return (short)mockRecordFields[i].Value;
        }

        int IDataRecord.GetInt32(int i)
        {
            return (int)mockRecordFields[i].Value;
        }

        long IDataRecord.GetInt64(int i)
        {
            return (long)mockRecordFields[i].Value;
        }

        string IDataRecord.GetName(int i)
        {
            return mockRecordFields[i].FieldName;
        }

        int IDataRecord.GetOrdinal(string name)
        {
            throw new NotImplementedException();
        }

        string IDataRecord.GetString(int i)
        {
            return (string)mockRecordFields[i].Value;
        }

        object IDataRecord.GetValue(int i)
        {
            return mockRecordFields[i].Value;
        }

        int IDataRecord.GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        bool IDataRecord.IsDBNull(int i)
        {
            var obj = mockRecordFields[i].Value;
            return obj == null;
        }
    }
}