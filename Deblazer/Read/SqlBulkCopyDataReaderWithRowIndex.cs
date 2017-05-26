using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq.Expressions;
using Dg.Deblazer.SqlGeneration;
using FastMember;

namespace Dg.Deblazer.Read
{
    internal class SqlBulkCopyDataReaderWithRowIndex : IDataReader
    {
        private readonly IDataReader sourceDataReader;
        private readonly int rowIndexForSqlBulkCopyColumnIndex;
        private int rowIndexForSqlBulkCopy = 0;
        private readonly SqlBulkCopy sqlBulkCopy;

        internal SqlBulkCopyDataReaderWithRowIndex(Type type, IEnumerable source, SqlBulkCopy sqlBulkCopy)
        {
            var memberNames = GetMemberNames(type);
            rowIndexForSqlBulkCopyColumnIndex = memberNames.Length - 1;
            this.sourceDataReader = new ObjectReader(type, source, memberNames);
            this.sqlBulkCopy = sqlBulkCopy;
        }

        private static string[] GetMemberNames(Type type)
        {
            var tableMemberNames = QueryHelpers.GetColumnsInInsertStatement(type);
            var bulkCopyTableMemberNames = new string[tableMemberNames.Length + 1];
            tableMemberNames.CopyTo(bulkCopyTableMemberNames, index: 0);
            bulkCopyTableMemberNames[bulkCopyTableMemberNames.Length - 1] = "RowIndexForSqlBulkCopy";
            return bulkCopyTableMemberNames;
        }

        public object this[string name]
        {
            get
            {
                if (name == "RowIndexForSqlBulkCopy")
                {
                    return rowIndexForSqlBulkCopy;
                }

                return sourceDataReader[name];
            }
        }

        public object this[int i]
        {
            get
            {
                return sourceDataReader[i];
            }
        }

        public int Depth
        {
            get
            {
                return sourceDataReader.Depth;
            }
        }

        public int FieldCount
        {
            get
            {
                return sourceDataReader.FieldCount;
            }
        }

        public bool IsClosed
        {
            get
            {
                return sourceDataReader.IsClosed;
            }
        }

        public int RecordsAffected
        {
            get
            {
                return sourceDataReader.RecordsAffected;
            }
        }

        public void Close()
        {
            sourceDataReader.Close();
        }

        public void Dispose()
        {
            sourceDataReader.Dispose();
        }

        public bool GetBoolean(int i)
        {
            return sourceDataReader.GetBoolean(i);
        }

        public byte GetByte(int i)
        {
            return sourceDataReader.GetByte(i);
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            return sourceDataReader.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
        }

        public char GetChar(int i)
        {
            return sourceDataReader.GetChar(i);
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            return sourceDataReader.GetChars(i, fieldoffset, buffer, bufferoffset, length);
        }

        public IDataReader GetData(int i)
        {
            return sourceDataReader.GetData(i);
        }

        public string GetDataTypeName(int i)
        {
            return sourceDataReader.GetDataTypeName(i);
        }

        public DateTime GetDateTime(int i)
        {
            return sourceDataReader.GetDateTime(i);
        }

        public decimal GetDecimal(int i)
        {
            return sourceDataReader.GetDecimal(i);
        }

        public double GetDouble(int i)
        {
            return sourceDataReader.GetDouble(i);
        }

        public Type GetFieldType(int i)
        {
            return sourceDataReader.GetFieldType(i);
        }

        public float GetFloat(int i)
        {
            return sourceDataReader.GetFloat(i);
        }

        public Guid GetGuid(int i)
        {
            return sourceDataReader.GetGuid(i);
        }

        public short GetInt16(int i)
        {
            return sourceDataReader.GetInt16(i);
        }

        public int GetInt32(int i)
        {
            return sourceDataReader.GetInt32(i);
        }

        public long GetInt64(int i)
        {
            return sourceDataReader.GetInt64(i);
        }

        public string GetName(int i)
        {
            return sourceDataReader.GetName(i);
        }

        public int GetOrdinal(string name)
        {
            return sourceDataReader.GetOrdinal(name);
        }

        public DataTable GetSchemaTable()
        {
            return sourceDataReader.GetSchemaTable();
        }

        public string GetString(int i)
        {
            return sourceDataReader.GetString(i);
        }

        public object GetValue(int i)
        {
            if (i == rowIndexForSqlBulkCopyColumnIndex)
            {
                return rowIndexForSqlBulkCopy;
            }

            var value = sourceDataReader.GetValue(i);
            if (value is decimal)
            {
                return CircumventSqlBulkCopyBug(i, value);
            }

            return value;
        }

        private System.Data.SqlTypes.SqlDecimal CircumventSqlBulkCopyBug(int i, object value)
        {
            // Workaround for Microsoft Bug (caused SUP-917)
            var precisionAndScale = accessorForCircumventSqlBulkCopyBug(sqlBulkCopy, i);
            var sqlDecimal = new System.Data.SqlTypes.SqlDecimal((decimal)value);
            // We create an SqlDecimal with the same precision and scale as in the metadata. SqlBulkCopy can handle this.
            sqlDecimal = System.Data.SqlTypes.SqlDecimal.ConvertToPrecScale(sqlDecimal, precision: precisionAndScale.Item1, scale: precisionAndScale.Item2);
            return sqlDecimal;
        }

        private static readonly Func<SqlBulkCopy, int, Tuple<byte, byte>> accessorForCircumventSqlBulkCopyBug = CompileAccessorForCircumventSqlBulkCopyBug();

        private static Func<SqlBulkCopy, int, Tuple<byte, byte>> CompileAccessorForCircumventSqlBulkCopyBug()
        {
            // The following code does the same as those two lines would except producing a compiler error
            //   var metadata = sqlBulkCopy._sortedColumnMappings[3]._metadata;
            //   return new Tuple<byte, byte>(metadata.precision, metadata.scale);
            var sqlBulkCopyParameter = Expression.Parameter(typeof(SqlBulkCopy), "sqlBulkCopy");
            var columnIndexParameter = Expression.Parameter(typeof(int), "columnIndex");

            var sortedColumnMappingsExpression = Expression.Field(sqlBulkCopyParameter, "_sortedColumnMappings");
            var typeInList = typeof(SqlBulkCopy).Assembly.GetType("System.Data.SqlClient._ColumnMapping");
            var listType = typeof(List<>).MakeGenericType(typeInList);
            var indexer = listType.GetProperty("Item");
            var indexerExpression = Expression.Property(sortedColumnMappingsExpression, indexer, new[] { columnIndexParameter });
            var metadataExpression = Expression.Field(indexerExpression, "_metadata");
            var precisionExpression = Expression.Field(metadataExpression, "precision");
            var scaleExpression = Expression.Field(metadataExpression, "scale");

            var tupleConstructor = typeof(Tuple<byte, byte>).GetConstructor(new[] { typeof(byte), typeof(byte) });
            var createTupleExpression = Expression.New(tupleConstructor, arguments: new[] { precisionExpression, scaleExpression });

            var lambda = Expression.Lambda<Func<SqlBulkCopy, int, Tuple<byte, byte>>>(
                body: createTupleExpression,
                parameters: new[] { sqlBulkCopyParameter, columnIndexParameter });

            return lambda.Compile();
        }

        public int GetValues(object[] values)
        {
            return sourceDataReader.GetValues(values);
        }

        public bool IsDBNull(int i)
        {
            return sourceDataReader.IsDBNull(i);
        }

        public bool NextResult()
        {
            throw new NotImplementedException();
        }

        public bool Read()
        {
            rowIndexForSqlBulkCopy++;
            return sourceDataReader.Read();
        }
    }
}