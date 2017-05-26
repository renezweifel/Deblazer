using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using FastMember;

namespace Dg.Deblazer.Read
{
    internal class SqlBulkCopyDataReader : IDataReader
    {
        private readonly SqlBulkCopy sqlBulkCopy;
        private IEnumerator source;
        Dictionary<string, object> current;
        private readonly IReadOnlyList<string> memberNames;
        private readonly Type[] effectiveTypes;
        private readonly BitArray allowNull;

        internal SqlBulkCopyDataReader(Type type, IEnumerable<Dictionary<string, object>> sourceValues, IReadOnlyList<string> members, SqlBulkCopy sqlBulkCopy)
        {
            this.sqlBulkCopy = sqlBulkCopy;
            this.memberNames = members;

            this.source = sourceValues.GetEnumerator();

            if (source == null) throw new ArgumentOutOfRangeException("source");

            var accessor = TypeAccessor.Create(type);
            if (accessor.GetMembersSupported)
            {
                var typeMembers = accessor.GetMembers();

                this.allowNull = new BitArray(members.Count);
                this.effectiveTypes = new Type[members.Count];
                for (int i = 0; i < members.Count; i++)
                {
                    Type memberType = null;
                    bool allowNull = true;
                    string hunt = members[i];
                    foreach (var member in typeMembers)
                    {
                        if (member.Name == hunt)
                        {
                            if (memberType == null)
                            {
                                var tmp = member.Type;
                                memberType = Nullable.GetUnderlyingType(tmp) ?? tmp;

                                allowNull = !(memberType.IsValueType && memberType == tmp);

                                // but keep checking, in case of duplicates
                            }
                            else
                            {
                                memberType = null; // duplicate found; say nothing
                                break;
                            }
                        }
                    }
                    this.allowNull[i] = allowNull;
                    this.effectiveTypes[i] = memberType ?? typeof(object);
                }
            }

            this.current = null;
            this.memberNames = members;
        }

        void IDataReader.Close()
        {
            Dispose();
        }

        int IDataReader.Depth
        {
            get { return 0; }
        }

        DataTable IDataReader.GetSchemaTable()
        {
            // these are the columns used by DataTable load
            DataTable table = new DataTable
            {
                Columns =
                {
                    {"ColumnOrdinal", typeof(int)},
                    {"ColumnName", typeof(string)},
                    {"DataType", typeof(Type)},
                    {"ColumnSize", typeof(int)},
                    {"AllowDBNull", typeof(bool)}
                }
            };
            object[] rowData = new object[5];
            for (int i = 0; i < memberNames.Count; i++)
            {
                rowData[0] = i;
                rowData[1] = memberNames[i];
                rowData[2] = effectiveTypes == null ? typeof(object) : effectiveTypes[i];
                rowData[3] = -1;
                rowData[4] = allowNull == null ? true : allowNull[i];
                table.Rows.Add(rowData);
            }
            return table;
        }

        bool IDataReader.IsClosed
        {
            get { return source == null; }
        }

        bool IDataReader.NextResult()
        {
            return false;
        }

        bool IDataReader.Read()
        {
            if (source != null && source.MoveNext())
            {
                current = source.Current as Dictionary<string, object>;
                return true;
            }
            current = null;
            return false;
        }

        int IDataReader.RecordsAffected
        {
            get { return 0; }
        }
        /// <summary>
        /// Releases all resources used by the ObjectReader
        /// </summary>
        public void Dispose()
        {
            current = null;
        }

        int IDataRecord.FieldCount
        {
            get { return memberNames.Count; }
        }

        bool IDataRecord.GetBoolean(int i)
        {
            return (bool)this[i];
        }

        byte IDataRecord.GetByte(int i)
        {
            return (byte)this[i];
        }

        long IDataRecord.GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            byte[] s = (byte[])this[i];
            int available = s.Length - (int)fieldOffset;
            if (available <= 0) return 0;

            int count = Math.Min(length, available);
            Buffer.BlockCopy(s, (int)fieldOffset, buffer, bufferoffset, count);
            return count;
        }

        char IDataRecord.GetChar(int i)
        {
            return (char)this[i];
        }

        long IDataRecord.GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            string s = (string)this[i];
            int available = s.Length - (int)fieldoffset;
            if (available <= 0) return 0;

            int count = Math.Min(length, available);
            s.CopyTo((int)fieldoffset, buffer, bufferoffset, count);
            return count;
        }

        IDataReader IDataRecord.GetData(int i)
        {
            throw new NotSupportedException();
        }

        string IDataRecord.GetDataTypeName(int i)
        {
            return (effectiveTypes == null ? typeof(object) : effectiveTypes[i]).Name;
        }

        DateTime IDataRecord.GetDateTime(int i)
        {
            return (DateTime)this[i];
        }

        decimal IDataRecord.GetDecimal(int i)
        {
            return (decimal)this[i];
        }

        double IDataRecord.GetDouble(int i)
        {
            return (double)this[i];
        }

        Type IDataRecord.GetFieldType(int i)
        {
            return effectiveTypes == null ? typeof(object) : effectiveTypes[i];
        }

        float IDataRecord.GetFloat(int i)
        {
            return (float)this[i];
        }

        Guid IDataRecord.GetGuid(int i)
        {
            return (Guid)this[i];
        }

        short IDataRecord.GetInt16(int i)
        {
            return (short)this[i];
        }

        int IDataRecord.GetInt32(int i)
        {
            return (int)this[i];
        }

        long IDataRecord.GetInt64(int i)
        {
            return (long)this[i];
        }

        string IDataRecord.GetName(int i)
        {
            return memberNames[i];
        }

        int IDataRecord.GetOrdinal(string name)
        {
            return Array.IndexOf(memberNames.ToArray(), name);
        }

        string IDataRecord.GetString(int i)
        {
            return (string)this[i];
        }

        object IDataRecord.GetValue(int i)
        {
            return this[i];
        }

        int IDataRecord.GetValues(object[] values)
        {
            // duplicate the key fields on the stack
            var members = this.memberNames;
            var current = this.current;

            int count = Math.Min(values.Length, members.Count);
            for (int i = 0; i < count; i++) values[i] = current[memberNames[i]] ?? DBNull.Value;
            return count;
        }

        bool IDataRecord.IsDBNull(int i)
        {
            return this[i] is DBNull;
        }

        object IDataRecord.this[string name]
        {
            get { return current[name] ?? DBNull.Value; }

        }
        /// <summary>
        /// Gets the value of the current object in the member specified
        /// </summary>
        public object this[int i]
        {
            get
            {
                var value = current[memberNames[i]] ?? DBNull.Value;
                if (value is decimal)
                {
                    return CircumventSqlBulkCopyBug(i, value);
                }
                return value;
            }
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
            // The following code does the same as those two lines would except not producing a compiler error
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
    }
}