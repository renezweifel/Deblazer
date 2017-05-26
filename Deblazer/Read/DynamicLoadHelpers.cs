using System;
using System.Data;
using System.Data.SqlClient;
using System.Reflection;
using System.Xml.Linq;

namespace Dg.Deblazer.Read
{
    internal class DynamicLoadHelpers
    {
        internal static readonly MethodInfo GetSqlXmlMethod =
            typeof(SqlDataReader).GetMethod(nameof(SqlDataReader.GetSqlXml), new[] { typeof(int) });

        internal static readonly MethodInfo LoadXElement =
            typeof(XElement).GetMethod("Parse", new[] { typeof(string) });

        internal static readonly MethodInfo IsDBNullMethodInfo = typeof(IDataRecord).GetMethod(nameof(IDataRecord.IsDBNull), new[] { typeof(int) });
        internal static readonly MethodInfo GetBinaryMethodInfo = new Func<IDataRecord, int, object>(PrimitiveTypeFiller.GetBinary).Method;
        internal static readonly MethodInfo GetDecimalMethodInfo = new Func<IDataRecord, int, decimal>(PrimitiveTypeFiller.GetDecimal).Method;
        internal static readonly MethodInfo GetNullableDecimalMethodInfo = new Func<IDataRecord, int, decimal?>(PrimitiveTypeFiller.GetNullableDecimal).Method;
        internal static readonly MethodInfo GetNullableIntMethodInfo = new Func<IDataRecord, int, int?>(PrimitiveTypeFiller.GetNullableInt32).Method;
        internal static readonly MethodInfo GetNullableLongMethodInfo = new Func<IDataRecord, int, long?>(PrimitiveTypeFiller.GetNullableLong).Method;
        internal static readonly MethodInfo GetCharMathodInfo = new Func<IDataRecord, int, char>(PrimitiveTypeFiller.GetChar).Method;
        internal static readonly MethodInfo GetNullableCharMethodInfo = new Func<IDataRecord, int, char?>(PrimitiveTypeFiller.GetNullableChar).Method;
        internal static readonly MethodInfo GetIdParameterMethodInfo = new Func<IDataRecord, int, int>(PrimitiveTypeFiller.GetInt32).Method;
        internal static readonly MethodInfo GetLongIdParameterMethodInfo = new Func<IDataRecord, int, long>(PrimitiveTypeFiller.GetLong).Method;
        internal static readonly MethodInfo GetDateMethodInfo = new Func<IDataRecord, int, Date>(PrimitiveTypeFiller.GetDate).Method;
        internal static readonly MethodInfo GetNullableDateMethodInfo = new Func<IDataRecord, int, Date?>(PrimitiveTypeFiller.GetNullableDate).Method;
    }
}