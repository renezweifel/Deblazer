using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Linq;
using System.Data.SqlClient;
using System.Linq;
using Dg.Deblazer.Extensions;

namespace Dg.Deblazer.SqlUtils
{
    public static class SqlParameterUtils
    {
        public static SqlParameter Create(string paramName, object parameter)
        {
            var sqlParameter = parameter as SqlParameter;
            if (sqlParameter != null)
            {
                // We need the Clone() for the usage of WhereDb("Id > @id", SqlParameterUtils.Create("@id", 3)), because if the query is then executed twice
                // (with query.Clone().CountDb() and query.Clone().ToList() for example), the execution crashes because the code tries to add
                // the same sqlParameter to two different sqlParameterCollection, which seems not to be allowed...
                return (SqlParameter)((ICloneable)sqlParameter).Clone();
            }
            else
            {
                if (parameter is Binary)
                {
                    return new SqlParameter(paramName, ((Binary)parameter).ToArray());
                }
                else if (parameter is Date?)
                {
                    var param = new SqlParameter(paramName, SqlDbType.Date);
                    param.Value = parameter ?? DBNull.Value;
                    return (param);
                }
                else if (parameter is Date)
                {
                    var param = new SqlParameter(paramName, SqlDbType.Date);
                    param.Value = parameter;
                    return param;
                }
                else if (parameter is IEnumerable<int>)
                {
                    return ValuesToParameter(paramName, parameter as IEnumerable<int>);
                }
                else if (parameter is IEnumerable<int?>)
                {
                    return ValuesToParameter(paramName, parameter as IEnumerable<int?>);
                }
                else if (parameter is IEnumerable<long>)
                {
                    return ValuesToParameter(paramName, parameter as IEnumerable<long>);
                }
                else if (parameter is IEnumerable<long?>)
                {
                    return ValuesToParameter(paramName, parameter as IEnumerable<long?>);
                }
                else if (parameter is IEnumerable<string>)
                {
                    return ValuesToParameter(paramName, parameter as IEnumerable<string>);
                }
                else if (parameter is IConvertibleToInt32)
                {
                    return new SqlParameter(paramName, ((IConvertibleToInt32)parameter).ConvertToInt32());
                }
                else if (parameter is IConvertibleToInt64)
                {
                    return new SqlParameter(paramName, ((IConvertibleToInt64)parameter).ConvertToInt64());
                }
                else if (parameter != null
                    && IsIEnumerableOfIConvertibleToInt32(parameter.GetType()))
                {
                    var ints = ((System.Collections.IEnumerable)parameter)
                        .Cast<IConvertibleToInt32>()
                        .Select(c => c.ConvertToInt32());
                    return ValuesToParameter(paramName, ints);
                }
                else if (parameter != null
                    && IsIEnumerableOfIConvertibleToInt64(parameter.GetType()))
                {
                    var ints = ((System.Collections.IEnumerable)parameter)
                        .Cast<IConvertibleToInt64>()
                        .Select(c => c.ConvertToInt64());
                    return ValuesToParameter(paramName, ints);
                }
                else if (parameter is IId)
                {
                    return new SqlParameter(paramName, ((IId)parameter).Id);
                }
                else
                {
                    return new SqlParameter(paramName, parameter ?? DBNull.Value);
                }
            }
        }

        private static bool IsIEnumerableOfIConvertibleToInt32(Type type)
        {
            return type.GetInterfaces()
                .Any(t => t.IsGenericType
                    && t.GetGenericTypeDefinition() == typeof(IEnumerable<>)
                    && typeof(IConvertibleToInt32).IsAssignableFrom(t.GetGenericArguments()[0]));
        }

        private static bool IsIEnumerableOfIConvertibleToInt64(Type type)
        {
            return type.GetInterfaces()
                .Any(t => t.IsGenericType
                    && t.GetGenericTypeDefinition() == typeof(IEnumerable<>)
                    && typeof(IConvertibleToInt64).IsAssignableFrom(t.GetGenericArguments()[0]));
        }

        private static SqlParameter ValuesToParameter(string paramName, IEnumerable<int?> values)
        {
            return ValuesToParameter(paramName, values.Where(i => i.HasValue).Select(i => i.Value));
        }

        private static SqlParameter ValuesToParameter(string paramName, IEnumerable<int> values)
        {
            var sqlParam = new SqlParameter(paramName, SqlDbType.Structured);
            sqlParam.TypeName = "dbo.IntTableParameter";

            var tblPub = new DataTable("");
            tblPub.Columns.Add("Value", typeof(int));
            values.ForEach(i => tblPub.Rows.Add(i));
            sqlParam.Value = tblPub;
            return sqlParam;
        }

        private static SqlParameter ValuesToParameter(string paramName, IEnumerable<string> values)
        {
            var sqlParam = new SqlParameter(paramName, SqlDbType.Structured);
            sqlParam.TypeName = "dbo.StringTableParameter";

            var tblPub = new DataTable("");
            tblPub.Columns.Add("Value", typeof(string));
            values.ForEach(i => tblPub.Rows.Add(i));
            sqlParam.Value = tblPub;
            return sqlParam;
        }

        private static SqlParameter ValuesToParameter(string paramName, IEnumerable<long?> values)
        {
            return ValuesToParameter(paramName, values.Where(i => i.HasValue).Select(i => i.Value));
        }

        private static SqlParameter ValuesToParameter(string paramName, IEnumerable<long> values)
        {
            var sqlParam = new SqlParameter(paramName, SqlDbType.Structured);
            sqlParam.TypeName = "dbo.BigIntTableParameter";

            var tblPub = new DataTable("");
            tblPub.Columns.Add("Value", typeof(long));
            values.ForEach(i => tblPub.Rows.Add(i));
            sqlParam.Value = tblPub;
            return sqlParam;
        }
    }
}
