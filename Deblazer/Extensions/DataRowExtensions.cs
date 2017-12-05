using System;
using System.Data;

namespace Dg.Deblazer.Extensions
{
    public static class DataRowExtensionsCustom
    {
        public static T GetOr<T>(object obj, T valueIfNull)
        {
            Type type = GetValueType<T>();
            // We need to convert this here, otherwise it would crash when doing MaxDb(RowVersion) because the db layer will try to do a Binary for RowVersion out of a byte[].
            if (obj is byte[] && typeof(byte[]) == typeof(T))
            {
                return (T)obj;
            }
            if(obj is DateTime && typeof(Date) == typeof(T))
            {
                var date = new Date((DateTime)obj);
                return (T)(object)date;
            }

            return NotNull(obj) ? (T)Convert.ChangeType(obj, type) : valueIfNull;
        }

        public static T Get<T>(object obj)
        {
            return GetOr(obj, default(T));
        }

        public static T Get<T>(this DataRow dr, int columnIndex)
        {
            return GetOr(dr, columnIndex, default(T));
        }

        public static T GetOr<T>(this DataRow dr, int columnIndex, T valueIfNull)
        {
            return GetOr(dr[columnIndex], valueIfNull);
        }

        private static Type GetValueType<T>()
        {
            Type type = typeof(T);
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                type = type.GetGenericArguments()[0];
            }

            return type;
        }

        public static bool NotNull(object obj)
        {
            return obj != null && Convert.IsDBNull(obj) == false;
        }
    }
}