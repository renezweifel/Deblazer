using System;

namespace Dg.Deblazer
{
    public static class LongIdTypeExtension
    {
        public static bool IsLongIdType<TValue>()
        {
            return IsLongIdType(typeof(TValue));
        }

        public static bool IsLongIdType(Type idType)
        {
            return idType.IsGenericType && idType.GetGenericTypeDefinition() == typeof(LongId<>);
        }

        internal static bool IsNullable(this Type type)
        {
            return type.IsGenericType
                && type.IsGenericTypeDefinition == false
                && type.GetGenericTypeDefinition() == typeof(Nullable<>);
        }

        public static bool IsNullableLongIdType(Type idType)
        {
            if (idType.IsNullable() && idType.IsGenericType)
            {
                var baseType = idType.GetGenericArguments()[0];
                return IsLongIdType(baseType);
            }

            return false;
        }

        public static bool IsNullableLongIdType<T>()
        {
            return IsNullableLongIdType(typeof(T));
        }

        public static LongId<TILongId> ToTypedId<TILongId>(this long id) where TILongId : class, ILongId
        {
            return new LongId<TILongId>(id);
        }
    }
}
