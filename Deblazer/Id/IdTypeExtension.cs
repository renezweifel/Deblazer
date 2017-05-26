using System;

namespace Dg.Deblazer.Extensions
{
    public static class IdTypeExtension
    {
        public static bool IsIdType<TValue>()
        {
            return IsIdType(typeof(TValue));
        }

        public static bool IsIdType(Type idType)
        {
            return idType.IsGenericType && idType.GetGenericTypeDefinition() == typeof(Id<>);
        }

        public static bool IsNullableIdType(Type idType)
        {
            if (idType.IsNullable() && idType.IsGenericType)
            {
                var baseType = idType.GetGenericArguments()[0];
                return IsIdType(baseType);
            }

            return false;
        }

        public static bool IsNullableIdType<T>()
        {
            return IsNullableIdType(typeof(T));
        }
    }
}
