using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Dg.Deblazer.Comparer
{
    /// <summary>
    /// A generic object comparer that uses only the object's reference,
    /// ignoring any <see cref="IEquatable{T}"/> or <see cref="object.Equals(object)"/>  overrides.
    /// </summary>
    public class ObjectReferenceEqualityComparer<T> : IEqualityComparer<T>
        where T : class
    {
        public bool Equals(T x, T y)
        {
            return ReferenceEquals(x, y);
        }

        public int GetHashCode(T obj)
        {
            return RuntimeHelpers.GetHashCode(obj);
        }
    }
}


