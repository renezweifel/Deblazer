using System;
using System.Collections.Generic;

namespace Dg.Deblazer
{
    public class IdComparer<TIId> : IComparer<TIId>, IEqualityComparer<TIId> where TIId : IId
    {
        public bool Equals(TIId x, TIId y)
        {
            return EqualsStatic(x, y);
        }

        public int GetHashCode(TIId obj)
        {
            return typeof(TIId).GetHashCode() ^ obj.Id.GetHashCode();
        }

        public int Compare(TIId x, TIId y)
        {
            return x.Id - y.Id;
        }

        internal static bool EqualsStatic(TIId x, TIId y)
        {
            return (x == null && y == null) || (x != null && y != null && x.Id == y.Id);
        }
    }

    internal class IdComparer : IComparer<IId>, IEqualityComparer<IId>
    {
        public bool Equals(IId x, IId y)
        {
            return EqualsStatic(x, y);
        }

        public int GetHashCode(IId obj)
        {
            return obj.Id.GetHashCode();
        }

        public int Compare(IId x, IId y)
        {
            return x.Id - y.Id;
        }

        internal static bool EqualsStatic(IId x, IId y)
        {
            return (x == null && y == null) || (x != null && y != null && x.Id == y.Id && x.GetType() == y.GetType());
        }
    }

    internal class IdComparer<TIIdFirst, TIIdSecond> : IComparer<Tuple<TIIdFirst, TIIdSecond>>, IEqualityComparer<Tuple<TIIdFirst, TIIdSecond>>
        where TIIdFirst : IId where TIIdSecond : IId
    {
        public bool Equals(Tuple<TIIdFirst, TIIdSecond> x, Tuple<TIIdFirst, TIIdSecond> y)
        {
            return (x == null && y == null) || (x != null && y != null
                                                && IdComparer<TIIdFirst>.EqualsStatic(x.Item1, y.Item1) && IdComparer<TIIdSecond>.EqualsStatic(x.Item2, y.Item2));
        }

        public int GetHashCode(Tuple<TIIdFirst, TIIdSecond> obj)
        {
            return typeof(TIIdFirst).GetHashCode() ^ typeof(TIIdSecond).GetHashCode() ^ obj.Item1.Id.GetHashCode() ^ obj.Item2.Id.GetHashCode();
        }

        public int Compare(Tuple<TIIdFirst, TIIdSecond> x, Tuple<TIIdFirst, TIIdSecond> y)
        {
            return (x.Item1.Id + x.Item2.Id) - (y.Item1.Id + y.Item2.Id);
        }

    }
}