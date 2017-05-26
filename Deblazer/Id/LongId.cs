using JetBrains.Annotations;
using System;
using System.Diagnostics;

namespace Dg.Deblazer
{
    public interface IConvertibleToInt64
    {
        long ConvertToInt64();
    }

    [Serializable]
    [DebuggerDisplay("{GetDebuggerDisplayString()}")]
    //[TypeConverter(typeof(LongIdTypeConverter))]
    public struct LongId<TILongId> : IEquatable<LongId<TILongId>>, IComparable<LongId<TILongId>>, IConvertibleToInt64, IComparable, ILongId
        where TILongId : ILongId
    {
        [CanBeNull]
        public static LongId<TILongId>? Nullable(long? rawId)
        {
            if (rawId == null)
            {
                return null;
            }

            return new LongId<TILongId>(rawId.Value);
        }

        public static bool TryParse(string s, out LongId<TILongId> result)
        {
            long i;
            if (long.TryParse(s, out i))
            {
                result = new LongId<TILongId>(i);
                return true;
            }

            result = new LongId<TILongId>(0);
            return false;
        }

        private readonly long rawId;


        // used by LongIdBuilder (using reflection)
        public LongId(long longId)
        {
            rawId = longId;
        }

        public override int GetHashCode()
        {
            return rawId.GetHashCode();
        }

        public bool Equals(LongId<TILongId> other)
        {
            return other.rawId == rawId;
        }

        public override bool Equals(object obj)
        {
            return obj is LongId<TILongId> && Equals((LongId<TILongId>)obj);
        }

        public static bool operator ==(LongId<TILongId> x, LongId<TILongId> y)
        {
            return x.Equals(y);
        }

        public static bool operator !=(LongId<TILongId> x, LongId<TILongId> y)
        {
            return !x.Equals(y);
        }

        public int CompareTo(LongId<TILongId> other)
        {
            return rawId.CompareTo(other.rawId);
        }

        public static bool operator >(LongId<TILongId> x, LongId<TILongId> y)
        {
            return x.rawId > y.rawId;
        }

        public static bool operator <(LongId<TILongId> x, LongId<TILongId> y)
        {
            return x.rawId < y.rawId;
        }

        public static bool operator >=(LongId<TILongId> x, LongId<TILongId> y)
        {
            return x.rawId >= y.rawId;
        }

        public static bool operator <=(LongId<TILongId> x, LongId<TILongId> y)
        {
            return x.rawId <= y.rawId;
        }

        public override string ToString()
        {
            // Only show LongId, so DbLayer-queries get cast correctly. Type information is shown in the DebuggerDisplay.
            return rawId.ToString();
        }

        /// <summary>
        /// Gets the LongId including table name (e.g. "Product/15")
        /// </summary>
        public string FullLongId => typeof(TILongId).Name + "/" + rawId;

        [System.Diagnostics.Contracts.Pure]
        public long ToLong() => rawId;

        // 2MS MG Auf die Gefahr hin dass es jetzt etwas gar philosophisch wird: ILongId stellt sicher dass etwas eine LongId hat. Eine LongId haben ist aus meiner Sicht was
        // ganz anderes als eine LongId sein (dieser struct IST eine LongId). Erinnert mich an den Spruch "Du bist Deutschland!" welcher genau so viel Sinn ergibt.
        // Was war den die Motivation dafür, dass LongId ILongId implementiert?
        long ILongId.Id => rawId;

        long IConvertibleToInt64.ConvertToInt64()
        {
            return rawId;
        }

        private string GetDebuggerDisplayString()
        {
            return "LongId<" + typeof(TILongId).Name + ">(" + rawId + ")";
        }

        public int CompareTo(object other)
        {
            if (!(other is LongId<TILongId>?))
            {
                throw new ArgumentException($"'other' must be of type {nameof(TILongId)}");
            }

            if (other == null)
            {
                return 1;
            }

            long otherrawId = ((LongId<TILongId>)other).rawId;
            if (rawId > otherrawId)
            {
                return 1;
            }

            if (rawId < otherrawId)
            {
                return -1;
            }

            return 0;
        }

        /// <summary>
        /// OBSOLETE: USE ToLong() INSTEAD
        /// </summary>
        public static explicit operator long(LongId<TILongId> id)
        {
            return id.ToLong();
        }
    }
}