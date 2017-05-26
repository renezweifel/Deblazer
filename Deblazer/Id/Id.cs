using JetBrains.Annotations;
using System;
using System.ComponentModel;
using System.Diagnostics;

namespace Dg.Deblazer
{
    public interface IConvertibleToInt32
    {
        int ConvertToInt32();
    }

    [Serializable]
    [DebuggerDisplay("{GetDebuggerDisplayString()}")]
    [TypeConverter(typeof(IdTypeConverter))]
    public struct Id<TIId> : IEquatable<Id<TIId>>, IComparable<Id<TIId>>, IConvertibleToInt32, IComparable, IId
        where TIId : IId
    {
        [CanBeNull]
        // used by IdBuilder (using reflection)
        public static Id<TIId>? Nullable(int? rowId)
        {
            if (rowId == null)
            {
                return null;
            }

            return new Id<TIId>(rowId.Value);
        }

        public static bool TryParse(string s, out Id<TIId> result)
        {
            int i;
            if (int.TryParse(s, out i))
            {
                result = new Id<TIId>(i);
                return true;
            }

            result = new Id<TIId>(0);
            return false;
        }

        private readonly int rowId;

        // used by IdBuilder (using reflection)
        public Id(int id)
        {
            rowId = id;
        }

        [Obsolete("Use entity.GetTypedId() instead")]
        public Id(IId entity)
        {
            rowId = entity.Id;
        }

        /// <summary>
        /// OBSOLETE: Because Typed ids currently implement IId (which is ugly) it is possible to create a typed id based on a typed id (which is very
        /// silly and does not really have a use case). This constructor can be removed as soon as the 
        /// </summary>
        // Will this sentence ever be finished? Will the constructor ever be removed? Will the Id ever find salvation? And who is the silly one? Find out in the next episode of "An Id to remember".
        [Obsolete("So you have a Id<T> and want to create a Id<T> out of it? Don't be silly...")]
        public Id(Id<TIId> youAlreadyHaveWhatYouWantDontBeSilly)
        {
            rowId = youAlreadyHaveWhatYouWantDontBeSilly.rowId;
        }

        public override int GetHashCode()
        {
            return rowId.GetHashCode();
        }

        public bool Equals(Id<TIId> other)
        {
            return other.rowId == rowId;
        }

        public override bool Equals(object obj)
        {
            return obj is Id<TIId> && Equals((Id<TIId>)obj);
        }

        public static bool operator ==(Id<TIId> x, Id<TIId> y)
        {
            return x.Equals(y);
        }

        public static bool operator !=(Id<TIId> x, Id<TIId> y)
        {
            return !x.Equals(y);
        }

        public int CompareTo(Id<TIId> other)
        {
            return rowId.CompareTo(other.rowId);
        }

        public static bool operator >(Id<TIId> x, Id<TIId> y)
        {
            return x.rowId > y.rowId;
        }

        public static bool operator <(Id<TIId> x, Id<TIId> y)
        {
            return x.rowId < y.rowId;
        }

        public static bool operator >=(Id<TIId> x, Id<TIId> y)
        {
            return x.rowId >= y.rowId;
        }

        public static bool operator <=(Id<TIId> x, Id<TIId> y)
        {
            return x.rowId <= y.rowId;
        }

        public override string ToString()
        {
            // Only show id, so DbLayer-queries get cast correctly. Type information is shown in the DebuggerDisplay.
            return rowId.ToString();
        }

        /// <summary>
        /// Gets the Id including table name (e.g. "Product/15")
        /// </summary>
        public string FullId => typeof(TIId).Name + "/" + rowId;

        [System.Diagnostics.Contracts.Pure]
        public int ToInt() => rowId;

        // 2MS MG Auf die Gefahr hin dass es jetzt etwas gar philosophisch wird: IId stellt sicher dass etwas eine Id hat. Eine Id haben ist aus meiner Sicht was
        // ganz anderes als eine Id sein (dieser struct IST eine Id). Erinnert mich an den Spruch "Du bist Deutschland!" welcher genau so viel Sinn ergibt.
        // Was war den die Motivation dafür, dass Id IId implementiert?
        int IId.Id => ToInt();
        long ILongId.Id => ((IId)this).Id;

        int IConvertibleToInt32.ConvertToInt32()
        {
            return ToInt();
        }

        /// <summary>
        /// OBSOLETE: USE ToInt() INSTEAD
        /// </summary>
        public static explicit operator int(Id<TIId> id)
        {
            return id.ToInt();
        }

        /// <summary>
        /// Implicit operator from int for backward compatibility of existing code
        /// </summary>
        public static implicit operator Id<TIId>(int id)
        {
            return new Id<TIId>(id);
        }

        private string GetDebuggerDisplayString()
        {
            return "Id<" + typeof(TIId).Name + ">(" + rowId + ")";
        }

        public int CompareTo(object other)
        {
            if (!(other is Id<TIId>?))
            {
                throw new ArgumentException($"'other' must be of type {nameof(TIId)}");
            }

            if (other == null)
            {
                return 1;
            }

            var otherRowId = ((Id<TIId>)other).rowId;
            if (rowId > otherRowId)
            {
                return 1;
            }

            if (rowId < otherRowId)
            {
                return -1;
            }

            return 0;
        }
    }
}