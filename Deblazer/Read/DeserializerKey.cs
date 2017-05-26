using System;

namespace Dg.Deblazer.Read
{
    internal struct DeserializerKey : IEquatable<DeserializerKey>
    {
        private readonly int length;
        private readonly string[] columnNames;
        private readonly Type[] columnTypes;
        private readonly int hashCode;

        public DeserializerKey(int hashCode, int length, string[] columnNames, Type[] columnTypes)
        {
            this.hashCode = hashCode;
            this.length = length;
            this.columnNames = columnNames;
            this.columnTypes = columnTypes;
        }

        public override int GetHashCode()
        {
            return hashCode;
        }
        public override string ToString()
        { // only used in the debugger
            if (columnNames != null)
            {
                return string.Join(", ", columnNames);
            }

            return base.ToString();
        }
        public override bool Equals(object obj)
        {
            return obj is DeserializerKey && Equals((DeserializerKey)obj);
        }
        public bool Equals(DeserializerKey other)
        {
            if (this.hashCode != other.hashCode
                || this.length != other.length)
            {
                return false; // clearly different
            }
            for (int i = 0; i < length; i++)
            {
                if ((this.columnNames?[i] != (other.columnNames?[i]))
                    || (this.columnTypes?[i] != (other.columnTypes?[i])))
                {
                    return false; // different column name or type
                }
            }
            return true;
        }
    }
}
