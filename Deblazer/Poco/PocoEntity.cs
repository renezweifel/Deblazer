using System;
using Dg.Deblazer.Utils;
using Dg.Deblazer.Extensions;

namespace Dg.Deblazer.Poco
{
    public abstract class PocoEntity : IComparable
    {
        protected abstract long InternalId { get; }

        public override int GetHashCode()
        {
            return InternalId.GetHashCode();
        }

        public override string ToString()
        {
            return "[" + GetType().Name + "] " + ObjectUtils.PropertiesToString(this);
        }

        public override bool Equals(object other)
        {
            if (other != null && other.GetType() == GetType())
            {
                if (InternalId == ((PocoEntity)other).InternalId)
                {
                    if (InternalId == 0)
                    {
                        // If item does not come from the database, we compare the reference
                        return ReferenceEquals(this, other);
                    }

                    return true;
                }
            }

            return false;
        }

        int IComparable.CompareTo(object obj)
        {
            if (obj == null)
                return -1;
            if (obj.GetType() != GetType())
            {
                throw new ArgumentException($"object is not a {GetType().Name}");
            }

            return InternalId.CompareTo(((PocoEntity)obj).InternalId);
        }

        // Use this to save a database request when updating the DbEntity from a poco (see ErpRepository.UpdateOnSubmit())
        private DbEntity originalDbEntity;

        public DbEntity GetOriginalDbEntity()
        {
            return originalDbEntity;
        }

        public void SetOriginalDbEntity(DbEntity originalDbEntity)
        {
            this.originalDbEntity = originalDbEntity;
        }
    }
}