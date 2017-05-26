using Dg.Deblazer.Visitors;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Dg.Deblazer
{
    public partial class DbEntity
    {
        public static readonly Type DbValueGenericType = typeof(DbValue<>);

        protected struct DbValue<TValue> : IHaveAssignedValue
        {
            // Constructor for serialization
            private TValue entity;
            private bool hasAssignedValue;
            private bool hasLoadedValue;

            public DbValue(TValue entity)
            {
                // Load(entity);
                this.entity = entity;
                hasLoadedValue = true;
                hasAssignedValue = false;
            }

            public TValue Entity
            {
                get
                {
                    if (hasLoadedValue || hasAssignedValue)
                    {
                        return entity;
                    }

                    return default(TValue);
                }

                set
                {
                    if (!EqualityComparer<TValue>.Default.Equals(value, Entity))
                    {
                        hasAssignedValue = true;
                        hasLoadedValue = false;
                        entity = value;
                    }
                }
            }

            public void Load(TValue entity)
            {
                hasLoadedValue = true;
                hasAssignedValue = false;
                this.entity = entity;
            }

            public void Welcome(IUpdateVisitor visitor, string columnName, string columnDbDataTypeName, bool doForce)
            {
                if (visitor.DoReset
                    && (visitor.ColumnsToReset == null || visitor.ColumnsToReset.Contains(columnName)))
                {
                    hasLoadedValue = true;
                    hasAssignedValue = false;
                }
                else if (hasAssignedValue || doForce)
                {
                    visitor.AddUpdatedValue(columnName, columnDbDataTypeName, entity);
                }
            }

            internal void Welcome(InsertSetVisitor visitor)
            {
                if (visitor.DoReset)
                {
                    hasLoadedValue = true;
                    hasAssignedValue = false;
                }
            }

            public override string ToString()
            {
#pragma warning disable S2955 // Generic parameters not constrained to reference types should not be compared to "null"
                if (entity == null)
                {
                    return "NULL";
                }
#pragma warning restore S2955 // Generic parameters not constrained to reference types should not be compared to "null"

                return entity.ToString();
            }

            public bool ValueWasAssigned()
            {
                return hasAssignedValue;
            }
        }
    }
}