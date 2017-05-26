using System;
using System.Collections.Generic;

namespace Dg.Deblazer.Validation
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public sealed class ValidateAttribute : Attribute
    {
        public IEnumerable<string> DbEntityMembers { get; private set; }

        public ValidateAttribute() : this(null)
        {
        }

        public ValidateAttribute(params string[] dbEntityMembers)
        {
            DbEntityMembers = dbEntityMembers;
        }
    }
}