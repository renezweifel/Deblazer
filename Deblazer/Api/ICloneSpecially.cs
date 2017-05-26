using System;
using System.Collections.Immutable;

namespace Dg.Deblazer.Api
{
    public interface ICloneSpecially
    {
        object GetClone(bool cloneIdField);
        IImmutableSet<Type> GetExcludedTypes();
    }
}