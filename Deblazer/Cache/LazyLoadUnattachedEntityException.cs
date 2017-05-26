using System;

namespace Dg.Deblazer.Cache
{
    internal class LazyLoadUnattachedEntityException : Exception
    {
        public LazyLoadUnattachedEntityException(string message)
            : base(message)
        {

        }
    }
}
