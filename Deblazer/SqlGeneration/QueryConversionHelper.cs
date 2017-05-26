using System;

namespace Dg.Deblazer.SqlGeneration
{
    internal class QueryConversionHelper
    {
        public static QueryEl ConvertParameter<T>(T x)
        {
            if (x == null)
            {
                throw new ArgumentException("A parameter in SQL cannot be null");
            }

            if (x is QueryEl)
            {
                return x as QueryEl;
            }

            return new QueryElParameter<T>(x);
        }

        public static QueryEl ConvertMember(QueryEl x)
        {
            if (x is QueryElMember<bool>)
            {
                return (QueryElMember<bool>)x == true;
            }

            if (x is QueryElMember<bool?>)
            {
                return (QueryElMember<bool?>)x == true;
            }

            return x;
        }
    }
}