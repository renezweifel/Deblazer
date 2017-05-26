using Dg.Deblazer.SqlGeneration;

namespace Dg.Deblazer.Extensions
{
    public static class DbExtensions
    {
        public static QueryElOperator<bool> ToDbBool(this bool b)
        {
            return new QueryElBool(b);
        }
    }
}