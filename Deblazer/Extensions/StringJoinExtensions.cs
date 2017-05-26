using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dg.Deblazer.Extensions
{
    public static class StringJoinExtensions
    {
        public static string JoinString<TSource>(this IEnumerable<TSource> source, string separator, bool onlyNonEmptyValues)
        {
            if (!onlyNonEmptyValues)
            {
                return string.Join(separator, source);
            }

            var strings = source as IEnumerable<string>;
            // Casting the whole collection to Ienumerable<string> is much faster than casting every element...
            if (strings == null)
            {
                strings = source.Select(s => s != null ? s.ToString() : null);
            }

            strings = strings.Where(s => !string.IsNullOrEmpty(s));

            return string.Join(separator, strings);
        }
    }
}
