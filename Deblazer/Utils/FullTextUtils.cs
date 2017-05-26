using System.Collections.Generic;
using System.Linq;
using Microsoft.SqlServer.Server;

namespace Dg.Deblazer.Utils
{
    public class FullTextUtils
    {
        private const int MAX_TERM_LEN = 128;
        private const bool SKIP_INVALID = true;

        public static string GetFullTextStartsWithQuery([SqlFacet(MaxSize = -1)] string query)
        {
            return string.Join(" AND ", Extract(query).Select(f => $"\"{f}*\""));
        }

        public static string GetFullTextContainsExactWordWithQuery([SqlFacet(MaxSize = -1)] string query)
        {
            return string.Join(" AND ", Extract(query).Select(e => $"'{e}'"));
        }

        private static IEnumerable<string> Extract(string phrase)
        {
            if (string.IsNullOrEmpty(phrase))
            {
                // http://stackoverflow.com/questions/189765/7645-null-or-empty-full-text-predicate
                yield return "\"\"";
                yield break;
            }

            char[] chars = phrase.ToCharArray();

            int count = chars.Length;

            char[] buffer = new char[count];
            int buffersize = 0;

            for (int i = 0; i < count; i++)
            {
                var cchar = chars[i];

                switch (cchar)
                {
                    // ignoring symbols
                    // case '.': // ricardo.ch should find the Company ricardo.ch
                    case '\'':
                    case '"':
                        continue;

                    // breaking symbols
                    case ' ':
                    case ',':
                    case '/':
                    case '[':
                    case ']':
                    case '{':
                    case '}':
                    case '(':
                    case ')':
                        // NOTE: Removed because for example Stand-up Paddel should be treated as two words (Stand-up + Paddel)
                        //case '-':

                        if (!SKIP_INVALID || buffersize <= MAX_TERM_LEN)
                        {
                            var value = new string(buffer, 0, buffersize).Trim();
                            if (!string.IsNullOrEmpty(value))
                            {
                                yield return value;
                            }
                        }

                        buffersize = 0;
                        continue;

                    // continue iteration
                    default:
                        buffer[buffersize++] = cchar;
                        break;
                }
            }

            // clear buffer
            if (buffersize > 0 && (!SKIP_INVALID || buffersize <= MAX_TERM_LEN))
            {
                var value = new string(buffer, 0, buffersize).Trim();
                if (!string.IsNullOrEmpty(value))
                {
                    yield return new string(buffer, 0, buffersize);
                }
            }
        }
    }
}