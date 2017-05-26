using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dg.Deblazer.Settings;
using Dg.Deblazer.Write;

namespace Dg.Deblazer.Test
{
    class TestDb : WriteDb
    {
        public TestDb(WriteDbSettings settings) : base(settings)
        {
        }
    }
}
