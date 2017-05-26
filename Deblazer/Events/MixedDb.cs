using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dg.Deblazer
{
    public delegate void MixedDbEventHandler(object sender, MixedDbEventArgs e);

    public class MixedDbEventArgs : EventArgs
    {
        public readonly DbEntity Value;

        public MixedDbEventArgs(DbEntity value)
        {
            Value = value ?? throw new ArgumentNullException(nameof(value));
        }
    }
}
