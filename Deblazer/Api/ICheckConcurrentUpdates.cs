using System.Data.Linq;

namespace Dg.Deblazer.Api
{
    public interface ICheckConcurrentUpdates : IId
    {
        Binary RowVersion
        {
            get;
            set;
        }
    }
}