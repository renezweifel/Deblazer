using System.Data.Linq;

namespace Dg.Deblazer.Api
{
    public interface INotifyEntityInsertOrUpdate
    {
        Binary RowVersion { get; }
    }
}
