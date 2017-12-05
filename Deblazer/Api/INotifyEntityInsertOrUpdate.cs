using System.Data;

namespace Dg.Deblazer.Api
{
    public interface INotifyEntityInsertOrUpdate
    {
        byte[] RowVersion { get; }
    }
}
