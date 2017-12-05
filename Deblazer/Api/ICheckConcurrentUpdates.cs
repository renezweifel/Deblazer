using System.Data;

namespace Dg.Deblazer.Api
{
    public interface ICheckConcurrentUpdates : IId
    {
        byte[] RowVersion
        {
            get;
            set;
        }
    }
}