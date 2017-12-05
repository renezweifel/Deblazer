
using System.Data;

namespace Dg.Deblazer.Cache
{
    public interface IIsCached : IId
    {
        byte[] RowVersion
        {
            get;
            set;
        }

        int ContentAuditId { get; }
    }
}