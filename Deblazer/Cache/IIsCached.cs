using System.Data.Linq;

namespace Dg.Deblazer.Cache
{
    public interface IIsCached : IId
    {
        Binary RowVersion
        {
            get;
            set;
        }

        int ContentAuditId { get; }
    }
}