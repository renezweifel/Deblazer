using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dg.Deblazer.Internal
{
    internal interface IDbEntitySetInternal : IRaiseDbSubmitEvent
    {
        // - new base interface of IDbEntitySet<TMember>
        // - EntitiesInternal returns the values of the private field "DbEntitySet.values"
        // - Implemented explicitly so the methods are not visible in intellisense
        IReadOnlyList<DbEntity> EntitiesInternal { get; }

        bool IsForeignKey { get; }

        void SetDb(BaseDb db);
        void DisableLazyLoadChildren();
        void EnableLoadingChildrenFromCache();
        void MakeReadOnly();
    }
}
