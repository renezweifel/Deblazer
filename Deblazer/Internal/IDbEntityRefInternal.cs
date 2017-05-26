using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dg.Deblazer.Internal
{
    internal interface IDbEntityRefInternal : IRaiseDbSubmitEvent
    {
        // - new base interface of IDbEntityRef<TMember>
        // - EntityInternal returns the value of the private field "DbEntityRef.entity"
        // - Implemented explicitly so the methods are not visible in intellisense
        DbEntity EntityInternal { get; }

        bool IsForeignKey { get; }

        void SetDb(BaseDb db);
        void DisableLazyLoadChildren();
        void EnableLoadingChildrenFromCache();
        void MakeReadOnly();
    }
}
