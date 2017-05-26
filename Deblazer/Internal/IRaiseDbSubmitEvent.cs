using System.Collections.Immutable;

namespace Dg.Deblazer.Internal
{
    internal interface IRaiseDbSubmitEvent
    {
        bool RaiseBeforeInsertEvent();

        IImmutableSet<DbEntity> RaiseAfterInsertEvent();

        bool RaiseBeforeDeleteEvent();

        bool RaiseBeforeUpdateEvent();

        bool RaiseOnSubmitTransactionAbortedEvent();

        bool RaiseAfterDeleteEvent();
    }
}
