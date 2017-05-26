namespace Dg.Deblazer.AggregateUpdate
{
    public enum AggregateUpdateProcessingMode
    {
        /// <summary>
        /// IAggregateUpdates are executed directly or async in system task depending on chunk size
        /// </summary>
        InSubmitChangesIfBelowChunkSize = 0,

        /// <summary>
        /// IAggregateUpdates are directly executed during the SubmitChanges
        /// </summary>
        InSubmitChanges,

        /// <summary>
        /// IAggregateUpdates are always executed async in the system task
        /// </summary>
        DeferredInSystemTask
    }
}