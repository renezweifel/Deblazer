namespace Dg.Deblazer.AggregateUpdate
{
    /// <summary>
    /// This interface can be implemented optionaly in addition to IAggregateUpdate. If it is implemented, it takes precedence over the AggregateUpdateProcessingMode
    /// specified for a SubmitChanges(). See https://conf.devinite.com/display/ING/2016/10/18/IAggregateUpdateWithEnforcedMode for Details.
    /// </summary>
    public interface IAggregateUpdateWithEnforcedMode
    {
        AggregateUpdateProcessingMode EnforcedMode { get; }
    }
}
