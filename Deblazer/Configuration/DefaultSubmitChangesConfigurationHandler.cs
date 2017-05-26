namespace Dg.Deblazer.Configuration
{
    internal class DefaultSubmitChangesConfigurationHandler : ISubmitChangesConfigurationHandler
    {
        public static readonly DefaultSubmitChangesConfigurationHandler Instance = new DefaultSubmitChangesConfigurationHandler();

        private DefaultSubmitChangesConfigurationHandler() { }

        /// <summary>Per default submit changes is allowed on every WriteDb-Instance. If you want some custom behaviour implement your own ISubmitChangesConfigurationHandler.</summary>
        public bool SubmitChangesIsAllowed => true;
    }
}
