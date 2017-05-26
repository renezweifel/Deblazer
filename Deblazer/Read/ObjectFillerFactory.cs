namespace Dg.Deblazer.Read
{
    internal class ObjectFillerFactory : IObjectFillerFactory
    {
        private static ObjectFiller instance;

        public ObjectFiller GetObjectFiller()
        {
            if (instance == null)
            {
                instance = new ObjectFiller();
            }
            return instance;
        }
    }
}
