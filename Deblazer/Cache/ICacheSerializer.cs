namespace Dg.Deblazer.Cache
{
    public interface ICacheSerializer
    {
        string Serialize(IIsCached dbEntity);
    }    

}
