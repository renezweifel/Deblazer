namespace Dg.Deblazer
{
    public interface IId : ILongId
    {
        new int Id { get; }
    }
}