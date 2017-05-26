using System.Collections.Generic;

namespace Dg.Deblazer.Cache
{
    public interface ICachedEntityList<out TEntity> : IReadOnlyList<TEntity> where TEntity : DbEntity
    {
    }
}