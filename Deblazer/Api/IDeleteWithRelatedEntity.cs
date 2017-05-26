using System.Collections.Immutable;

namespace Dg.Deblazer.Api
{
    public interface IDeleteWithRelatedEntity
    {
        IImmutableSet<DbEntity> GetEntitiesToDeleteTogether(DbEntity entity);
    }
}