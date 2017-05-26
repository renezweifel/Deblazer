using System.Collections.Generic;
using Dg.Deblazer.Write;

namespace Dg.Deblazer.AggregateUpdate
{
    public interface IAggregateUpdate<in TDbWrite> : IAggregateUpdate where TDbWrite : IDbWrite
    {
        void SetDb(TDbWrite db);
    }

    public interface IAggregateUpdate
    {
        AggregateUpdateSortingKey UniqueSortingKey { get; }
        int ChunkSize { get; }

        void AddDbEntitiesToUpdate(IEnumerable<DbEntity> entities);

        int GetEntitiesToUpdateCount();

        void UpdateEntities();

        /// <summary>
        /// Do not use commas within the serialized entity since commas are used to separate the serialized entities by the framework
        /// </summary>
        IReadOnlyList<string> SerializeEntities();

        void DeserializeEntities(IEnumerable<string> serializedEntities);
    }
}