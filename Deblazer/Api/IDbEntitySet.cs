using System.Collections.Generic;

namespace Dg.Deblazer.Api
{
    public interface IDbEntitySet<TMember> : IDbEntitySet, IList<TMember>, IReadOnlyList<TMember> where TMember : DbEntity
    {
        void AddRange(IEnumerable<TMember> entities);

        void Attach(IEnumerable<TMember> entities);

        IEnumerable<TMember> ValuesNoLoad { get; }

        long? ForeignKey { get; }

        new int Count { get; }

        new TMember this[int index] { get; set; }
    }

    public interface IDbEntitySet
    {
    }
}