using System;

namespace Dg.Deblazer.Api
{
    public interface IDbEntityRef<TMember> : IDbEntityRef where TMember : DbEntity
    {
        bool HasLoadedValue { get; }

        bool HasAssignedValue { get; }

        void Load(TMember newEntity);

        void ResetValue();

        TMember GetEntity(Action<TMember> beforeRightsCheckAction);

        void SetEntity(TMember value);
    }

    public interface IDbEntityRef
    {
    }
}