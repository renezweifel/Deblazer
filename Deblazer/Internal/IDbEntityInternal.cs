using Dg.Deblazer.Visitors;

namespace Dg.Deblazer.Internal
{
    internal interface IDbEntityInternal
    {
        void MarkForDeletion();

        void UnmarkForDeletion();

        void SetDb(IDbInternal db);

        void DisableLazyLoadChildren();

        void SetAllowSettingColumns(bool allowSettingColumns);

        void EnableLoadingChildrenFromCache();

        void MakeReadOnly();

        void ModifyInternalState(UpdateSetVisitor visitor);

        void ModifyInternalState(InsertSetVisitor visitor);

        void ModifyInternalState(FillVisitor visitor);
    }
}
