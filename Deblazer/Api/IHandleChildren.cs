using Dg.Deblazer.Visitors;

namespace Dg.Deblazer.Api
{
    internal interface IHandleChildren
    {
        void HandleChildren(DbEntityVisitorBase visitor);
    }
}
