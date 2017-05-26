using System.Collections.Generic;

namespace Dg.Deblazer.SqlGeneration
{
    public class ILongIdQuery<TElement> : Query<ILongIdQuery<TElement>, TElement, TElement, ILongIdWrapper<TElement>, ILongIdQuery<TElement>>
        where TElement : DbEntity, ILongId
    {
        public ILongIdQuery(IDb db)
            : base(db)
        {
        }

        protected override ILongIdWrapper<TElement> GetWrapper()
        {
            return ILongIdWrapper<TElement>.Instance;
        }

        protected override IEnumerable<string> GetColumns()
        {
            return QueryHelpers.GetColumnsInSelectStatement<TElement>(joinCount);
        }

        protected override string GetTableName()
        {
            return QueryHelpers.GetFullTableName<TElement>(joinCount);
        }
    }

    public class ILongIdWrapper<TElement> : QueryLongWrapper<TElement> where TElement : DbEntity, ILongId
    {
        public static readonly ILongIdWrapper<TElement> Instance = new ILongIdWrapper<TElement>();

        private ILongIdWrapper()
            : base(QueryHelpers.GetFullTableName<TElement>(), typeof(TElement).Name)
        {
        }
    }
}