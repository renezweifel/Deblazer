using System.Collections.Generic;

namespace Dg.Deblazer.SqlGeneration
{
    public class IIdQuery<TElement> : Query<IIdQuery<TElement>, TElement, TElement, IIdWrapper<TElement>, IIdQuery<TElement>>
        where TElement : DbEntity, IId
    {
        public IIdQuery(IDb db)
            : base(db)
        {
        }

        protected override IIdWrapper<TElement> GetWrapper()
        {
            return IIdWrapper<TElement>.Instance;
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

    public class IIdWrapper<TElement> : QueryWrapper<TElement> where TElement : DbEntity, IId
    {
        public static readonly IIdWrapper<TElement> Instance = new IIdWrapper<TElement>();

        private IIdWrapper()
            : base(QueryHelpers.GetFullTableName<TElement>(), typeof(TElement).Name)
        {
        }
    }
}