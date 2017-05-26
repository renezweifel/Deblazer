namespace Dg.Deblazer.SqlGeneration
{
    /// <summary>
    /// Documentation: https://erp.galaxus.ch/de/Wiki/4833
    /// </summary>
    public abstract class QueryWrapper
    {
        public const string IdColumnName = "Id";

        protected readonly string tableNameAndAlias;
        protected string alias;

        protected QueryWrapper(string tableName, string alias)
        {
            this.alias = alias;
            tableNameAndAlias = string.Format("{0} AS [{1}]", tableName, alias);
        }

        public override string ToString()
        {
            return tableNameAndAlias;
        }
    }
    public abstract class QueryWrapper<TEntity> : QueryWrapper where TEntity : IId
    {
        protected QueryWrapper(string tableName, string alias, string id = IdColumnName) : base(tableName, alias)
        {
            Id = new QueryElMemberId<TEntity>(id);
        }

        public readonly QueryElMemberId<TEntity> Id;
    }

    public abstract class QueryLongWrapper<TEntity> : QueryWrapper where TEntity : ILongId
    {
        protected QueryLongWrapper(string tableName, string alias, string id = IdColumnName) : base(tableName, alias)
        {
            Id = new QueryElMemberLongId<TEntity>(id);
        }

        public readonly QueryElMemberLongId<TEntity> Id;
    }

    public static class QueryWrapperExtensions
    {
        public static QueryEl[] Choose<TWrapper>(this TWrapper wrapper, params QueryEl[] func) where TWrapper : QueryWrapper
        {
            return func;
        }
    }
}