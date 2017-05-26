using Dg.Deblazer.Internal;
using Dg.Deblazer.SqlUtils;
using Dg.Deblazer.Visitors;
using JetBrains.Annotations;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace Dg.Deblazer.SqlGeneration
{
    public delegate T FillDelegate<T>(T entity, FillVisitor fillVisitor, Func<object, object> preProcessEntity);

    public delegate void InadequateTakeCountHandler(int takeCount);

    public abstract class QueryBase
    {
        protected readonly IDb Db;
        protected Func<object, object> GetMember;
        protected QueryEl avgQueryEl = null;
        protected string avgQueryElString = null;

        protected string callerFilePath = null;
        protected int callerLineNumber = 0;
        protected Type dbEntityTypeToLoad;
        private List<(QueryBase, CombinationType)> combinedQueries;
        protected bool concatWithAnd = true;
        protected bool distinct = false;
        protected FillDelegate<object> fillMember;

        protected int globalJoinCount = 0;
        protected int groupingJoinCount = 0;
        protected int joinCount = 0;
        protected string joinString;

        protected object joinValue;
        protected List<QueryBase> joinedQueries;

        protected QueryEl queryEl;
        internal List<string> selectColumns;

        protected internal QueryBase superQuery = null;

        protected string options = null;

        protected GlobalQueryData globalQueryData = null;
        protected internal List<QueryToAttach> queriesToAttach = null;
        private QueryBase clonedQueryForAttach;

        protected QueryBase(IDb Db)
        {
            this.Db = Db;
        }

        internal IDbInternal DbInternal => (IDbInternal)Db;

        protected List<(QueryBase, CombinationType)> CombinedQueries
        {
            get
            {
                if (combinedQueries == null)
                {
                    combinedQueries = new List<(QueryBase, CombinationType)>();
                }

                return combinedQueries;
            }

            set { combinedQueries = value; }
        }

        public QueryBase queryToAttachJoinedQueriesTo { get; private set; }

        protected void AddOrderByToTopMostSuperQuery(QueryEl queryEl, Tuple<OrderByType, OrderByAggregation> tuple)
        {
            var orderByData = new OrderByData(queryEl, tuple.Item1, tuple.Item2, joinCount);
            GetGlobalQueryData().OrderByQueryEls.Add(orderByData);
        }

        protected bool HasOrderByClause()
        {
            var globalQueryData = GetGlobalQueryData();
            if (globalQueryData.OrderByQueryEls != null && globalQueryData.OrderByQueryEls.Count > 0)
            {
                return true;
            }

            if (combinedQueries != null)
            {
                foreach (var combinedQuery in combinedQueries)
                {
                    if (combinedQuery.Item1.HasOrderByClause())
                    {
                        return true;
                    }
                }
            }

            if (joinedQueries != null)
            {
                foreach (var joinedQuery in joinedQueries)
                {
                    if (joinedQuery.HasOrderByClause())
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        protected GlobalQueryData GetGlobalQueryData()
        {
            if (superQuery == null)
            {
                if (globalQueryData == null)
                {
                    globalQueryData = new GlobalQueryData();
                }

                return globalQueryData;
            }
            else
            {
                return superQuery.GetGlobalQueryData();
            }
        }

        internal abstract QueryBase Clone(QueryBase clonedSuperQuery);

        internal abstract void ResetClone();

        protected void EmitValueLoaded(DbEntity entity)
        {
            if (superQuery != null)
            {
                superQuery.EmitValueLoaded(entity);
            }

            DbInternal.TriggerEntitiesLoaded(new[] { entity });
        }

        protected object PreProcessEntity(object subEntity)
        {
            if (subEntity is IId)
            {
                return DbInternal.LoadedEntityCache.GetOrAdd(subEntity.GetType(), "Id", ((IId)subEntity).Id, subEntity);
            }

            return subEntity;
        }

        protected void SetSelectColumnsInCombinedQueries<TTuple>(Type currentTableType, HashSet<QueryBase> processedQueries, params QueryEl[] queryEls)
        {
            if (!processedQueries.Contains(this))
            {
                processedQueries.Add(this);

                if (combinedQueries != null)
                {
                    foreach (var combinedQuery in combinedQueries)
                    {
                        combinedQuery.Item1.SetSelectColumns(currentTableType, queryEls);
                    }
                }

                if (joinedQueries != null)
                {
                    joinedQueries.ForEach(q => q.SetSelectColumnsInCombinedQueries<TTuple>(currentTableType, processedQueries, queryEls));
                }

                if (superQuery != null)
                {
                    superQuery.SetSelectColumnsInCombinedQueries<TTuple>(currentTableType, processedQueries, queryEls);
                }
            }
        }

        protected internal void SetDistinctFalseInAllConnectedQueries()
        {
            // when we take one element anyway we do not need to make a distinct. Distinct is an overhead for the Sql server in this case.
            VisitAllConnectedQueries(q => q.distinct = false);
        }


        protected void VisitAllConnectedQueries(Action<QueryBase> action)
        {
            HashSet<QueryBase> processedQueries = new HashSet<QueryBase>();
            VisitAllConnectedQueries(this, processedQueries, action);
        }

        // NL TC Bitte an den Test CodeTest_CheckForSimpleRecursiveCalls_NoRecursiveCalls bei Rekursionen denken!
        // 2TC NL Hatte sogar kurz daran gedacht, aber nachher wieder vergessen ihn anzupassen. Sorry. Danke fürs Anpassen!
        private static void VisitAllConnectedQueries(QueryBase currentQuery, HashSet<QueryBase> processedQueries, Action<QueryBase> action)
        {
            if (!processedQueries.Contains(currentQuery))
            {
                processedQueries.Add(currentQuery);
                action(currentQuery);

                if (currentQuery.combinedQueries != null)
                {
                    foreach (var combinedQuery in currentQuery.combinedQueries)
                    {
                        VisitAllConnectedQueries(combinedQuery.Item1, processedQueries, action);
                    }
                }

                if (currentQuery.joinedQueries != null)
                {
                    foreach (var joinedQuery in currentQuery.joinedQueries)
                    {
                        VisitAllConnectedQueries(joinedQuery, processedQueries, action);
                    }
                }

                if (currentQuery.superQuery != null)
                {
                    VisitAllConnectedQueries(currentQuery.superQuery, processedQueries, action);
                }
            }
        }

        protected void SetSelectColumns(Type currentTableType, QueryEl[] queryEls)
        {
            string tableAlias = QueryHelpers.GetTableAlias(currentTableType, joinCount);
            selectColumns = new List<string>(queryEls.Length);
            selectColumns.AddRange(queryEls.Select(e => string.Format(e.GetSql(parameters: null, joinCount: joinCount), tableAlias)));
            // If there are order by clauses, we need to add them too. Otherwise we get corrupt SQL code
        }

        protected U JoinSet<U, TTableQuery>(
            Func<TTableQuery> getQueryToPrefetch,
            U joinedQuery,
            string _joinString,
            Func<QueryWrapper, IReadOnlyList<long>, QueryEl> queryFilter,
            Action<DbEntity, IReadOnlyList<DbEntity>> attachEntitiesAction,
            Func<DbEntity, long> discriminator,
            bool attachJoinedObject)
            where U : QueryBase
            where TTableQuery : QueryBase
        {
            if (attachJoinedObject)
            {
                var q = clonedQueryForAttach ?? this;
                if (q.queriesToAttach == null)
                {
                    q.queriesToAttach = new List<QueryToAttach>();
                }
                var queryToPrefetch = getQueryToPrefetch();

                q.queriesToAttach.Add(
                    new QueryToAttach
                    {
                        QueryBase = queryToPrefetch,
                        QueryFilter = queryFilter,
                        AttachEntitiesAction = attachEntitiesAction,
                        GroupingKey = discriminator
                    });

                joinedQuery.queryToAttachJoinedQueriesTo = queryToPrefetch;
            }

            // Assume we never want to duplicate the root entities when joining a 1:N relation
            // and use this join only to filter the result set or to attach the entities
            // we automatically add a distinct here
            distinct = true;

            return JoinStatic(
                this,
                joinedQuery,
                _joinString,
                getMember: null,
                _fillMember: null,
                typeToGetSelectColumnsFrom: null,
                loadJoinedObject: false);
        }

        protected U Join<U>(
            U joinedQuery,
            string _joinString,
            Func<object, object> getMember,
            FillDelegate<object> _fillMember,
            Type typeToGetSelectColumnsFrom,
            bool loadJoinedObject) where U : QueryBase
        {
            return JoinStatic(
                this,
                joinedQuery,
                _joinString,
                getMember: getMember,
                _fillMember: _fillMember,
                typeToGetSelectColumnsFrom: typeToGetSelectColumnsFrom,
                loadJoinedObject: loadJoinedObject);
        }

        protected static U JoinStatic<U>(
            QueryBase currentQuery,
            U joinedQuery,
            string _joinString,
            Func<object, object> getMember,
            FillDelegate<object> _fillMember,
            Type typeToGetSelectColumnsFrom,
            bool loadJoinedObject) where U : QueryBase
        {
            joinedQuery.superQuery = currentQuery;
            if (loadJoinedObject)
            {
                joinedQuery.dbEntityTypeToLoad = typeToGetSelectColumnsFrom;
            }

            joinedQuery.joinString = _joinString;

            if (getMember != null && loadJoinedObject)
            {
                if (currentQuery.GetMember == null)
                {
                    joinedQuery.GetMember = getMember;
                }
                else
                {
                    joinedQuery.GetMember = t => getMember(currentQuery.GetMember(t));
                }

                joinedQuery.fillMember = (entity, fillVisitor, ppe) =>
                {
                    var member = currentQuery.GetMember == null ? entity : currentQuery.GetMember(entity);
                    var dbMember = member as DbEntity;
                    if (dbMember != null)
                    {
                        ((IDbEntityInternal)dbMember).SetAllowSettingColumns(false);
                    }

                    return _fillMember(member, fillVisitor, ppe);
                };
            }

            if (currentQuery.joinedQueries == null)
            {
                currentQuery.joinedQueries = new List<QueryBase>();
            }

            currentQuery.joinedQueries.Add(joinedQuery);

            currentQuery.UpdateGlobalJoinCountInJoinedQueries(currentQuery.globalJoinCount + 1);

            joinedQuery.joinCount = currentQuery.globalJoinCount;

            if (loadJoinedObject && currentQuery.queryToAttachJoinedQueriesTo != null)
            {
                var clonedQueryForAttach = joinedQuery.Clone(clonedSuperQuery: null);

                JoinStatic(
                    currentQuery.queryToAttachJoinedQueriesTo,
                    clonedQueryForAttach,
                    _joinString,
                    getMember: null,
                    _fillMember: null,
                    typeToGetSelectColumnsFrom: null,
                    loadJoinedObject: false);

                clonedQueryForAttach.queryToAttachJoinedQueriesTo = currentQuery.queryToAttachJoinedQueriesTo;

                joinedQuery.clonedQueryForAttach = clonedQueryForAttach;
            }

            return joinedQuery;
        }

        private void UpdateGlobalJoinCountInJoinedQueries(int newGlobalJoinCount)
        {
            if (globalJoinCount != newGlobalJoinCount)
            {
                globalJoinCount = newGlobalJoinCount;
                if (joinedQueries != null)
                {
                    joinedQueries.ForEach(q => q.UpdateGlobalJoinCountInJoinedQueries(newGlobalJoinCount));
                }

                if (superQuery != null)
                {
                    superQuery.UpdateGlobalJoinCountInJoinedQueries(newGlobalJoinCount);
                }
            }
        }

        protected void AddColumnsFillMethods(
            List<string> addColumns,
            List<JoinedQueryDataSet> fillDelegates,
            List<string> joinStrings,
            List<string> whereStrings,
            SqlParameterCollection sqlParameters,
            List<Tuple<string, OrderByAggregation, OrderByType>> orderByColumns,
            ref bool doDistinctQuery,
            HashSet<string> joinedTableNames,
            bool doAddColumns)
        {
            doAddColumns = doAddColumns && (fillMember != null || superQuery == null);
            if (doAddColumns)
            {
                if (dbEntityTypeToLoad != null)
                {
                    addColumns.AddRange(QueryHelpers.GetColumnsInSelectStatement(dbEntityTypeToLoad, joinCount: joinCount));
                }

                if (fillMember != null)
                {
                    fillDelegates.Add(new JoinedQueryDataSet(fillMember, queriesToAttach));
                }
            }

            if (joinString != null && superQuery != null)
            {
                var thisTableAlias = string.Concat("[t", superQuery.joinCount, "]");
                var otherTableAlias = string.Concat("[t", joinCount, "]");

                if (joinValue != null)
                {
                    string paramName = $"@{sqlParameters.Count}";
                    string.Format(joinString, Db.Settings.WithNoLock ? "WITH (NOLOCK) " : "", otherTableAlias, thisTableAlias, paramName);
                    joinStrings.Add(string.Format(joinString, Db.Settings.WithNoLock ? "WITH (NOLOCK) " : "", otherTableAlias, thisTableAlias, paramName));
                    QueryHelpers.AddSqlParameter(sqlParameters, paramName, joinValue);
                }
                else
                {
                    joinStrings.Add(string.Format(joinString, Db.Settings.WithNoLock ? "WITH (NOLOCK) " : "", otherTableAlias, thisTableAlias));
                }
            }

            if (!IsNull(queryEl))
            {
                string whereString = "";
                if (whereStrings.Count > 0)
                {
                    whereString = concatWithAnd ? " AND " : " OR ";
                }

                whereStrings.Add(whereString + QueryConversionHelper.ConvertMember(queryEl).GetSql(sqlParameters, joinCount));
            }

            if (distinct)
            {
                doDistinctQuery = true;
            }

            if (joinedQueries != null)
            {
                foreach (QueryBase joinedQuery in joinedQueries)
                {
                    if (joinedQuery.options != null)
                    {
                        options = joinedQuery.options;
                    }

                    joinedQuery.AddColumnsFillMethods(
                        addColumns,
                        fillDelegates,
                        joinStrings,
                        whereStrings,
                        sqlParameters,
                        orderByColumns,
                        ref doDistinctQuery,
                        joinedTableNames,
                        doAddColumns);

                    CombinedQueries.AddRange(joinedQuery.CombinedQueries);
                }
            }
        }

        protected void CheckCombinedQueries(QueryBase otherQuery)
        {
            var otherQueryOrParent = otherQuery;
            do
            {
                if (otherQueryOrParent == this)
                {
                    throw new InvalidOperationException("You are combining a query with itself. This is not supported and leads currently to StackOverflow exceptions. Use .Clone() on the query before you pass it to UnionDb, ExceptDb etc.");
                }
                otherQueryOrParent = otherQueryOrParent.superQuery;
            } while (otherQueryOrParent != null);

            CombinedQueries.ForEach(cq => cq.Item1.CheckCombinedQueries(otherQuery));

            if (superQuery != null)
            {
                superQuery.CheckCombinedQueries(otherQuery);
            }
        }

        private string OrderByAggregationToString(string queryElString, OrderByAggregation orderByAggregation)
        {
            // Kandidat für ENG-279 inkl. Parameter
            switch (orderByAggregation)
            {
                case OrderByAggregation.Avg:
                    return "AVG(" + queryElString + ")";

                case OrderByAggregation.Max:
                    return "MAX(" + queryElString + ")";

                case OrderByAggregation.Min:
                    return "MIN(" + queryElString + ")";

                case OrderByAggregation.Sum:
                    return "SUM(" + queryElString + ")";

                case OrderByAggregation.Count:
                    return "COUNT(" + queryElString + ")";

                default:
                    return queryElString;
            }
        }

        [CanBeNull]

        // Kandidat für ENG-279
        private string OrderByTypeToString(OrderByType orderByType)
        {
            switch (orderByType)
            {
                case OrderByType.Ascending:
                    return "ASC";

                case OrderByType.Descending:
                    return "DESC";

                default:
                    return null;
            }
        }

        protected abstract IEnumerable<string> GetColumns();

        protected abstract string GetTableName();

        protected DbSqlCommand GetSqlCommand(DbSqlConnection dbSqlConnection)
        {
            List<JoinedQueryDataSet> fillDelegates;
            return GetSqlCommand(dbSqlConnection, out fillDelegates);
        }

        protected DbSqlCommand GetSqlCommand(DbSqlConnection dbSqlConnection, out List<JoinedQueryDataSet> fillDelegates)
        {
            var sqlCommand = new DbSqlCommand();
            if (dbSqlConnection != null)
            {
                sqlCommand.Connection = dbSqlConnection.SqlConnection;
            }

            fillDelegates = new List<JoinedQueryDataSet>();

            sqlCommand.CommandTimeout = (int)Db.Settings.CommandTimeout.TotalSeconds;
            var orderByColumns = new List<Tuple<string, OrderByAggregation, OrderByType>>();
            var groupByStrings = new List<string>();

            StringBuilder sql = new StringBuilder();
            if (callerFilePath != null && callerLineNumber > 0)
            {
                sql.AppendLine("-- Generated in " + callerFilePath + " at line " + callerLineNumber);
            }

            var globalQueryData = GetGlobalQueryData();
            if (globalQueryData.OrderByQueryEls.Count > 0)
            {
                // Achtung in Item.ToString werden, je nach type des QueryEl, die im OrderBy benötigten Parameter abgefüllt.
                orderByColumns.AddRange(globalQueryData.OrderByQueryEls
                    .Select(p => Tuple.Create(p.QueryEl.GetSql(sqlCommand.Parameters, p.JoinCount), p.Aggregation, p.OrderByType)));
            }

            var superQuery = AddToCommand(fillDelegates, sqlCommand, sql, orderByColumns, groupByStrings);

            if (orderByColumns.Count > 0
                && superQuery.globalQueryData.SkipCount <= 0
                && !superQuery.globalQueryData.DoCount
                && !superQuery.globalQueryData.DoCaseWhenExists
                && !(superQuery.globalQueryData.TopCount.HasValue
                    && superQuery.globalQueryData.TopCount.Value > 0
                    && superQuery.CombinedQueries.Count > 0))
            {
                // Distinct() is needed when you do a UnionDb and the sub-queries have the same OrderByDb clause
                // ORDER BY outside of AddToCommand because when using UnionDb, it would add ORDER BY multiple times to each sub query. But we want one global ORDER BY.
                sql.Append(" ORDER BY ");
                sql.Append(
                    string.Join(", ", orderByColumns.Select(
                        p => string.Concat(
                            OrderByAggregationToString(p.Item1, p.Item2),
                            " ",
                            OrderByTypeToString(p.Item3))).Distinct()));
            }

            // Only one OPTION clause can be specified with the statement.
            if (options != null)
            {
                sql.Append(" OPTION(" + options + ") ");
            }

            sqlCommand.CommandText = sql.ToString();
            return sqlCommand;
        }

        protected QueryBase AddToCommand(
            List<JoinedQueryDataSet> fillDelegates,
            DbSqlCommand sqlCommand,
            StringBuilder sql,
            List<Tuple<string, OrderByAggregation, OrderByType>> orderByColumns,
            List<string> groupByStrings,
            bool addOrderByColumns = false,
            bool needsGlobalCount = false)
        {
            var globalQueryData = GetGlobalQueryData();

            if (superQuery != null)
            {
                if (globalQueryData.DoGroup || globalQueryData.GroupingType != null)
                {
                    superQuery.groupingJoinCount = groupingJoinCount == 0 ? joinCount : groupingJoinCount;
                }

                if (globalQueryData.DoCount)
                {
                    if (globalQueryData.DoGroup) // DistinctDb().CountDb() should result in COUNT(DISTINCT *)
                    {
                        globalQueryData.DistinctCount = true;
                    }
                }

                if (!IsNull(globalQueryData.MaxQueryEl))
                {
                    superQuery.globalQueryData.MaxQueryElString = globalQueryData.MaxQueryEl.GetSql(null, joinCount);
                }

                if (!IsNull(globalQueryData.MinQueryEl))
                {
                    superQuery.globalQueryData.MinQueryElString = globalQueryData.MinQueryEl.GetSql(null, joinCount);
                }

                if (!IsNull(globalQueryData.SumQueryEl))
                {
                    superQuery.globalQueryData.SumQueryElString = globalQueryData.SumQueryEl.GetSql(null, joinCount);
                }

                if (!IsNull(avgQueryEl))
                {
                    superQuery.avgQueryElString = avgQueryEl.GetSql(null, joinCount);
                }

                if (superQuery.selectColumns == null)
                {
                    superQuery.selectColumns = selectColumns;
                }

                return superQuery.AddToCommand(fillDelegates, sqlCommand, sql, orderByColumns, groupByStrings, addOrderByColumns: addOrderByColumns, needsGlobalCount: needsGlobalCount);
            }
            else
            {
                var queryColumns = new List<string>();
                var joinStrings = new List<string>();
                var whereStrings = new List<string>();
                var joinedTableNames = new HashSet<string>();
                bool doDistinctQuery = false;

                queryColumns.AddRange(GetColumns());

                AddColumnsFillMethods(
                    queryColumns,
                    fillDelegates,
                    joinStrings,
                    whereStrings,
                    sqlCommand.Parameters,
                    orderByColumns,
                    ref doDistinctQuery,
                    joinedTableNames,
                    true);
                if (globalQueryData.DoGroup && !globalQueryData.DistinctCount) // DistinctDb().CountDb() should result in COUNT(DISTINCT *)
                {
                    groupByStrings.AddRange(queryColumns);
                }

                if (globalQueryData.GroupingType != null)
                {
                    groupByStrings.AddRange(QueryHelpers.GetColumnsInSelectStatement(globalQueryData.GroupingType, groupingJoinCount));
                }

                if (!IsNull(globalQueryData.MaxQueryEl))
                {
                    globalQueryData.MaxQueryElString = globalQueryData.MaxQueryEl.GetSql(null, joinCount);
                }

                if (!IsNull(globalQueryData.MinQueryEl))
                {
                    globalQueryData.MinQueryElString = globalQueryData.MinQueryEl.GetSql(null, joinCount);
                }

                if (!IsNull(globalQueryData.SumQueryEl))
                {
                    globalQueryData.SumQueryElString = globalQueryData.SumQueryEl.GetSql(null, joinCount);
                }

                if (!IsNull(avgQueryEl))
                {
                    avgQueryElString = avgQueryEl.GetSql(null, joinCount);
                }

                // orderbyStrings = orderbyStrings.Select(s => "[c" + _columns.IndexOf(s.FirstDb) + "] " + s.Second);

                // string columnsString = _columns.Select((c, i) => c + " [c" + i + "]").Concat(", ");
                bool orderByColumnsAdded = false;

                sql.Append("SELECT ");
                if (doDistinctQuery)
                {
                    sql.Append("DISTINCT ");
                }

                if (globalQueryData.TopCount.HasValue
                    && !DoWrapStatementForRowNumber(globalQueryData))
                {
                    sql.Append("TOP " + globalQueryData.TopCount.Value + " ");
                }

                List<string> queryOrderByColumns = null;
                if (!needsGlobalCount && globalQueryData.DoCount && CombinedQueries.Count == 0)
                {
                    var countOver = globalQueryData.DistinctCount ? ("DISTINCT " + queryColumns[0]) : "*";
                    sql.Append($"COUNT({countOver})");
                }
                else if (globalQueryData.MaxQueryElString != null)
                {
                    sql.Append("MAX(" + globalQueryData.MaxQueryElString + ")");
                }
                else if (globalQueryData.MinQueryElString != null)
                {
                    sql.Append("MIN(" + globalQueryData.MinQueryElString + ")");
                }
                else if (globalQueryData.SumQueryElString != null)
                {
                    sql.Append("SUM(" + globalQueryData.SumQueryElString + ")");
                }
                else
                {
                    sql.Append(GetColumnsStringWithAliases(queryColumns));
                    if (doDistinctQuery)
                    {
                        if (orderByColumns.Count > 0)
                        {
                            var orderByColumnsInSelectStatement = string.Join(", ",orderByColumns
                                .Select(c => OrderByAggregationToString(c.Item1, c.Item2))
                                .Except(selectColumns ?? queryColumns));

                            if (orderByColumnsInSelectStatement.Length > 0)
                            {
                                sql.Append(", " + orderByColumnsInSelectStatement);
                            }
                            orderByColumnsAdded = true;
                        }
                    }
                    else
                    {
                        if (orderByColumns.Count > 0
                            && (addOrderByColumns
                                || globalQueryData.SkipCount > 0
                                || CombinedQueries.Count > 0
                                || globalQueryData.TopCount > 0))
                        {
                            queryOrderByColumns =
                                orderByColumns
                                .Select(c => OrderByAggregationToString(c.Item1, c.Item2))
                                .Except(queryColumns)
                                .ToList();
                            if (queryOrderByColumns.Count > 0)
                            {
                                sql.Append(", ");
                                sql.Append(GetColumnsStringWithAliases(queryOrderByColumns, queryColumns.Count));
                                orderByColumnsAdded = true;
                            }
                        }
                    }
                }

                sql.Append(" FROM " + GetTableName() + " ");
                if (Db.Settings.WithNoLock)
                {
                    sql.Append("WITH (NOLOCK) ");
                }

                // 2TK BDA ist das effizient???
                sql.Append(string.Join(" ", joinStrings));
                if (whereStrings.Count > 0)
                {
                    sql.Append(" WHERE " + string.Join("", whereStrings));
                }

                if (groupByStrings.Count > 0)
                {
                    sql.Append(" GROUP BY " + string.Join(", ",groupByStrings.Union(orderByColumns.Where(t => t.Item2 == OrderByAggregation.None).Select(p => p.Item1))));
                }

                foreach (var combinedQuery in CombinedQueries)
                {
                    sql.Append(CombinationTypeToString(combinedQuery.Item2));
                    // Only take into account the fillDelegates of the first query, ignore those of all subsequent queries
                    sql.Append("(");
                    combinedQuery.Item1.AddToCommand(
                        new List<JoinedQueryDataSet>(),
                        sqlCommand,
                        sql,
                        orderByColumns,
                        new List<string>(),
                        addOrderByColumns: orderByColumnsAdded,
                        needsGlobalCount: globalQueryData.DoCount && CombinedQueries.Count > 0);
                    sql.Append(")");
                }

                if (DoWrapStatementForRowNumber(globalQueryData))
                {
                    StringBuilder rowNumberColumnsString;
                    string columnsString = null;
                    if (globalQueryData.DoCaseWhenExists)
                    {
                        columnsString = "1";
                        rowNumberColumnsString = new StringBuilder(columnsString);
                    }
                    else
                    {
                        columnsString = GetColumnsAliases(selectColumns, queryColumns);

                        if (orderByColumns.Count > 0)
                        {
                            // Nasty stuff...but I have no other solution at the moment, and it should not be called too often. So ignore poor performance.
                            rowNumberColumnsString = new StringBuilder();
                            foreach (var orderByColumn in orderByColumns)
                            {
                                if (rowNumberColumnsString.Length > 0)
                                {
                                    rowNumberColumnsString.Append(", ");
                                }

                                string rowNumberColumnString;
                                if (selectColumns != null)
                                {
                                    rowNumberColumnString = string.Concat("[Item", (selectColumns.IndexOf(orderByColumn.Item1) + 1), "]");
                                }
                                else
                                {
                                    int cIndex = queryColumns.IndexOf(orderByColumn.Item1);
                                    if (cIndex < 0 && queryOrderByColumns != null && queryOrderByColumns.Count > 0)
                                    {
                                        cIndex = queryOrderByColumns.IndexOf(orderByColumn.Item1) + queryColumns.Count;
                                    }

                                    rowNumberColumnString = string.Concat("[c", cIndex, "]");
                                }

                                rowNumberColumnsString.Append(
                                   OrderByAggregationToString(rowNumberColumnString, orderByColumn.Item2)
                                    + " "
                                    + OrderByTypeToString(orderByColumn.Item3));
                            }
                        }
                        else
                        {
                            rowNumberColumnsString = new StringBuilder(GetColumnsAliases(selectColumns, queryColumns));
                        }
                    }

                    sql.Insert(
                        0,
                        $@"WITH _chunk AS (SELECT {columnsString}, ROW_NUMBER() OVER (ORDER BY {rowNumberColumnsString}) [_RN_] FROM (");
                    sql.Append(") ___");
                    if (globalQueryData.TopCount.HasValue
                        && globalQueryData.TopCount.Value > 0)
                    {
                        sql.AppendFormat(
                            @") SELECT {0} FROM [_chunk] WHERE [_chunk].[_RN_] BETWEEN @skipCount + 1 AND @skipCount + @topCount ORDER BY [_chunk].[_RN_]",
                            columnsString);
                        sqlCommand.Parameters.AddWithValue("@topCount", globalQueryData.TopCount.Value);
                    }
                    else
                    {
                        sql.AppendFormat(@") SELECT {0} FROM [_chunk] WHERE [_chunk].[_RN_] > @skipCount ORDER BY [_chunk].[_RN_]", columnsString);
                    }

                    sqlCommand.Parameters.AddWithValue("@skipCount", globalQueryData.SkipCount);
                }

                // If there are unions etc., we create a big query around the query to count all elements
                if (globalQueryData.DoCount && CombinedQueries.Count > 0)
                {
                    sql.Insert(0, "WITH _count AS (");
                    sql.AppendFormat(") SELECT COUNT({0}) FROM [_count]", globalQueryData.DistinctCount ? "DISTINCT c0" : "*");
                }

                if (globalQueryData.DoCaseWhenExists)
                {
                    sql.Insert(0, "SELECT CASE WHEN EXISTS (");
                    sql.AppendFormat(") THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END");
                }

                return this;
            }
        }

        private bool DoWrapStatementForRowNumber(GlobalQueryData globalQueryData)
        {
            return globalQueryData.SkipCount > 0
                || (globalQueryData.TopCount.HasValue
                    && globalQueryData.TopCount.Value > 0
                    && CombinedQueries.Count > 0);
        }

        protected bool IsNull(QueryEl queryEl)
        {
            return ReferenceEquals(queryEl, null);
        }

        private static string GetColumnsAliases(IReadOnlyList<string> selectColumns, IReadOnlyList<string> columns)
        {
            StringBuilder columnsString = new StringBuilder();
            if (selectColumns != null)
            {
                for (int i = 0; i < selectColumns.Count; i++)
                {
                    columnsString.Append(", [Item");
                    columnsString.Append(i + 1);
                    columnsString.Append("]");
                }
            }
            else
            {
                for (int i = 0; i < columns.Count; i++)
                {
                    columnsString.Append(", [c");
                    columnsString.Append(i);
                    columnsString.Append("]");
                }
            }

            columnsString.Remove(0, 2);
            return columnsString.ToString();
        }

        internal void AttachEntities(IEnumerable<DbEntity> entities, IReadOnlyList<QueryToAttach> queriesToAttach)
        {
            if (queriesToAttach != null)
            {
                var ids = entities.Cast<ILongId>().Select(id => id.Id).ToList();

                foreach (var queryToAttach in queriesToAttach)
                {
                    var entitiesToAttach = queryToAttach
                        .QueryBase
                        .Clone(clonedSuperQuery: null)
                        .WhereDb(qw => queryToAttach.QueryFilter(qw, ids))
                        .ToList();

                    var lookup = entitiesToAttach.ToLookup(queryToAttach.GroupingKey);

                    foreach (var entity in entities)
                    {
                        queryToAttach.AttachEntitiesAction(entity, lookup[((ILongId)entity).Id].ToList());
                    }
                }
            }
        }


        // Two different "GetColumnsString..." methods for performance reasons...
        // 2 methods are about 10% faster than having 1 method with an if statement in the for loop
        private string GetColumnsStringWithAliases(List<string> _columns, int offset = 0)
        {
            StringBuilder columnsString = new StringBuilder();
            if (selectColumns != null)
            {
                for (int i = 0; i < selectColumns.Count; i++)
                {
                    columnsString.Append(", ");
                    columnsString.Append(selectColumns[i]);
                    columnsString.Append(" [Item");
                    columnsString.Append(i + 1 + offset);
                    columnsString.Append("]");
                }
            }
            else
            {
                for (int i = 0; i < _columns.Count; i++)
                {
                    columnsString.Append(", ");
                    columnsString.Append(_columns[i]);
                    columnsString.Append(" [c");
                    columnsString.Append(i + offset);
                    columnsString.Append("]");
                }
            }

            columnsString.Remove(0, 2);
            return columnsString.ToString();
        }

        private string CombinationTypeToString(CombinationType combinationType)
        {
            switch (combinationType)
            {
                case CombinationType.Union:
                    return " UNION ";

                case CombinationType.UnionAll:
                    return " UNION ALL ";

                case CombinationType.Intersect:
                    return " INTERSECT ";

                case CombinationType.Except:
                    return " EXCEPT ";

                default:
                    throw new NotImplementedException($"CombinationType {combinationType} is not implemented!");
            }
        }

        protected enum CombinationType : byte
        {
            Union,
            UnionAll,
            Intersect,
            Except
        };

        public enum OrderByType : byte
        {
            Ascending,
            Descending
        };

        internal abstract IReadOnlyList<DbEntity> WhereDb(Func<QueryWrapper, QueryEl> queryFilter);
    }
}