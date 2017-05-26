// example query class
// Tom Janssens
// http://www.corebvba.be
//
// conceived 2007/06/05 for codeproject http://www.codeproject.com/Articles/19056/Who-Needs-LINQ-Anyway-Build-Queries-with-Intellise
// File is licensed under CPOL https://www.codeproject.com/info/cpol10.aspx
// Modifications By Digitec Galaxus from 2009-2017

using Dg.Deblazer.Api;
using Dg.Deblazer.Configuration;
using Dg.Deblazer.Extensions;
using Dg.Deblazer.Internal;
using Dg.Deblazer.Read;
using Dg.Deblazer.SqlUtils;
using Dg.Deblazer.Visitors;
using JetBrains.Annotations;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Runtime.CompilerServices;

namespace Dg.Deblazer.SqlGeneration
{
    /// <summary>
    /// Documentation: https://erp.galaxus.ch/de/Wiki/4833
    /// </summary>
    public abstract class Query<TBack, TOriginal, TCurrent, TWrapper, TQuery> : QueryBase, IDbReadOnlyList<TOriginal>,
        IQueryInternal<TBack, TOriginal, TCurrent, TWrapper, TQuery>
        where TBack : QueryBase
        where TOriginal : DbEntity, ILongId
        where TCurrent : IQueryReturnType
        where TWrapper : QueryWrapper
        where TQuery : Query<TBack, TOriginal, TCurrent, TWrapper, TQuery>
    {
        private TQuery clone = null;
        private int? count;
        private bool? anyDb;

        protected Query(IDb db) : base(db)
        {
        }

        private IList cachedElements { get; set; }
        private readonly object cachedElementsLock = new object();

        public string SqlCommandText
        {
            get { return SqlCommands.GetSqlCommandText(GetSqlCommand(null).SqlCommand); }
        }

        IDbReadOnlyList<TOriginal> IDbReadOnlyList<TOriginal>.Clone()
        {
            return Clone();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        TOriginal IDbReadOnlyList<TOriginal>.FirstDb()
        {
            return FirstDb(callerFilePath: null, callerLineNumber: 0);
        }

        public int CountDb(
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            if (!count.HasValue)
            {
                var globalQueryData = GetGlobalQueryData();
                globalQueryData.DoCount = true;
                globalQueryData.DistinctCount = globalQueryData.DoGroup; // DistinctDb().CountDb() should result in COUNT(DISTINCT *)

                this.callerFilePath = callerFilePath;
                this.callerLineNumber = callerLineNumber;

                count = SqlCommands.ExecuteSqlCommand(() =>
                {
                    using (var sqlConnection = DbInternal.GetConnection())
                    {
                        using (var sqlCommand = GetSqlCommand(sqlConnection))
                        {
                            sqlConnection.Open();
                            return sqlCommand.SelectSingleValue<int>();
                        }
                    }
                });
            }

            return count.Value;
        }

        IEnumerator<TOriginal> IEnumerable<TOriginal>.GetEnumerator()
        {
            return GetEnumerator();
        }

        IDbReadOnlyList<TOriginal> IDbReadOnlyList<TOriginal>.SkipDb(int elementCount)
        {
            return SkipDb(elementCount);
        }

        IDbReadOnlyList<TOriginal> IDbReadOnlyList<TOriginal>.TakeDb(int elementCount)
        {
            return TakeDb(elementCount);
        }

        public TOriginal this[int index]
        {
            get
            {
                EnsureElementsAreLoaded<TOriginal>();
                return ((IReadOnlyList<TOriginal>)cachedElements)[index];
            }
        }

        public int Count
        {
            get
            {
                EnsureElementsAreLoaded<TOriginal>();
                return cachedElements.Count;
            }
        }

        public TQuery Clone()
        {
            ResetClone();
            return Clone(null) as TQuery;
        }

        public List<TOriginal> ToList(
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            this.callerFilePath = callerFilePath;
            this.callerLineNumber = callerLineNumber;

            EnsureElementsAreLoaded<TOriginal>();
            return new List<TOriginal>((IReadOnlyList<TOriginal>)cachedElements);
        }

        public IImmutableSet<TOriginal> ToReadOnlySet(
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            this.callerFilePath = callerFilePath;
            this.callerLineNumber = callerLineNumber;

            EnsureElementsAreLoaded<TOriginal>();
            return ((IReadOnlyList<TOriginal>)cachedElements).ToImmutableHashSet();
        }

        public ILookup<TKey, TOriginal> ToLookup<TKey>(
            Func<TOriginal, TKey> keySelector,
            IEqualityComparer<TKey> comparer = null,
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            this.callerFilePath = callerFilePath;
            this.callerLineNumber = callerLineNumber;

            return Enumerable.ToLookup(this, keySelector: keySelector, comparer: comparer);
        }

        public ILookup<TKey, TElement> ToLookup<TKey, TElement>(
            Func<TOriginal, TKey> keySelector,
            Func<TOriginal, TElement> elementSelector,
            IEqualityComparer<TKey> comparer = null,
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            this.callerFilePath = callerFilePath;
            this.callerLineNumber = callerLineNumber;

            return Enumerable.ToLookup(this, keySelector: keySelector, elementSelector: elementSelector, comparer: comparer);
        }

        internal override void ResetClone()
        {
            if (clone != null)
            {
                clone = null;

                if (superQuery != null)
                {
                    superQuery.ResetClone();
                }

                if (CombinedQueries != null)
                {
                    foreach (var combinedQuery in CombinedQueries)
                    {
                        combinedQuery.Item1.ResetClone();
                    }
                }

                if (joinedQueries != null)
                {
                    foreach (var joinedQuery in joinedQueries)
                    {
                        joinedQuery.ResetClone();
                    }
                }
            }
        }

        internal override QueryBase Clone(QueryBase clonedSuperQuery)
        {
            if (clone == null)
            {
                clone = MemberwiseClone() as TQuery;

                if (clone != null)
                {
                    clone.dbEntityTypeToLoad = dbEntityTypeToLoad;
                    clone.concatWithAnd = concatWithAnd;
                    clone.distinct = distinct;

                    clone.fillMember = fillMember;
                    clone.GetMember = GetMember;
                    clone.globalJoinCount = globalJoinCount;
                    clone.groupingJoinCount = groupingJoinCount;
                    clone.joinCount = joinCount;
                    clone.joinString = joinString;
                    clone.globalQueryData = GetGlobalQueryData().Clone();
                    clone.queryEl = queryEl;
                    clone.superQuery = clonedSuperQuery ?? (superQuery != null ? superQuery.Clone(null) : null);
                    clone.selectColumns = selectColumns == null ? null : new List<string>(selectColumns);
                    clone.options = options;

                    if (CombinedQueries != null)
                    {
                        clone.CombinedQueries = new List<(QueryBase, CombinationType)>();
                        foreach (var combinedQuery in CombinedQueries)
                        {
                            clone.AddCombinedQuery(combinedQuery.Item1.Clone(null), combinedQuery.Item2);
                        }
                    }

                    if (joinedQueries != null)
                    {
                        clone.joinedQueries = new List<QueryBase>();
                        foreach (var joinedQuery in joinedQueries)
                        {
                            clone.joinedQueries.Add(joinedQuery.Clone(this));
                        }
                    }
                }
            }

            return clone;
        }

        protected abstract TWrapper GetWrapper();

        [CanBeNull]
        public TOriginal SingleOrDefaultDb(
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            return TakeDb(2).ToList(callerFilePath: callerFilePath, callerLineNumber: callerLineNumber).SingleOrDefault();
        }

        [CanBeNull]
        public TOriginal SingleOrDefaultDb(
            Func<TWrapper, QueryEl> func,
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            return WhereDb(func).SingleOrDefaultDb(callerFilePath: callerFilePath, callerLineNumber: callerLineNumber);
        }

        public TOriginal SingleDb(
            Func<TWrapper, QueryEl> func,
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            return WhereDb(func).SingleDb(callerFilePath: callerFilePath, callerLineNumber: callerLineNumber);
        }

        public TOriginal SingleDb(
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            return TakeDb(2).ToList(callerFilePath: callerFilePath, callerLineNumber: callerLineNumber).Single();
        }

        public TQuery TakeDb(int elementCount)
        {
            var globalQueryData = GetGlobalQueryData();
            var topCount = globalQueryData.TopCount.HasValue ? globalQueryData.TopCount.Value : 0;
            globalQueryData.TopCount = Math.Max(topCount, elementCount);

            return (TQuery)this;
        }

        public TQuery SkipDb(int elementCount)
        {
            var globalQueryData = GetGlobalQueryData();
            globalQueryData.SkipCount = Math.Max(globalQueryData.SkipCount, elementCount);

            return (TQuery)this;
        }

        [CanBeNull]
        public TOriginal FirstOrDefaultDb(
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            return TakeOne().ToList(callerFilePath: callerFilePath, callerLineNumber: callerLineNumber).SingleOrDefault();
        }

        private TQuery TakeOne()
        {
            // when we take one element anyway we do not need to make a distinct. Distinct is an overhead for the Sql server in this case.
            SetDistinctFalseInAllConnectedQueries();
            return TakeDb(1);
        }

        [CanBeNull]
        public TOriginal FirstOrDefaultDb(
            Func<TWrapper, QueryEl> func,
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            return WhereDb(func).FirstOrDefaultDb(callerFilePath: callerFilePath, callerLineNumber: callerLineNumber);
        }

        public TOriginal FirstDb(
            Func<TWrapper, QueryEl> func,
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            return WhereDb(func).FirstDb(callerFilePath: callerFilePath, callerLineNumber: callerLineNumber);
        }

        public TOriginal FirstDb(
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            return TakeOne().ToList(callerFilePath: callerFilePath, callerLineNumber: callerLineNumber).First();
        }

        private QueryEl GetQueryEl(Func<TWrapper, QueryEl> func)
        {
            return func(GetWrapper());
        }

        public TQuery OrderByDb(Func<TWrapper, QueryEl> func, OrderByAggregation orderByAggregation = OrderByAggregation.None)
        {
            AddOrderByToTopMostSuperQuery(func(GetWrapper()), Tuple.Create(OrderByType.Ascending, orderByAggregation));

            return (TQuery)this;
        }

        public TQuery OrderByWithDirectionDb(Func<TWrapper, QueryEl> func, SortingDirection sortingDirection, OrderByAggregation orderByAggregation = OrderByAggregation.None)
        {
            switch (sortingDirection)
            {
                case SortingDirection.Ascending:
                    return OrderByDb(func, orderByAggregation);

                case SortingDirection.Descending:
                    return OrderByDescendingDb(func, orderByAggregation);

                default:
                    throw new NotImplementedException();
            }
        }

        public TQuery OrderByDescendingDb(Func<TWrapper, QueryEl> func)
        {
            return OrderByDescendingDb(func, OrderByAggregation.None);
        }

        public TQuery OrderByDescendingDb(Func<TWrapper, QueryEl> func, OrderByAggregation orderByAggregation)
        {
            AddOrderByToTopMostSuperQuery(func(GetWrapper()), Tuple.Create(OrderByType.Descending, orderByAggregation));

            return (TQuery)this;
        }

        public TQuery OrderByRandomDb()
        {
            // ORDER BY NEWID(), that's the way to do it
            // http://stackoverflow.com/questions/580639/how-to-randomly-select-rows-in-sql
            AddOrderByToTopMostSuperQuery(new QueryElLiteral("NEWID()"), Tuple.Create(OrderByType.Ascending, OrderByAggregation.None));

            return (TQuery)this;
        }

        public Id<TIId> MaxDb<TIId>(Func<TWrapper, QueryElMember<Id<TIId>>> func) where TIId : IId
        {
            var id = MaxDb(x => new QueryElMember<int>(func(x).MemberName));
            return new Id<TIId>(id);
        }

        public Id<TIId>? MaxDb<TIId>(Func<TWrapper, QueryElMember<Id<TIId>?>> func) where TIId : IId
        {
            var id = MaxDb(x => new QueryElMember<int?>(func(x).MemberName));
            return Id<TIId>.Nullable(id);
        }

        public U MaxDb<U>(Func<TWrapper, QueryElMember<U>> func)
        {
            GetGlobalQueryData().MaxQueryEl = func(GetWrapper());

            return SqlCommands.ExecuteSqlCommand(() =>
            {
                using (var sqlConnection = DbInternal.GetConnection())
                {
                    using (var sqlCommand = GetSqlCommand(sqlConnection))
                    {
                        sqlConnection.Open();
                        return sqlCommand.SelectSingleValue<U>();
                    }
                }
            });
        }

        public Id<TIId> MinDb<TIId>(Func<TWrapper, QueryElMember<Id<TIId>>> func) where TIId : IId
        {
            var id = MinDb(x => new QueryElMember<int>(func(x).MemberName));
            return new Id<TIId>(id);
        }

        public Id<TIId>? MinDb<TIId>(Func<TWrapper, QueryElMember<Id<TIId>?>> func) where TIId : IId
        {
            var id = MinDb(x => new QueryElMember<int?>(func(x).MemberName));
            return id.HasValue ? new Id<TIId>?(id.Value) : null;
        }

        public U MinDb<U>(Func<TWrapper, QueryElMember<U>> func)
        {
            GetGlobalQueryData().MinQueryEl = func(GetWrapper());

            return SqlCommands.ExecuteSqlCommand(() =>
            {
                using (var sqlConnection = DbInternal.GetConnection())
                {
                    using (var sqlCommand = GetSqlCommand(sqlConnection))
                    {
                        sqlConnection.Open();
                        return sqlCommand.SelectSingleValue<U>();
                    }
                }
            });
        }

        public int SumDb<TIId>(Func<TWrapper, QueryElMember<Id<TIId>>> func) where TIId : IId
        {
            GetGlobalQueryData().SumQueryEl = func(GetWrapper());

            return SqlCommands.ExecuteSqlCommand(() =>
            {
                using (var sqlConnection = DbInternal.GetConnection())
                {
                    using (var sqlCommand = GetSqlCommand(sqlConnection))
                    {
                        sqlConnection.Open();
                        return sqlCommand.SelectSingleValue<int>();
                    }
                }
            });
        }

        public int SumDb<TIId>(Func<TWrapper, QueryElMember<Id<TIId>?>> func) where TIId : IId
        {
            GetGlobalQueryData().SumQueryEl = func(GetWrapper());

            return SqlCommands.ExecuteSqlCommand(() =>
            {
                using (var sqlConnection = DbInternal.GetConnection())
                {
                    using (var sqlCommand = GetSqlCommand(sqlConnection))
                    {
                        sqlConnection.Open();
                        return sqlCommand.SelectSingleValue<int>();
                    }
                }
            });
        }

        public U SumDb<U>(Func<TWrapper, QueryElMember<U>> func)
        {
            GetGlobalQueryData().SumQueryEl = func(GetWrapper());

            return SqlCommands.ExecuteSqlCommand(() =>
            {
                using (var sqlConnection = DbInternal.GetConnection())
                {
                    using (var sqlCommand = GetSqlCommand(sqlConnection))
                    {
                        sqlConnection.Open();
                        return sqlCommand.SelectSingleValue<U>();
                    }
                }
            });
        }

        public int AvgDb<TIId>(Func<TWrapper, QueryElMember<Id<TIId>>> func) where TIId : IId
        {
            avgQueryEl = func(GetWrapper());

            return SqlCommands.ExecuteSqlCommand(() =>
            {
                using (var sqlConnection = DbInternal.GetConnection())
                {
                    using (var sqlCommand = GetSqlCommand(sqlConnection))
                    {
                        sqlConnection.Open();
                        return sqlCommand.SelectSingleValue<int>();
                    }
                }
            });
        }

        public int AvgDb<TIId>(Func<TWrapper, QueryElMember<Id<TIId>?>> func) where TIId : IId
        {
            avgQueryEl = func(GetWrapper());

            return SqlCommands.ExecuteSqlCommand(() =>
            {
                using (var sqlConnection = DbInternal.GetConnection())
                {
                    using (var sqlCommand = GetSqlCommand(sqlConnection))
                    {
                        sqlConnection.Open();
                        return sqlCommand.SelectSingleValue<int>();
                    }
                }
            });
        }

        public IReadOnlyList<TTargetType> SelectDistinctDb<TTargetType>(
            Func<TWrapper, QueryEl[]> funcs,
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            this.callerFilePath = callerFilePath;
            this.callerLineNumber = callerLineNumber;

            distinct = true;

            var selectedElements = SelectDb<TTargetType>(funcs, callerFilePath: callerFilePath, callerLineNumber: callerLineNumber);

            if (HasOrderByClause())
            {
                // see comment in SelectDistinctDb<T1>
                return selectedElements.Distinct().ToList();
            }
            else
            {
                return SelectDb<TTargetType>(funcs, callerFilePath: callerFilePath, callerLineNumber: callerLineNumber);
            }
        }

        public IReadOnlyList<T1> SelectDistinctDb<T1>(
            Func<TWrapper, QueryElMember<T1>> func,
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            this.callerFilePath = callerFilePath;
            this.callerLineNumber = callerLineNumber;

            // return DistinctDb().SelectDb(func); is wrong,
            distinct = true;

            var selectedElements = SelectDb(func, callerFilePath: callerFilePath, callerLineNumber: callerLineNumber);

            if (HasOrderByClause())
            {
                // Call Distinct(), because when calling
                // db.ProductMandatorCountries().Join(91495).Back().JoinProductMandatorCountryRankings(JoinType.Left).OrderByDb(r => r.RankingPeriodId.IfElse(HttpContext.Portal.Default_RankingPeriodId, r.Ranking, int.MaxValue))
                // We get 2 times the same entry if we don't call Distinct(), because the query above generates this SQL:
                // SELECT DISTINCT TOP 12 [t0].*, CASE WHEN [t2].[RankingPeriodId] = 6 THEN [t2].[Ranking] ELSE 2147483647 END
                // FROM [dbo].[ProductMandatorCountry] AS [t0]
                // INNER JOIN dbo.fn_IntsToTable('91495') AS [t1] ON [t1].[Value] = [t0].[Id] LEFT JOIN [dbo].[ProductMandatorCountryRanking] AS [t2]
                // ON [t0].[Id] = [t2].[ProductMandatorCountryId] ORDER BY CASE WHEN [t2].[RankingPeriodId] = 6 THEN [t2].[Ranking] ELSE 2147483647 END ASC
                // I don't see no better solution for that problem than calling Distinct() here...
                return selectedElements.Distinct().ToList();
            }
            else
            {
                return selectedElements;
            }
        }

        public IReadOnlyList<Tuple<T1, T2>> SelectDistinctDb<T1, T2>(
            Func<TWrapper, QueryElMember<T1>> func1,
            Func<TWrapper, QueryElMember<T2>> func2,
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            this.callerFilePath = callerFilePath;
            this.callerLineNumber = callerLineNumber;

            distinct = true;
            return SelectDb(func1, func2, callerFilePath: callerFilePath, callerLineNumber: callerLineNumber);
        }

        public IReadOnlyList<Tuple<T1, T2, T3>> SelectDistinctDb<T1, T2, T3>(
            Func<TWrapper, QueryElMember<T1>> func1,
            Func<TWrapper, QueryElMember<T2>> func2,
            Func<TWrapper, QueryElMember<T3>> func3,
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            this.callerFilePath = callerFilePath;
            this.callerLineNumber = callerLineNumber;

            distinct = true;
            return SelectDb(func1, func2, func3, callerFilePath: callerFilePath, callerLineNumber: callerLineNumber);
        }

        public IReadOnlyList<Tuple<T1, T2, T3, T4>> SelectDistinctDb<T1, T2, T3, T4>(
            Func<TWrapper, QueryElMember<T1>> func1,
            Func<TWrapper, QueryElMember<T2>> func2,
            Func<TWrapper, QueryElMember<T3>> func3,
            Func<TWrapper, QueryElMember<T4>> func4,
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            this.callerFilePath = callerFilePath;
            this.callerLineNumber = callerLineNumber;

            distinct = true;
            return SelectDb(func1, func2, func3, func4, callerFilePath: callerFilePath, callerLineNumber: callerLineNumber);
        }

        public IReadOnlyList<Tuple<T1, T2, T3, T4, T5>> SelectDistinctDb<T1, T2, T3, T4, T5>(
            Func<TWrapper, QueryElMember<T1>> func1,
            Func<TWrapper, QueryElMember<T2>> func2,
            Func<TWrapper, QueryElMember<T3>> func3,
            Func<TWrapper, QueryElMember<T4>> func4,
            Func<TWrapper, QueryElMember<T5>> func5,
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            this.callerFilePath = callerFilePath;
            this.callerLineNumber = callerLineNumber;

            distinct = true;
            return SelectDb(func1, func2, func3, func4, func5, callerFilePath: callerFilePath, callerLineNumber: callerLineNumber);
        }

        public TOriginal RandomElementDb()
        {
            return OrderByRandomDb()
                .TakeOne()
                .Single();
        }

        public TQuery DistinctDb()
        {
            GetGlobalQueryData().DoGroup = true;

            return (TQuery)this;
        }

        public int CountDb(Func<TWrapper, QueryEl> func,
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            return WhereDb(GetQueryEl(func)).CountDb(callerFilePath, callerLineNumber);
        }

        public bool AnyDb(Func<TWrapper, QueryEl> func,
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            WhereDb(GetQueryEl(func));
            return AnyDb(callerFilePath, callerLineNumber);
        }

        internal TQuery SetDoCaseWhenExists()
        {
            var globalQueryData = GetGlobalQueryData();
            globalQueryData.DoCaseWhenExists = true;

            return (TQuery)this;
        }

        public bool AnyDb(
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            if (!anyDb.HasValue)
            {
                // Do not use TakeDb(1).SelectDb(e => e.Id).Any()! It causes problems because SelectDb operates on the last query which may be null
                distinct = false;
                SetDoCaseWhenExists();

                this.callerFilePath = callerFilePath;
                this.callerLineNumber = callerLineNumber;

                anyDb = SqlCommands.ExecuteSqlCommand(() =>
                {
                    using (var sqlConnection = DbInternal.GetConnection())
                    {
                        using (var sqlCommand = GetSqlCommand(sqlConnection))
                        {
                            sqlConnection.Open();
                            return sqlCommand.SelectSingleValue<bool>();
                        }
                    }
                });
            }

            return anyDb.Value;
        }

        public bool NoneDb(Func<TWrapper, QueryEl> func,
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            return !AnyDb(func, callerFilePath, callerLineNumber);
        }

        public bool NoneDb(
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            return !AnyDb(callerFilePath, callerLineNumber);
        }

        public TQuery WhereDb(Func<TWrapper, QueryEl> func)
        {
            return WhereDb(GetQueryEl(func));
        }

        internal override IReadOnlyList<DbEntity> WhereDb(Func<QueryWrapper, QueryEl> queryFilter)
        {
            return WhereDb((Func<TWrapper, QueryEl>)queryFilter);
        }


        public TQuery WhereDb(string sql, params object[] parameters)
        {
            return WhereDb(new QueryElLiteral(sql, parameters));
        }

        private TQuery WhereDb(QueryEl whereQueryEl)
        {
            lock (cachedElementsLock)
            {
                cachedElements = null;
            }
            GetGlobalQueryData().DoCount = false;
            GetGlobalQueryData().DoCaseWhenExists = false;
            concatWithAnd = IsNull(queryEl) ? true : concatWithAnd;
            queryEl = IsNull(queryEl)
                ? whereQueryEl
                : new QueryElOperator<bool>("(({0}) AND ({1}))", QueryConversionHelper.ConvertMember(queryEl), QueryConversionHelper.ConvertMember(whereQueryEl));
            return (TQuery)this;
        }

        public TQuery OrDb(Func<TWrapper, QueryEl> func)
        {
            return OrDb(GetQueryEl(func));
        }

        private TQuery OrDb(QueryEl whereQueryEl)
        {
            lock (cachedElementsLock)
            {
                cachedElements = null;
            }
            GetGlobalQueryData().DoCount = false;
            GetGlobalQueryData().DoCaseWhenExists = false;
            concatWithAnd = IsNull(queryEl) ? false : concatWithAnd;
            queryEl = IsNull(queryEl)
                ? whereQueryEl
                : new QueryElOperator<bool>("(({0}) OR ({1}))", QueryConversionHelper.ConvertMember(queryEl), QueryConversionHelper.ConvertMember(whereQueryEl));
            return (TQuery)this;
        }

        protected override IEnumerable<string> GetColumns()
        {
            return QueryHelpers.GetColumnsInSelectStatement<TOriginal>(joinCount);
        }

        protected override string GetTableName()
        {
            return QueryHelpers.GetFullTableName<TOriginal>(joinCount);
        }

        public TBack Back()
        {
            if (GetType() == typeof(TBack))
            {
                return this as TBack;
            }
            else
            {
                return superQuery as TBack;
            }
        }

        private void AddCombinedQuery(QueryBase query, CombinationType combinationType)
        {
            CheckCombinedQueries(query);
            CombinedQueries.Add((query, combinationType));
        }

        public TQuery UnionDb<TBack2, TCurrent2, TWrapper2, TQuery2>(Query<TBack2, TOriginal, TCurrent2, TWrapper2, TQuery2> query2)
            where TBack2 : QueryBase
            where TCurrent2 : IQueryReturnType
            where TWrapper2 : QueryWrapper
            where TQuery2 : Query<TBack2, TOriginal, TCurrent2, TWrapper2, TQuery2>
        {
            AddCombinedQuery(query2, CombinationType.Union);
            return (TQuery)this;
        }

        public TQuery UnionAllDb<TBack2, TCurrent2, TWrapper2, TQuery2>(Query<TBack2, TOriginal, TCurrent2, TWrapper2, TQuery2> query2)
            where TBack2 : QueryBase
            where TCurrent2 : IQueryReturnType
            where TWrapper2 : QueryWrapper
            where TQuery2 : Query<TBack2, TOriginal, TCurrent2, TWrapper2, TQuery2>
        {
            AddCombinedQuery(query2, CombinationType.UnionAll);
            return (TQuery)this;
        }

        public TQuery IntersectDb<TBack2, TCurrent2, TWrapper2, TQuery2>(Query<TBack2, TOriginal, TCurrent2, TWrapper2, TQuery2> query2)
            where TBack2 : QueryBase
            where TCurrent2 : IQueryReturnType
            where TWrapper2 : QueryWrapper
            where TQuery2 : Query<TBack2, TOriginal, TCurrent2, TWrapper2, TQuery2>
        {
            AddCombinedQuery(query2, CombinationType.Intersect);
            return (TQuery)this;
        }

        public TQuery ExceptDb<TBack2, TCurrent2, TWrapper2, TQuery2>(Query<TBack2, TOriginal, TCurrent2, TWrapper2, TQuery2> query2)
            where TBack2 : QueryBase
            where TCurrent2 : IQueryReturnType
            where TWrapper2 : QueryWrapper
            where TQuery2 : Query<TBack2, TOriginal, TCurrent2, TWrapper2, TQuery2>
        {
            AddCombinedQuery(query2, CombinationType.Except);
            return (TQuery)this;
        }

        internal IEnumerator<TOriginal> GetEnumerator(
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            this.callerFilePath = callerFilePath;
            this.callerLineNumber = callerLineNumber;

            EnsureElementsAreLoaded<TOriginal>();
            return ((IReadOnlyList<TOriginal>)cachedElements).GetEnumerator();
        }

        private void EnsureElementsAreLoaded<U>()
        {
            if (cachedElements != null)
            {
                return;
            }

            lock (cachedElementsLock)
            {
                if (cachedElements != null)
                {
                    return;
                }

                var cachedTypedElements = new List<U>();
                if (GetGlobalQueryData().DoCount)
                {
                    throw new Exception("You cannot enumerate a query on which you used CountDb()! Call ToList() before calling Count().");
                }
                if (GetGlobalQueryData().DoCaseWhenExists)
                {
                    throw new Exception("You cannot enumerate a query on which you used AnyDb()! Call ToList() before calling Any().");
                }

                List<(U, List<object>)> loadedData = SqlCommands.ExecuteSqlCommand(() => ExecuteSqlCommands<U>());

                var configuration = GlobalDbConfiguration.GetConfigurationOrEmpty(typeof(U));
                var entityFilter = configuration.EntityFilter;
                var queryLogger = GlobalDbConfiguration.QueryLogger;

                for (int i = 0; i < loadedData.Count; i++)
                {
                    var entity = loadedData[i].Item1;
                    var joinedEntities = loadedData[i].Item2;

                    // Why?
                    if (entityFilter == null && entity != null)
                    {
                        var entityConfiguration = GlobalDbConfiguration.GetConfigurationOrEmpty(entity.GetType());
                        entityFilter = entityConfiguration.EntityFilter;
                        queryLogger = GlobalDbConfiguration.QueryLogger;
                    }

                    if (entityFilter != null && !entityFilter.DoReturnEntity(Db.Settings, entity, joinedEntities))
                    {
                        continue;
                    }

                    var dbEntity = entity as DbEntity;
                    if (dbEntity != null)
                    {
                        queryLogger.IncrementLoadedElementCount(increment: 1);
                        // Count the joined entities too
                        queryLogger.IncrementLoadedElementCount(increment: joinedEntities?.Count ?? 0);

                        ((IDbEntityInternal)dbEntity).SetAllowSettingColumns(allowSettingColumns: true);
                    }

                    cachedTypedElements.Add(entity);

                    if (typeof(U) == typeof(TOriginal))
                    {
                        EmitValueLoaded(entity as TOriginal);
                        if (joinedEntities != null)
                        {
                            for (int j = 0; j < joinedEntities.Count; j++)
                            {
                                dbEntity = joinedEntities[j] as DbEntity;
                                if (dbEntity != null)
                                {
                                    ((IDbEntityInternal)dbEntity).SetAllowSettingColumns(allowSettingColumns: true);
                                    // Add the entities to the loaded set directly because if we sth like
                                    // itemProduct.ItemProductStatic.ItemStateId = ItemStateIds.Lost;
                                    // itemProduct.ItemProductStatic = null;
                                    // Then the change to the original itemProduct.ItemProductStatic is not stored, because only itemProduct was in the loaded set, and
                                    // the connection to itemProduct.ItemProductStatic is lost in db.SubmitChanges()
                                    EmitValueLoaded(dbEntity);
                                }
                            }
                        }
                    }
                }

                // To avoid executing the SQL twice when first .AnyDb() is called (which only takes the first item) and then i.e. .CountDb() is called,
                // we first fully cache all elements and then yield return them
                cachedElements = cachedTypedElements.AsReadOnly();
            }
        }

        private List<(U, List<object>)> ExecuteSqlCommands<U>()
        {
            var loadedEntities = new List<(U, List<object>)>();
            List<JoinedQueryDataSet> fillDelegates;

            using (var sqlConnection = DbInternal.GetConnection())
            {
                using (var sqlCommand = GetSqlCommand(sqlConnection, out fillDelegates))
                {
                    sqlConnection.Open();

                    using (SqlDataReader reader = sqlCommand.ExecuteReader(CommandBehavior.SequentialAccess))
                    {
                        FillVisitor fillVisitor = new FillVisitor(
                            reader: reader,
                            db: Db,
                            objectFillerFactory: new ObjectFillerFactory());

                        int k = 0;
                        while (fillVisitor.Read())
                        {
                            U entity = QueryHelpers.Fill(default(U), fillVisitor);
                            var dbEntity = entity as DbEntity;
                            if (dbEntity != null)
                            {
                                ((IDbEntityInternal)dbEntity).SetAllowSettingColumns(allowSettingColumns: false);
                            }

                            if (entity is IId)
                            {
                                entity = (U)DbInternal.LoadedEntityCache.GetOrAdd(entity.GetType(), "Id", ((IId)entity).Id, entity);
                            }

                            k++;

                            var subEntities = new List<object>();

                            // If there are no explicit select collumns clause, we fill joined members
                            if (selectColumns == null)
                            {
                                if (fillVisitor.HasNext)
                                {
                                    for (int i = 0; i < fillDelegates.Count; i++)
                                    {
                                        object subEntity = fillDelegates[i].FillMember(entity, fillVisitor, PreProcessEntity);

                                        var subDbEntity = entity as DbEntity;
                                        if (subDbEntity != null)
                                        {
                                            ((IDbEntityInternal)subDbEntity).SetAllowSettingColumns(false);
                                        }

                                        subEntities.Add(subEntity);
                                    }
                                }
                            }

                            loadedEntities.Add((entity, subEntities));
                        }


                    }
                }
            }

            QueryBase rootQuery = this;
            while (rootQuery.superQuery != null)
            {
                rootQuery = rootQuery.superQuery;
            }
            AttachEntities(
                loadedEntities.Select(e => (DbEntity)(object)e.Item1),
                rootQuery.queriesToAttach);

            for (int i = 0; i < fillDelegates.Count; i++)
            {
                AttachEntities(
                    loadedEntities.Select(e => (DbEntity)e.Item2.Skip(i).First()),
                    fillDelegates[i].QueriesToPrefetch);
            }

            return loadedEntities;
        }

        public IReadOnlyDictionary<T1, T2> ToReadOnlyDictionaryDb<T1, T2>(
            Func<TWrapper, QueryElMember<T1>> keySelector,
            Func<TWrapper, QueryElMember<T2>> elementSelector,
            IEqualityComparer<T1> comparer,
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            this.callerFilePath = callerFilePath;
            this.callerLineNumber = callerLineNumber;

            return SelectDb(keySelector, elementSelector)
                .ToDictionary(s => s.Item1, s => s.Item2, comparer);
        }

        public IReadOnlyDictionary<T1, T2> ToReadOnlyDictionaryDb<T1, T2>(
            Func<TWrapper, QueryElMember<T1>> keySelector,
            Func<TWrapper, QueryElMember<T2>> elementSelector,
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            this.callerFilePath = callerFilePath;
            this.callerLineNumber = callerLineNumber;

            return SelectDb(keySelector, elementSelector)
                .ToDictionary(s => s.Item1, s => s.Item2);
        }

        public IDictionary<T1, T2> ToDictionaryDb<T1, T2>(
            Func<TWrapper, QueryElMember<T1>> keySelector,
            Func<TWrapper, QueryElMember<T2>> elementSelector,
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            this.callerFilePath = callerFilePath;
            this.callerLineNumber = callerLineNumber;

            return SelectDb(keySelector, elementSelector)
                .ToDictionary(s => s.Item1, s => s.Item2);
        }

        public IReadOnlyList<T1> SelectDb<T1>(
            Func<TWrapper, QueryElMember<T1>> func,
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            this.callerFilePath = callerFilePath;
            this.callerLineNumber = callerLineNumber;

            return SelectDb<T1>(func(GetWrapper()));
        }

        public string GetSelectDbSqlCommand<T1>(
            Func<TWrapper, QueryElMember<T1>> func,
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            this.callerFilePath = callerFilePath;
            this.callerLineNumber = callerLineNumber;

            return GetSelectDbSqlCommand<T1>(func(GetWrapper()));
        }

        public T1 SelectSingleDb<T1>(
          Func<TWrapper, QueryElMember<T1>> func,
          [CallerFilePath] string callerFilePath = null,
          [CallerLineNumber] int callerLineNumber = 0)
        {
            this.callerFilePath = callerFilePath;
            this.callerLineNumber = callerLineNumber;

            return TakeDb(2).SelectDb<T1>(func(GetWrapper())).Single();
        }

        public T1 SelectSingleOrDefaultDb<T1>(
          Func<TWrapper, QueryElMember<T1>> func,
          [CallerFilePath] string callerFilePath = null,
          [CallerLineNumber] int callerLineNumber = 0)
        {
            this.callerFilePath = callerFilePath;
            this.callerLineNumber = callerLineNumber;

            return TakeDb(2).SelectDb<T1>(func(GetWrapper())).SingleOrDefault();
        }

        public T1 SelectFirstDb<T1>(
          Func<TWrapper, QueryElMember<T1>> func,
          [CallerFilePath] string callerFilePath = null,
          [CallerLineNumber] int callerLineNumber = 0)
        {
            this.callerFilePath = callerFilePath;
            this.callerLineNumber = callerLineNumber;

            return TakeOne().SelectDb<T1>(func(GetWrapper())).First();
        }

        public T1 SelectFirstOrDefaultDb<T1>(
          Func<TWrapper, QueryElMember<T1>> func,
          [CallerFilePath] string callerFilePath = null,
          [CallerLineNumber] int callerLineNumber = 0)
        {
            this.callerFilePath = callerFilePath;
            this.callerLineNumber = callerLineNumber;

            return TakeOne().SelectDb<T1>(func(GetWrapper())).FirstOrDefault();
        }

        public IReadOnlyList<Tuple<T1, T2>> SelectDb<T1, T2>(
            Func<TWrapper, QueryElMember<T1>> func1,
            Func<TWrapper, QueryElMember<T2>> func2,
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            this.callerFilePath = callerFilePath;
            this.callerLineNumber = callerLineNumber;

            return SelectDb<Tuple<T1, T2>>(func1(GetWrapper()), func2(GetWrapper()));
        }

        public IReadOnlyList<Tuple<T1, T2, T3>> SelectDb<T1, T2, T3>(
            Func<TWrapper, QueryElMember<T1>> func1,
            Func<TWrapper, QueryElMember<T2>> func2,
            Func<TWrapper, QueryElMember<T3>> func3,
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            this.callerFilePath = callerFilePath;
            this.callerLineNumber = callerLineNumber;

            return SelectDb<Tuple<T1, T2, T3>>(func1(GetWrapper()), func2(GetWrapper()), func3(GetWrapper()));
        }

        public IReadOnlyList<Tuple<T1, T2, T3, T4>> SelectDb<T1, T2, T3, T4>(
            Func<TWrapper, QueryElMember<T1>> func1,
            Func<TWrapper, QueryElMember<T2>> func2,
            Func<TWrapper, QueryElMember<T3>> func3,
            Func<TWrapper, QueryElMember<T4>> func4,
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            this.callerFilePath = callerFilePath;
            this.callerLineNumber = callerLineNumber;

            return SelectDb<Tuple<T1, T2, T3, T4>>(func1(GetWrapper()), func2(GetWrapper()), func3(GetWrapper()), func4(GetWrapper()));
        }

        public IReadOnlyList<Tuple<T1, T2, T3, T4, T5>> SelectDb<T1, T2, T3, T4, T5>(
            Func<TWrapper, QueryElMember<T1>> func1,
            Func<TWrapper, QueryElMember<T2>> func2,
            Func<TWrapper, QueryElMember<T3>> func3,
            Func<TWrapper, QueryElMember<T4>> func4,
            Func<TWrapper, QueryElMember<T5>> func5,
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            return SelectDb<Tuple<T1, T2, T3, T4, T5>>(func1(GetWrapper()), func2(GetWrapper()), func3(GetWrapper()), func4(GetWrapper()), func5(GetWrapper()));
        }

        public IReadOnlyList<Tuple<T1, T2, T3, T4, T5, T6>> SelectDb<T1, T2, T3, T4, T5, T6>(
            Func<TWrapper, QueryElMember<T1>> func1,
            Func<TWrapper, QueryElMember<T2>> func2,
            Func<TWrapper, QueryElMember<T3>> func3,
            Func<TWrapper, QueryElMember<T4>> func4,
            Func<TWrapper, QueryElMember<T5>> func5,
            Func<TWrapper, QueryElMember<T6>> func6,
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            this.callerFilePath = callerFilePath;
            this.callerLineNumber = callerLineNumber;

            return SelectDb<Tuple<T1, T2, T3, T4, T5, T6>>(
                func1(GetWrapper()),
                func2(GetWrapper()),
                func3(GetWrapper()),
                func4(GetWrapper()),
                func5(GetWrapper()),
                func6(GetWrapper()));
        }

        public IReadOnlyList<TTargetType> SelectDb<TTargetType>(
            Func<TWrapper, QueryEl[]> funcs,
            [CallerFilePath] string callerFilePath = null,
            [CallerLineNumber] int callerLineNumber = 0)
        {
            this.callerFilePath = callerFilePath;
            this.callerLineNumber = callerLineNumber;

            return SelectDb<TTargetType>(funcs(GetWrapper()));
        }

        private IReadOnlyList<TTargetType> SelectDb<TTargetType>(params QueryEl[] queryEls)
        {
            MutateToSelectDbQuery<TTargetType>(queryEls);
            EnsureElementsAreLoaded<TTargetType>();
            return ((IReadOnlyList<TTargetType>)cachedElements);
        }

        private void MutateToSelectDbQuery<TTargetType>(params QueryEl[] queryEls)
        {
            var currentTableType = typeof(TCurrent);
            SetSelectColumns(currentTableType, queryEls);
            SetSelectColumnsInCombinedQueries<TTargetType>(currentTableType, new HashSet<QueryBase>(), queryEls);
        }

        private string GetSelectDbSqlCommand<TTargetType>(params QueryEl[] queryEls)
        {
            var myClone = (TQuery)Clone(null);
            myClone.MutateToSelectDbQuery<TTargetType>(queryEls);
            return myClone.SqlCommandText;
        }

        public TQuery OptionsDb(string options)
        {
            this.options = options;
            return (TQuery)this;
        }

        private QueryJoinElements<TQuery, TOriginal, Id<TOther>> JoinIds<TOther>(JoinType joinType, string memberName, IEnumerable<Id<TOther>> ids)
            where TOther : DbEntity, IId
        {
            QueryJoinElements<TQuery, TOriginal, Id<TOther>> joinedQuery = new QueryJoinIds<TQuery, TOriginal, TOther>(Db, ids);
            return Join(joinType, memberName, joinedQuery);
        }

        public QueryJoinElements<TQuery, TOriginal, LongId<TOther>> JoinIds<TOther>(JoinType joinType, string memberName, IEnumerable<LongId<TOther>> ids)
           where TOther : DbEntity, ILongId
        {
            QueryJoinElements<TQuery, TOriginal, LongId<TOther>> joinedQuery = new QueryJoinLongIds<TQuery, TOriginal, TOther>(Db, ids);
            return Join(joinType, memberName, joinedQuery);
        }

        private QueryJoinElements<TQuery, TOriginal, TValue> Join<TValue>(JoinType joinType, string memberName, QueryJoinElements<TQuery, TOriginal, TValue> joinedQuery)
        {
            if (typeof(TValue) == typeof(int)
                || typeof(TValue) == typeof(int?)
                || typeof(TValue) == typeof(string)
                || IdTypeExtension.IsIdType<TValue>()
                || IdTypeExtension.IsNullableIdType<TValue>()
                || LongIdTypeExtension.IsLongIdType<TValue>()
                || LongIdTypeExtension.IsNullableLongIdType<TValue>())
            {
                var globalJoinCountPlusOne = globalJoinCount + 1;

                string joinString = joinType.GetJoinString()
                    + " {3} AS [t"
                    + globalJoinCountPlusOne
                    + "] ON [t"
                    + globalJoinCountPlusOne
                    + "].[Value] = [t" + joinCount + "].["
                    + memberName
                    + "]";

                return Join(
                    joinedQuery,
                    joinString,
                    null,
                    null,
                    typeof(JoinEntity),
                    false);
            }
            else
            {
                throw new NotImplementedException($"Join for type {typeof(TValue).FullName} is not implemented yet. Ask Nicolas to do it");
            }
        }

        public class JoinEntity
        {
            public int Value { get; set; }
        }

        QueryJoinElements<TQuery, TOriginal, Id<TOther>> IQueryInternal<TBack, TOriginal, TCurrent, TWrapper, TQuery>.JoinIds<TOther>(
            JoinType joinType,
            string memberName,
            IEnumerable<Id<TOther>> ids)
        {
            return JoinIds(joinType, memberName, ids);
        }

        QueryJoinElements<TQuery, TOriginal, TValue> IQueryInternal<TBack, TOriginal, TCurrent, TWrapper, TQuery>.Join<TValue>(
            JoinType joinType,
            string memberName,
            QueryJoinElements<TQuery, TOriginal, TValue> joinedQuery)
        {
            return Join(joinType, memberName, joinedQuery);
        }

        TWrapper IQueryInternal<TBack, TOriginal, TCurrent, TWrapper, TQuery>.GetWrapper() => GetWrapper();

        IDbInternal IQueryInternal<TBack, TOriginal, TCurrent, TWrapper, TQuery>.Db => this.DbInternal;
        int IQueryInternal<TBack, TOriginal, TCurrent, TWrapper, TQuery>.GlobalJoinCount => globalJoinCount;
    }
}