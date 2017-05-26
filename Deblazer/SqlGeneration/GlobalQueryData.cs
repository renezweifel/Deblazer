using System;
using System.Collections.Generic;

namespace Dg.Deblazer.SqlGeneration
{
    /// <summary>
    /// This class contains specific data about the database query like top, skip count , order and grouping information
    /// </summary>
    public class GlobalQueryData
    {
        public int? TopCount { get; set; }
        public int SkipCount { get; set; }
        public bool DoCount { get; set; }
        public bool DoGroup { get; set; }
        public int GroupingJoinCount { get; set; }
        public Type GroupingType { get; set; }
        public bool DistinctCount { get; set; }
        public bool DoCaseWhenExists { get; internal set; }
        public QueryEl MaxQueryEl { get; set; }
        public string MaxQueryElString { get; set; }
        public QueryEl MinQueryEl { get; set; }
        public string MinQueryElString { get; set; }
        public QueryEl SumQueryEl { get; set; }
        public string SumQueryElString { get; set; }

        private List<OrderByData> orderByQueryEls;
        public List<OrderByData> OrderByQueryEls
        {
            get { return orderByQueryEls; }
            set { orderByQueryEls = value; }
        }


        public GlobalQueryData()
        {
            orderByQueryEls = new List<OrderByData>();
        }

        public GlobalQueryData Clone()
        {
            var clone = MemberwiseClone() as GlobalQueryData;

            if (clone != null)
            {
                clone.GroupingType = GroupingType;
                clone.MaxQueryEl = MaxQueryEl;
                clone.MaxQueryElString = MaxQueryElString;
                clone.MinQueryEl = MinQueryEl;
                clone.MinQueryElString = MinQueryElString;
                clone.OrderByQueryEls = new List<OrderByData>(OrderByQueryEls);
                clone.SumQueryEl = SumQueryEl;
                clone.SumQueryElString = SumQueryElString;
            }

            return clone;
        }
    }

    public struct OrderByData
    {
        public readonly QueryEl QueryEl;
        public readonly QueryBase.OrderByType OrderByType;
        public readonly OrderByAggregation Aggregation;
        public readonly int JoinCount;

        public OrderByData(QueryEl queryEl, QueryBase.OrderByType orderByType, OrderByAggregation aggregation, int joinCount)
        {
            QueryEl = queryEl;
            OrderByType = orderByType;
            Aggregation = aggregation;
            JoinCount = joinCount;
        }
    }
}
