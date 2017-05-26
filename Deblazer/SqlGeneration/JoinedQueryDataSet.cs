using System.Collections.Generic;

namespace Dg.Deblazer.SqlGeneration
{
    public class JoinedQueryDataSet
    {
        //FillDelegate<object> FillDele, IReadOnlyList<QueryToPrefetch>
        public readonly FillDelegate<object> FillMember;
        public readonly IReadOnlyList<QueryToAttach> QueriesToPrefetch;

        public JoinedQueryDataSet(FillDelegate<object> fillMember, IReadOnlyList<QueryToAttach> queriesToPrefetch)
        {
            this.FillMember = fillMember;
            this.QueriesToPrefetch = queriesToPrefetch;
        }
    }
}