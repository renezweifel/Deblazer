using Dg.Deblazer.Internal;
using Dg.Deblazer.Read;
using Dg.Deblazer.SqlUtils;

namespace Dg.Deblazer.SqlGeneration
{
    public abstract class FunctionsHelperBase
    {
        protected static MultipleResultSetReader LoadMultipleResults(IDb db, DbSqlCommand sqlCommand)
        {
            return ((IDbInternal)db).LoadMultipleResults(sqlCommand);
        }
    }
}
