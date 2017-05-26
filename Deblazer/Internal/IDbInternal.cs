using System.Collections.Generic;
using Dg.Deblazer.Cache;
using Dg.Deblazer.Read;
using Dg.Deblazer.SqlGeneration;
using Dg.Deblazer.SqlUtils;

namespace Dg.Deblazer.Internal
{
    /// <summary>
    /// The purpose is to hide some Methods from the public API through explicitly implementing this Interface. This way the Methods are not visible for the Developer
    /// But can still be accessed from generated code.
    /// </summary>
    internal interface IDbInternal : IDb
    {
        void TriggerEntitiesLoaded(IReadOnlyList<DbEntity> entities);

        LoadedEntityCache LoadedEntityCache { get; }

        DbSqlConnection GetConnection();

        MultipleResultSetReader LoadMultipleResults(DbSqlCommand sqlCommand);

        QuerySql<T, T, T> LoadBy<T>(string[] columns, IReadOnlyList<int> ids);
        
        void RaiseNotifyMixedDb(MixedDbEventArgs mixedDbEventArgs);
    }
}