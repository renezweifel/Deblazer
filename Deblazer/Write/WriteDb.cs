using Dg.Deblazer.Api;
using Dg.Deblazer.Comparer;
using Dg.Deblazer.ContextValues.DgSpecific;
using Dg.Deblazer.Extensions;
using Dg.Deblazer.Settings;
using Dg.Deblazer.SqlGeneration;
using Dg.Deblazer.SqlUtils;
using Dg.Deblazer.Validation;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Dg.Deblazer.Write
{
 
    public abstract partial class WriteDb : BaseDb, IDbWrite
    {
        public int LoadedSetThreshold { get; set; } = int.MaxValue;
        private static readonly ConcurrentDictionary<Type, PropertyInfo[]> propertyInfosByType =
            new ConcurrentDictionary<Type, PropertyInfo[]>(concurrencyLevel: 1, capacity: 64);

        public new WriteDbSettings Settings => (WriteDbSettings)base.Settings;

        protected WriteDb(
            string connectionString,
            bool showEntitiesFromOtherMandators = false,
            bool checkUserRights = true,
            bool enableCache = true,
            bool allowLoadingBinaryData = false,
            bool allowSubmitChangesForSpecialCase = false)
            : this(settings: new WriteDbSettings(
                connectionString: connectionString,
                allowSubmitChangesForSpecialCase: allowSubmitChangesForSpecialCase)
            {
                ShowEntitiesFromOtherMandators = showEntitiesFromOtherMandators,
                CheckUserRights = checkUserRights,
                EnableCache = enableCache,
                AllowLoadingBinaryData = allowLoadingBinaryData
            })
        {
        }

        public WriteDb(WriteDbSettings settings)
            : base(settings)
        {
        }

        public int Execute(string sql, params object[] values)
        {
            return Execute(sql, Settings.CommandTimeout, values);
        }

        public int Execute(string sql, TimeSpan timeout, params object[] values)
        {
            using (var sqlConnection = GetConnection())
            {
                using (var sqlCommand = new DbSqlCommand(sql))
                {
                    sqlCommand.CommandTimeout = (int)timeout.TotalSeconds;
                    QueryHelpers.AddSqlParameters(sqlCommand, values);

                    sqlCommand.Connection = sqlConnection.SqlConnection;
                    sqlConnection.Open();

                    return sqlCommand.ExecuteNonQuery();
                }
            }
        }

        public void ExecuteOnBeginOfSubmit(string sql, params object[] values)
        {
            ExecuteOnBeginOfSubmit(sql, null, values);
        }

        public void ExecuteOnBeginOfSubmit(string sql, Action<int> onExecutedAction, params object[] values)
        {
            if (onBeginOfSubmitCommands == null)
            {
                onBeginOfSubmitCommands = new List<(DbSqlCommand command, Action<int> action)>();
            }

            var sqlCommand = new DbSqlCommand(sql);
            sqlCommand.CommandTimeout = (int)Settings.CommandTimeout.TotalSeconds;
            QueryHelpers.AddSqlParameters(sqlCommand, values);
            onBeginOfSubmitCommands.Add((sqlCommand, onExecutedAction));
        }

        public void ExecuteOnEndOfSubmit(string sql, params object[] values)
        {
            ExecuteOnEndOfSubmit(sql, null, values);
        }

        public void ExecuteOnEndOfSubmit(string sql, Action<int> onExecutedAction, params object[] values)
        {
            if (onEndOfSubmitCommands == null)
            {
                onEndOfSubmitCommands = new List<(DbSqlCommand, Action<int>)>();
            }

            var sqlCommand = new DbSqlCommand(sql);
            sqlCommand.CommandTimeout = (int)Settings.CommandTimeout.TotalSeconds;
            QueryHelpers.AddSqlParameters(sqlCommand, values);
            onEndOfSubmitCommands.Add((sqlCommand, onExecutedAction));
        }

        public void InsertOnSubmit(DbEntity entity)
        {
            if (entity != null)
            {
                if (!Settings.SupportSubmitChanges_Obsolete)
                {
                    //throw new NotSupportedException("Settings.SupportSubmitChanges == false!");
                }

                insertOnSubmitSet.Add(entity);
            }
        }

        public void InsertOnSubmit<T>(IEnumerable<T> entities) where T : DbEntity
        {
            foreach (T entity in entities)
            {
                InsertOnSubmit(entity);
            }

        }

        public bool IsMarkedForDeletion(DbEntity entity)
        {
            return deleteOnEndOfSubmitSet.Contains(entity) || deleteOnBeginOfSubmitSet.Contains(entity);
        }

        private bool DoHardDeleteEntity(DbEntity entity)
        {
            return !(entity is IDeleteDate)
                || iDeleteDateEntitiesToHardDelete?.Contains(entity) == true;
        }

        public void CheckForConcurrentChange(ICheckConcurrentUpdates entity)
        {
            if (entity != null)
            {
                checkConcurrentChangeOnSubmitSet.Add(entity);
            }
        }

        public void CheckForConcurrentChange<T>(IEnumerable<T> entities) where T : ICheckConcurrentUpdates
        {
            foreach (T entity in entities)
            {
                CheckForConcurrentChange(entity);
            }
        }

        public IReadOnlyList<ValidationError> GetValidationErrors()
        {
            var validationErrors = new List<ValidationError>();

            validationErrors.AddRange(deleteOnEndOfSubmitSet
                .Where(e => ((ILongId)e).Id > 0)
                .SelectMany(e => e.ValidationErrors));
            validationErrors.AddRange(deleteOnBeginOfSubmitSet
                .Where(e => ((ILongId)e).Id > 0)
                .SelectMany(e => e.ValidationErrors));

            var insertSetVisitor = GetInsertSetVisitor();
            var toInsert = insertSetVisitor.GetInsertSet();
            validationErrors.AddRange(toInsert
                .SelectMany(e => e.ValidationErrors));

            var updateSetVisitor = RunAndGetUpdateSetVisitor();
            var toUpdate = updateSetVisitor.UpdateSet;
            validationErrors.AddRange(toUpdate
                .SelectMany(e => e.ValidationErrors));

            return validationErrors;
        }

        public void ClearLoadedSetAndCache()
        {
            // Create a new instance instead of clearing the collection. when calling Clear() the capacity is not decreased
            // so a big array might still be kept in memory.
            loadedSet = new HashSet<DbEntity>(new ObjectReferenceEqualityComparer<DbEntity>());

            ClearCache();
        }

        public void ClearCache()
        {
            loadedEntityCache.ClearCache();
        }

        private void PreventNotSupportedOperations(
            IList<DbEntity> toDelete,
            IEnumerable<DbEntity> toInsert,
            IReadOnlyList<DbEntity> toUpdate)
        {

            // 2TK BDA IPreventSubmitChangesInterface
            //if ((MachineInfo.IsDevelopmentMachine && !Settings.AllowSubmitChangesOnDeviniteAndLocalRequest)
            //    || TestDetector.IsInMsTestOrIsSeleniumTestServer)
            //{
            //    var connectionStringBuilder = new SqlConnectionStringBuilder(Settings.ConnectionString);
            //    if ("devinite".EqualsCaseInsensitive(connectionStringBuilder.InitialCatalog)
            //        && (toInsert.Any(i => !(i is IAllowLocalChangesOnDevinite))
            //                              || toUpdate.Any(i => !(i is IAllowLocalChangesOnDevinite))
            //                              || toDelete.Any(i => !(i is IAllowLocalChangesOnDevinite))))
            //    {
            //        HashSet<Type> types = new HashSet<Type>(toInsert
            //                                                    .Where(i => !(i is IAllowLocalChangesOnDevinite))
            //                                                    .Select(i => i.GetType()));
            //        types.UnionWith(toUpdate.Where(i => !(i is IAllowLocalChangesOnDevinite)).Select(i => i.GetType()));
            //        types.UnionWith(toDelete.Where(i => !(i is IAllowLocalChangesOnDevinite)).Select(i => i.GetType()));
            //        throw new NotSupportedException(
            //            "SubmitChanges on devinite for types {0} is only allowed if Db.Settings().AllowSubmitChangesOnDeviniteAndLocalRequest is true or the request is not local".FormatWith(types.Select(t => t.FullName).Concat(", ")));
            //    }
            //}
        }
    }
}
