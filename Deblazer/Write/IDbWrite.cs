using Dg.Deblazer.AggregateUpdate;
using Dg.Deblazer.Api;
using Dg.Deblazer.Settings;
using Dg.Deblazer.Validation;
using System;
using System.Collections.Generic;

namespace Dg.Deblazer.Write
{
    public interface IDbWrite : IDb
    {
        event Action<DbEntity> ItemMarkedForDeletion;

        /// <summary>
        /// Register events to execute after SubmitChanges()
        /// </summary>
        event Action<IDbWrite> SubmitChangesExecutedSuccessfully;


        new WriteDbSettings Settings { get; }
        void CheckForConcurrentChange(ICheckConcurrentUpdates entity);
        void CheckForConcurrentChange<T>(IEnumerable<T> entities) where T : ICheckConcurrentUpdates;
        void ClearCache();
        void ClearLoadedSetAndCache();
        void DeleteOnSubmit(DbEntity entity, DeletionInstant deletionInstant = DeletionInstant.OnEndOfSubmit, bool deleteIDeleteDate = false);
        void DeleteOnSubmit<T>(IEnumerable<T> entities, DeletionInstant deletionInstant = DeletionInstant.OnEndOfSubmit, bool deleteIDeleteDate = false) where T : DbEntity;
        int Execute(string sql, params object[] values);
        int Execute(string sql, TimeSpan timeout, params object[] values);
        void ExecuteOnBeginOfSubmit(string sql, params object[] values);
        void ExecuteOnBeginOfSubmit(string sql, Action<int> onExecutedAction, params object[] values);
        void ExecuteOnEndOfSubmit(string sql, params object[] values);
        void ExecuteOnEndOfSubmit(string sql, Action<int> onExecutedAction, params object[] values);
        IReadOnlyList<ValidationError> GetValidationErrors();
        void InsertOnSubmit(DbEntity entity);
        void InsertOnSubmit<T>(IEnumerable<T> entities) where T : DbEntity;
        bool IsMarkedForDeletion(DbEntity entity);
        void ReactivateOnSubmit<TDbEntity>(TDbEntity entity) where TDbEntity : DbEntity, IDeleteDate;
        void ReactivateOnSubmit<T>(IEnumerable<T> entities) where T : DbEntity, IDeleteDate;
        SubmitInfo SubmitChanges(
            TimeSpan transactionTimeout = default(TimeSpan),
            bool setDefaultValues = true,
            AggregateUpdateProcessingMode? aggregateUpdateMode = null
            );
    }
}