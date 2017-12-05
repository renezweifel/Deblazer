using Dg.Deblazer.AggregateUpdate;
using Dg.Deblazer.Api;
using Dg.Deblazer.Cache;
using Dg.Deblazer.Comparer;
using Dg.Deblazer.Configuration;
using Dg.Deblazer.ContextValues;
using Dg.Deblazer.ContextValues.DgSpecific;
using Dg.Deblazer.Extensions;
using Dg.Deblazer.Internal;
using Dg.Deblazer.Read;
using Dg.Deblazer.Settings;
using Dg.Deblazer.SqlGeneration;
using Dg.Deblazer.SqlUtils;
using Dg.Deblazer.Utils;
using Dg.Deblazer.Validation;
using Dg.Deblazer.Visitors;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Transactions;

namespace Dg.Deblazer.Write
{

    public partial class WriteDb
    {
        public static ICacheSerializer CacheSerializer { get; set; }
        public static event AfterSubmitChangesEventHandler OnSubmitChangesExecutedSuccessfully;
        public static event DeletionCompleteEventHandler OnDeletionCompleted;

        public const int MinEntitiesOfSameTypeForBulkInsert = 21;

        /// <summary>
        /// Register events to execute after SubmitChanges()
        /// </summary>
        public event Action<IDbWrite> SubmitChangesExecutedSuccessfully;

        protected HashSet<DbEntity> loadedSet = new HashSet<DbEntity>(new ObjectReferenceEqualityComparer<DbEntity>());
        protected readonly HashSet<DbEntity> insertOnSubmitSet = new HashSet<DbEntity>(new ObjectReferenceEqualityComparer<DbEntity>());
        protected readonly InsertOrderPreservingSet<DbEntity> deleteOnEndOfSubmitSet = new InsertOrderPreservingSet<DbEntity>();
        protected readonly InsertOrderPreservingSet<DbEntity> deleteOnBeginOfSubmitSet = new InsertOrderPreservingSet<DbEntity>();
        protected HashSet<DbEntity> iDeleteDateEntitiesToHardDelete;
        protected readonly List<ICheckConcurrentUpdates> checkConcurrentChangeOnSubmitSet = new List<ICheckConcurrentUpdates>();

        protected List<(DbSqlCommand, Action<int>)> onBeginOfSubmitCommands;
        protected List<(DbSqlCommand, Action<int>)> onEndOfSubmitCommands;

        public event Action<DbEntity> ItemMarkedForDeletion;
        private readonly IWriteDefaultInsertValues writeDefaultValues = new WriteDefaultInsertValues();
        public void RegisterInsertValueSetter<T>(Action<T> action) => writeDefaultValues.AddCustomValueSetter(action);

        private readonly IWriteDefaultUpdateValues writeDefaultUpdateValues = new WriteDefaultUpdateValues();

        public void RegisterUpdateValueSetter<TEntity, TColumn>(
            string dbColumnName,
            string dbColumnType,
            Action<TEntity, TColumn> setterFunction,
            Func<TColumn> valueFunction)
        {
            writeDefaultUpdateValues.AddCustomValueSetter(dbColumnName, dbColumnType, setterFunction, valueFunction);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="transactionTimeout"></param>
        /// <param name="setDefaultValues">Get or set whether columns like InsertUserId or UpdateDate should be set automatically by the DbLayer.</param>
        /// <param name="aggregateUpdateMode"></param>
        /// <returns></returns>
        public SubmitInfo SubmitChanges(
            TimeSpan transactionTimeout = default(TimeSpan),
            bool setDefaultValues = true,
            AggregateUpdateProcessingMode? aggregateUpdateMode = null)
        {
            if (transactionTimeout == default(TimeSpan))
            {
                transactionTimeout = Settings.SubmitChangesTimeout_Obsolete;
            }
            var aggregateUpdateModeNotNull = aggregateUpdateMode ?? Settings.AggregateUpdateMode_Obsolete;

            if (!Settings.SupportSubmitChanges_Obsolete)
            {
                throw new NotSupportedException("Settings.SupportSubmitChanges == false!");
            }

            // 2TK BDA if isanydevfarm
            if (transactionTimeout > TimeSpan.FromMinutes(10))
            {
                throw new InvalidOperationException("SubmitChanges with a timeout longer than 10 minutes makes no sense because it's the system upper limit.");
            }

            if (!Settings.AllowSubmitChangesForSpecialCase && !GlobalDbConfiguration.SubmitChangesConfiguration.SubmitChangesIsAllowed)
            {
                throw new NotSupportedException("SubmitChanges is not allowed in this context. Check the ISubmitChangesConfigurationHandler Implementations for you current Context or use WebSubmitChangesConfigurationHandler.Instance.AllowSubmitChangesForSpecialCases(db).");
            }

            // For correct SubmitInfo; entities which are not in the db cannot be deleted...
            deleteOnEndOfSubmitSet.RemoveWhere(e => ((ILongId)e).Id <= 0);
            deleteOnBeginOfSubmitSet.RemoveWhere(e => ((ILongId)e).Id <= 0);
            var toDelete = deleteOnEndOfSubmitSet.Union(deleteOnBeginOfSubmitSet).ToList();

            // Set the db to this instance such that we get no exception "Network access for Distributed Transaction Manager (MSDTC) has been disabled."
            // And before validating toDelete, as they might not have a _db set yet
            var dbUpdateVisitor = new SetDbVisitor(this);
            dbUpdateVisitor.Process(toDelete);

            // entities need to be validated for deletion before nullable foreign keys are removed!
            ThrowIfAnyHasValidationError(toDelete);
            var serializedDeletedEntitiesByContentAuditId = toDelete
                .OfType<IIsCached>()
                .GroupBy(i => i.ContentAuditId)
                .ToDictionary(t => t.Key, t => string.Join(";", t.Select(e => CacheSerializer?.Serialize(e))));

            // all the entities deleted in this submit should have the same delete date (except those that have one already set)
            var deleteDate = DateTime.Now;
            RemoveNullableForeignKeysOrSetDeleteDate(deleteOnBeginOfSubmitSet, deleteDate);
            RemoveNullableForeignKeysOrSetDeleteDate(deleteOnEndOfSubmitSet, deleteDate);

            var insertSetVisitor = GetInsertSetVisitor();
            var toInsert = insertSetVisitor.GetInsertSet();
            var updateSetVisitor = RunAndGetUpdateSetVisitor();
            var toUpdate = updateSetVisitor.UpdateSet;

            dbUpdateVisitor.Process(toInsert);
            dbUpdateVisitor.Process(toUpdate);

            // AppContext.StartRequestTiming(requestTimingString);

            var submitInfo = new SubmitInfo();
            if (toDelete.Count > 0
                || toInsert.Any()
                || toUpdate.Any()
                || (onBeginOfSubmitCommands != null && onBeginOfSubmitCommands.Count > 0)
                || (onEndOfSubmitCommands != null && onEndOfSubmitCommands.Count > 0))
            {
                var typesOfEntitiesToChange = toDelete
                    .Concat(toInsert)
                    .Concat(toUpdate)
                    .Select(e => e.GetType())
                    .ToImmutableHashSet();

                var namespacesOfEntitiesToChange = typesOfEntitiesToChange
                    .Select(t => t.Namespace)
                    .ToImmutableHashSet();

                if (namespacesOfEntitiesToChange.Count > 1)
                {
                    throw new Exception(
$@"There are entities of multiple db-layers on the same Db-Instance. This isn't supported (yet?). {Environment.NewLine}
Following Namespaces were used: {Environment.NewLine}
{string.Join(", ", namespacesOfEntitiesToChange)}");
                }

                var configuration = GlobalDbConfiguration.GetConfigurationOrEmpty(typesOfEntitiesToChange.FirstOrDefault());
                var aggregateUpdateService = configuration.AggregateUpdateService;
                configuration.EntityFilter.PreventInvalidOperations(Settings, toDelete, toInsert, toUpdate);

                RaiseBeforeInsertEvent(toInsert, insertSetVisitor, updateSetVisitor);

                ThrowIfAnyHasValidationError(toInsert);

                // throw the BeforeUpdate event before validation...
                RaiseBeforeUpdateEvent(toUpdate, insertSetVisitor, updateSetVisitor);
                ThrowIfAnyHasValidationError(toUpdate);

                PreventNotSupportedOperations(toDelete, toInsert, toUpdate);
                
                var allInserts = new List<DbEntity>(toInsert);
                var allUpdates = new List<DbEntity>(toUpdate);
                var allDeletes = new List<DbEntity>(toDelete);

                SqlCommands.ExecuteSqlCommand(() =>
                {
                    // The aggregateUpdates must be initialized here; if we init them outside SqlCommands.ExecuteSqlCommand, we get foreign key exception on the second try after a deadlock,
                    // because the availability updater tries to insert an AvailabilityInformation for a ProductMandatorCountry which wasn't inserted (in the first try, the updater recorded the
                    // ProductMandatorCountryId, which in the second try does not exist anymore)
                    var aggregateUpdates = aggregateUpdateService.GetAggregateUpdates(this);
                    int onSubmitCommandCount = 0;

                    var transactionOptions = new TransactionOptions { Timeout = transactionTimeout };
                    // On the unit test server we must use the default isolation level since some tests are surrounded by TransactionScopes for Rollback
                    // Otherwise we get this error: The transaction specified for TransactionScope has a different IsolationLevel than the value requested for the scope.
                    //if (!appContext.IsUnitTest)
                    //{
                    transactionOptions.IsolationLevel = System.Transactions.IsolationLevel.ReadCommitted;
                    //}

                    // The same entity (same type and Id, but different reference my be submitted several times during a submit)
                    var newRowVersions = new Dictionary<(Type typeid, long id), byte[]>();

                    var transactionStopwatch = Stopwatch.StartNew();
                    using (var transactionScope = new TransactionScope(TransactionScopeOption.Required, transactionOptions))
                    {
                        using (currentOpenConnection = new SqlConnection(Settings.ConnectionString))
                        {
                            try
                            {
                                currentOpenConnection.Open();

                                new DbSqlCommand("SET CONTEXT_INFO 0x9999", currentOpenConnection).ExecuteNonQuery();

                                onSubmitCommandCount += ExecuteOnSubmitCommands(onBeginOfSubmitCommands, transactionTimeout);
                                var logs = new Dictionary<DbEntity, DbEntity>(); // We only want <= 1 log per DbEntity, not more

                                writeDefaultValues.SetDefaultValues(insertSetVisitor.GetInsertSet());

                                // First the deletes...(after the pointers to the entities to be deleted have been updated in the db (-> UpdateSet))
                                DeleteEntities(deleteOnBeginOfSubmitSet);

                                InsertEntities(insertSetVisitor, currentOpenConnection, transactionTimeout, logs);

                                // Get the update set again as it may have changed because of the inserts above (-> foreign key references updated)
                                updateSetVisitor = RunAndGetUpdateSetVisitor();
                                RaiseAfterInsertEvent(toInsert, updateSetVisitor);

                                toUpdate = UpdateEntities(currentOpenConnection, updateSetVisitor, logs, newRowVersions, setDefaultValues);

                                // Finally the deletes...(after the pointers to the entities to be deleted have been updated in the db (-> UpdateSet))
                                DeleteEntities(deleteOnEndOfSubmitSet);

                                RaiseAfterDeleteEvent(toDelete);

                                var aggregateInserts = new List<DbEntity>();
                                var aggregateUpdateList = new List<DbEntity>();
                                var aggregateDeletes = new List<DbEntity>();

                                bool showEntitiesFromOtherMandators = Settings.ShowEntitiesFromOtherMandators;
                                bool checkUserRights = Settings.CheckUserRights;
                                bool enableCache = Settings.EnableCache;
                                Settings.ShowEntitiesFromOtherMandators = true;
                                Settings.CheckUserRights = false;
                                Settings.EnableCache = true;
                                foreach (var aggregateUpdate in aggregateUpdates)
                                {
                                    aggregateUpdate.AddDbEntitiesToUpdate(toInsert);
                                    aggregateUpdate.AddDbEntitiesToUpdate(toUpdate);
                                    aggregateUpdate.AddDbEntitiesToUpdate(toDelete);
                                }

                                // Reset the entities
                                var resetInsertVisitor = new InsertSetVisitor(doReset: true);
                                resetInsertVisitor.Process(toInsert);
                                updateSetVisitor = new UpdateSetVisitor(doReset: true, columnsToReset: null, doProcessChildren: true);
                                updateSetVisitor.Process(loadedSet);

                                deleteOnEndOfSubmitSet.Clear();
                                deleteOnBeginOfSubmitSet.Clear();

                                int ii = 0;
                                foreach (var aggregateUpdate in aggregateUpdates)
                                {
                                    var dbEntityIdsToUpdateCount = aggregateUpdate.GetEntitiesToUpdateCount();
                                    if (dbEntityIdsToUpdateCount > 0)
                                    {
                                        AggregateUpdateProcessingMode processingMode;
                                        if (aggregateUpdate is IAggregateUpdateWithEnforcedMode)
                                        {
                                            processingMode = ((IAggregateUpdateWithEnforcedMode)aggregateUpdate).EnforcedMode;
                                        }
                                        else
                                        {
                                            processingMode = aggregateUpdateModeNotNull;
                                        }

                                        if ((processingMode == AggregateUpdateProcessingMode.InSubmitChangesIfBelowChunkSize && dbEntityIdsToUpdateCount <= aggregateUpdate.ChunkSize)
                                            || processingMode == AggregateUpdateProcessingMode.InSubmitChanges)
                                        {
                                            aggregateUpdate.UpdateEntities();
                                            onSubmitCommandCount += ExecuteOnSubmitCommands(onBeginOfSubmitCommands, transactionTimeout);

                                            RemoveNullableForeignKeysOrSetDeleteDate(deleteOnBeginOfSubmitSet, deleteDate);
                                            RemoveNullableForeignKeysOrSetDeleteDate(deleteOnEndOfSubmitSet, deleteDate);
                                            toDelete = deleteOnEndOfSubmitSet.Union(deleteOnBeginOfSubmitSet).ToList();
                                            DeleteEntities(deleteOnBeginOfSubmitSet);

                                            insertSetVisitor = GetInsertSetVisitor(ignoreLoadedSet: true);
                                            toInsert = insertSetVisitor.GetInsertSet();
                                            foreach (var entity in toInsert)
                                            {
                                                ((IDbEntityInternal)entity).SetDb(this);
                                            }
                                            aggregateInserts.AddRange(toInsert);
                                            allInserts.AddRange(toInsert);

                                            writeDefaultValues.SetDefaultValues(insertSetVisitor.GetInsertSet());

                                            InsertEntities(insertSetVisitor, currentOpenConnection, transactionTimeout, logs);
                                            updateSetVisitor = RunAndGetUpdateSetVisitor();
                                            RaiseAfterInsertEvent(toInsert, updateSetVisitor);

                                            toUpdate = UpdateEntities(currentOpenConnection, updateSetVisitor, logs, newRowVersions, setDefaultValues);
                                            foreach (var entity in toUpdate)
                                            {
                                                ((IDbEntityInternal)entity).SetDb(this);
                                            }
                                            aggregateUpdateList.AddRange(toUpdate);
                                            allUpdates.AddRange(toUpdate);

                                            foreach (var entity in toDelete)
                                            {
                                                ((IDbEntityInternal)entity).SetDb(this);
                                            }
                                            aggregateDeletes.AddRange(toDelete);
                                            allDeletes.AddRange(toDelete);
                                            DeleteEntities(deleteOnEndOfSubmitSet);

                                            for (int jj = ii + 1; jj < aggregateUpdates.Count; jj++)
                                            {
                                                aggregateUpdates[jj].AddDbEntitiesToUpdate(toInsert);
                                                aggregateUpdates[jj].AddDbEntitiesToUpdate(toUpdate);
                                                aggregateUpdates[jj].AddDbEntitiesToUpdate(toDelete);
                                            }

                                            // Reset the entities
                                            resetInsertVisitor.Process(toInsert);
                                            insertOnSubmitSet.Clear();
                                            updateSetVisitor = new UpdateSetVisitor(doReset: true, columnsToReset: null, doProcessChildren: true);
                                            updateSetVisitor.Process(loadedSet);
                                            deleteOnEndOfSubmitSet.Clear();
                                            deleteOnBeginOfSubmitSet.Clear();

                                            onSubmitCommandCount += ExecuteOnSubmitCommands(onEndOfSubmitCommands, transactionTimeout);
                                        }
                                        else
                                        {
                                            aggregateUpdateService.AddAggregateUpdateToProcess(aggregateUpdate, e => InsertEntity(e, currentOpenConnection));
                                        }
                                    }

                                    ii++;
                                }

                                if (logs.Count > 0)
                                {
                                    var insertSetVisitorLogs = new InsertSetVisitor(doReset: false);
                                    foreach (var log in logs.Values)
                                    {
                                        insertSetVisitorLogs.InsertEntity(log);
                                    }

                                    InsertEntities(insertSetVisitorLogs, currentOpenConnection, transactionTimeout, logs: null);
                                    foreach (var log in logs.Values)
                                    {
                                        resetInsertVisitor.Process(log);
                                        allInserts.Add(log);
                                    }
                                }

                                onSubmitCommandCount += ExecuteOnSubmitCommands(onEndOfSubmitCommands, transactionTimeout);

                                CheckForConcurrentChanges(checkConcurrentChangeOnSubmitSet, newRowVersions);
                                checkConcurrentChangeOnSubmitSet.Clear();

                                Settings.ShowEntitiesFromOtherMandators = showEntitiesFromOtherMandators;
                                Settings.CheckUserRights = checkUserRights;
                                Settings.EnableCache = enableCache;

                                UpdateContentAudit(allInserts.OfType<IIsCached>(), allUpdates.OfType<IIsCached>(), allDeletes.OfType<IIsCached>(), serializedDeletedEntitiesByContentAuditId);

                                var deletesToNotify = allDeletes.OfType<INotifyEntityDelete>().ToList();
                                DeletionCompleted(transactionTimeout, deletesToNotify);

                                deleteOnBeginOfSubmitSet.Clear();
                                deleteOnEndOfSubmitSet.Clear();
                                insertOnSubmitSet.Clear();
                            }

                            // It is important that DBConcurrencyException is not caught here, so do not "catch (Exception ex)"!                            
                            catch (SqlException ex)
                            {
                                var entities = toInsert.Union(toUpdate).Union(toDelete);

                                if (entities.Any())
                                {
                                    RaiseOnSubmitTransactionAbortedEvent(entities);
                                    ResetUpdatedValues(toDelete, toInsert, toUpdate);

                                    if (entities.Count() <= 5)
                                    {
                                        throw new DbSqlException(
                                           $"Cannot submit {string.Join(",", entities.Select(e => $"{e.GetType().Name} ({ObjectUtils.PropertiesToString(e)})"))}: {ex.Message}", ex);
                                    }
                                    else
                                    {
                                        throw new DbSqlException(
                                           $"Cannot submit {entities.Select(e => e.GetType().Name).GroupBy(n => n).Select(g => $"{g.Count()}x {g.Key}")}: {ex.Message}", ex);
                                    }
                                }
                            }
                            finally
                            {
                                currentOpenConnection.Close();
                                currentOpenConnection = null;
                            }
                        }

                        // Commit the whole transaction at the very end
                        transactionScope.Complete();
                    }

                    transactionStopwatch.Stop();

                    if (newRowVersions.Count > 0)
                    {
                        byte[] newRowVersion;
                        foreach (var updatedEntity in allUpdates.Union(allInserts).OfType<ICheckConcurrentUpdates>())
                        {
                            if (newRowVersions.TryGetValue((updatedEntity.GetType(), updatedEntity.Id), out newRowVersion))
                            {
                                // Update the rowVersions at the end, because they only are updated in the database when the transaction commits
                                updatedEntity.RowVersion = newRowVersion;
                            }
                        }
                    }

                    OnSubmitChangesExecutedSuccessfully?.Invoke(this, allInserts, allUpdates, allDeletes);
                    SubmitChangesExecutedSuccessfully?.Invoke(this);

                    // Remove all references to other entities, only after the IAggregateUpdate have been executed, because they may rely on the foreign keys... (like SupplierDeliveryInformation.cs:line 23 -> AffectedProductMandatorIds())
                    allDeletes.ForEach(d => RemoveNonNullableForeignKeys(d));

                    submitInfo.TransactionDuration = transactionStopwatch.Elapsed;
                    submitInfo.UpdateCount = allUpdates.Count;
                    submitInfo.InsertCount = allInserts.Count;
                    submitInfo.DeleteCount = allDeletes.Count;
                    submitInfo.OnSubmitCommandCount = onSubmitCommandCount;
                });
            }

            GlobalDbConfiguration.QueryLogger.LogTransaction(submitInfo.TransactionDuration);


            return submitInfo;
        }

        private void DeletionCompleted(TimeSpan transactionTimeout, List<INotifyEntityDelete> deletesToNotify)
        {
            if (OnDeletionCompleted != null && deletesToNotify.Any())
            {
                var afterDeleteEventArgs = new AfterDeleteEventArgs();
                OnDeletionCompleted?.Invoke(deletesToNotify, afterDeleteEventArgs);
                InsertOnDeletetionCompleteResults(afterDeleteEventArgs.EntitiesToInsert, transactionTimeout);
            }
        }

        private static void RaiseBeforeUpdateEvent(IEnumerable<DbEntity> toInsert, InsertSetVisitor insertSetVisitor, UpdateSetVisitor updateSetVisitor)
        {
            var dbEntitiesToReprocess = GetDbEntitiesToReprocess(toInsert, e => e.RaiseBeforeUpdateEvent());

            if (dbEntitiesToReprocess.Count > 0)
            {
                insertSetVisitor.Reprocess(dbEntitiesToReprocess);
                updateSetVisitor.Reprocess(dbEntitiesToReprocess, processOnlyLoadedEntities: true);
            }
        }

        private static void RaiseBeforeInsertEvent(IEnumerable<DbEntity> toInsert, InsertSetVisitor insertSetVisitor, UpdateSetVisitor updateSetVisitor)
        {
            var dbEntitiesToReprocess = GetDbEntitiesToReprocess(toInsert, e => e.RaiseBeforeInsertEvent());

            if (dbEntitiesToReprocess.Count > 0)
            {
                insertSetVisitor.Reprocess(dbEntitiesToReprocess);
                updateSetVisitor.Reprocess(dbEntitiesToReprocess, processOnlyLoadedEntities: true);
            }
        }

        private static void RaiseAfterInsertEvent(IEnumerable<DbEntity> toInsert, UpdateSetVisitor updateSetVisitor)
        {
            var dbEntitiesToReprocess = toInsert.Cast<IRaiseDbSubmitEvent>().SelectMany(e => e.RaiseAfterInsertEvent());
            // Do not use the pattern with updateSetVisitor.Reprocess here because after the insert, all entities that 
            // were inserted and are connected to an entity raising the AfterInsert event would be updated again in the database
            // because their columns have not been reset yet. This may be a big overhead and may lead to exceptions (see SUP-2659)
            // for example when inserting an order (the ItemProducts connected to the invoicing triggering the AfterInsert event
            // are updated again in the database, throwing a DbConcurrentException)
            dbEntitiesToReprocess.ForEach(e => updateSetVisitor.ReprocessSingleEntity(e, processOnlyLoadedEntities: false));
        }

        private static void RaiseOnSubmitTransactionAbortedEvent(IEnumerable<DbEntity> entities)
        {
            Func<IRaiseDbSubmitEvent, bool> eventTrigger = e => e.RaiseOnSubmitTransactionAbortedEvent();
            foreach (var dbEntity in entities)
            {
                eventTrigger(dbEntity);
            }
        }

        private static void RaiseAfterDeleteEvent(IEnumerable<DbEntity> entities)
        {
            Func<IRaiseDbSubmitEvent, bool> eventTrigger = e => e.RaiseAfterDeleteEvent();
            foreach (var dbEntity in entities)
            {
                eventTrigger(dbEntity);
            }
        }

        private static IReadOnlyList<DbEntity> GetDbEntitiesToReprocess(IEnumerable<DbEntity> entities, Func<IRaiseDbSubmitEvent, bool> eventTrigger)
        {
            var dbEntitiesToReprocess = new List<DbEntity>();
            foreach (var dbEntity in entities)
            {
                if (eventTrigger(dbEntity))
                {
                    dbEntitiesToReprocess.Add(dbEntity);
                }
            }

            return dbEntitiesToReprocess;
        }

        private void RemoveNullableForeignKeysOrSetDeleteDate(ISet<DbEntity> toDelete, DateTime deleteDate)
        {
            foreach (var entity in toDelete.ToList())
            {
                // We do not want to lazy load all neighbors when deleting a DbContent or a Stamp. A DbContent/Stamp should be connected to one DbEntity exclusively anyway.
                // The DbContentController.Delete-action will set the referencing properties to null by reflection.
                if (entity is IDoNotRemoveNullableForeignKeysOrSetDeleteDate)
                {
                    continue;
                }

                // Only update the neighbour references, if this entity has no DeleteDate and will thus be really deleted
                // upon SubmitChanges
                if (DoHardDeleteEntity(entity))
                {
                    foreach (var pair in ObjectUtils.GetAllNeighborsWithNullablePointers(entity, p => p.IsDefined(typeof(ValidateAttribute), inherit: false)))
                    {
                        if (!IsMarkedForDeletion(pair.Item2))
                        {
                            // Set the reference to null
                            pair.Item1.SetValue(obj: pair.Item2, value: null, index: null);
                            // And add the neighbor to the loadedSet because its reference got updated
                            OnEntitiesLoaded(new[] { pair.Item2 });
                        }
                    }
                }
                else
                {
                    // do not update the delete date if it is already set
                    if (!((IDeleteDate)entity).DeleteDate.HasValue)
                    {
                        // Handle this like a normal update such that the UpdateUserSession and the UpdateUser are set
                        ((IDeleteDate)entity).DeleteDate = deleteDate;
                    }
                    toDelete.Remove(entity);
                }
            }
        }

        private void RemoveNonNullableForeignKeys(DbEntity entity)
        {
            // Only update the neighbour references, if this entity has no DeleteDate and will thus be really deleted
            // upon SubmitChanges
            if (DoHardDeleteEntity(entity))
            {
                var type = entity.GetType();
                var propertyInfos = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);
                foreach (var propertyInfo in propertyInfos)
                {
                    // Set the reference to null
                    if (propertyInfo.CanWrite && propertyInfo.PropertyType.Namespace == type.Namespace)
                    {
                        propertyInfo.SetValue(obj: entity, value: null, index: null);
                    }
                }
            }
        }

        private IReadOnlyList<DbEntity> UpdateEntities(
            SqlConnection sqlConnection,
            UpdateSetVisitor updateSetVisitor,
            Dictionary<DbEntity, DbEntity> logs,
            Dictionary<(Type typeid, long id), byte[]> newRowVersions,
            bool setDefaultValues)
        {
            if (WriteDbSettings.UseSqlBulkCopyForUpdates_Experimental)
            {
                return UpdateEntitiesWithSqlBulkCopy(sqlConnection, updateSetVisitor, logs, newRowVersions, setDefaultValues);
            }
            else
            {
                return UpdateEntitiesWithQueryBatch(sqlConnection, updateSetVisitor, logs, newRowVersions, setDefaultValues);
            }
        }

        private IReadOnlyList<DbEntity> UpdateEntitiesWithQueryBatch(
            SqlConnection sqlConnection,
            UpdateSetVisitor updateSetVisitor,
            Dictionary<DbEntity, DbEntity> logs,
            Dictionary<(Type typeid, long id), byte[]> newRowVersions,
            bool setDefaultValues)
        {
            SetDefaultUpdateValues(updateSetVisitor, setDefaultValues);

            updateSetVisitor.DoProcessChildren = false;
            GetLogEntities(updateSetVisitor, logs);

            // Then the updates (which may have changed because the id's of the objects that have been inserted have been set)...
            var updateCommandWithAffectedEntities = updateSetVisitor.GetCommands(sqlConnection, newRowVersions);

            var checkForConcurrentChangesEntities = updateCommandWithAffectedEntities
                .SelectMany(u => u.UpdatedDbEntityInstances)
                .OfType<ICheckConcurrentUpdates>()
                .ToList();

            try
            {
                // Use Batch update for better performance
                DbSqlCommand.ExecuteNonQueryBatch(updateCommandWithAffectedEntities.Select(c => c.DbSqlCommand).ToList(), sqlConnection);
            }
            catch (SqlException ex) when (ex.Number == 33219281)
            {
                throw new DBConcurrencyException(ex.Message, ex);
            }
            catch (SqlException ex)
            {
                var firstEntity = updateCommandWithAffectedEntities.First().UpdatedDbEntityInstances.First();
                string message = $"Cannot update batch {firstEntity.GetType().Name} ({ObjectUtils.PropertiesToString(firstEntity)})";
                throw new DbSqlException(message, ex);
            }

            UpdateRowVersions(newRowVersions, checkForConcurrentChangesEntities);

            return updateSetVisitor.UpdateSet;
        }

        private IReadOnlyList<DbEntity> UpdateEntitiesWithSqlBulkCopy(
            SqlConnection sqlConnection,
            UpdateSetVisitor updateSetVisitor,
            Dictionary<DbEntity, DbEntity> logs,
            Dictionary<(Type typeid, long id), byte[]> newRowVersions,
            bool setDefaultValues)
        {
            SetDefaultUpdateValues(updateSetVisitor, setDefaultValues);

            GetLogEntities(updateSetVisitor, logs);

            try
            {
                updateSetVisitor.UpdateEntitiesUsingSqlBulkCopy(sqlConnection, (int)Settings.CommandTimeout.TotalSeconds, newRowVersions);
            }
            catch (SqlException ex) when (ex.Number == 33219281)
            {
                throw new DBConcurrencyException(ex.Message, ex);
            }
            catch (SqlException ex)
            {
                var firstEntity = updateSetVisitor.UpdateSet.First();
                string message = $"Cannot update batch {firstEntity.GetType().Name} ({ObjectUtils.PropertiesToString(firstEntity)})";
                throw new DbSqlException(message, ex);
            }

            return updateSetVisitor.UpdateSet;
        }

        private void UpdateRowVersions(Dictionary<(Type typeid, long id), byte[]> newRowVersions, List<ICheckConcurrentUpdates> checkForConcurrentChangesEntities)
        {
            if (checkForConcurrentChangesEntities.Count > 0)
            {
                var entitiesByTableNames = checkForConcurrentChangesEntities
                    .ToLookup(k1 => QueryHelpers.GetFullTableName(k1.GetType()));

                var rowVersionInfos = GetRowVersionInfos(entitiesByTableNames)
                    .ToDictionary(t => (t.TableName, t.Id));

                foreach (var entitiesByTableName in entitiesByTableNames)
                {
                    foreach (var entitiesById in entitiesByTableName.ToLookup(e => e.Id))
                    {
                        var rowVersion = rowVersionInfos[(entitiesByTableName.Key, entitiesById.Key)].RowVersion;
                        foreach (var entity in entitiesById)
                        {
                            entity.RowVersion = rowVersion;
                            newRowVersions[(entity.GetType(), entity.Id)] = rowVersion;
                        }
                    }
                }
            }
        }

        private void GetLogEntities(UpdateSetVisitor updateSetVisitor, Dictionary<DbEntity, DbEntity> logs)
        {
            updateSetVisitor.DoProcessChildren = false;
            foreach (DbEntity entity in updateSetVisitor.UpdateSet)
            {
                try
                {
                    if (!IsMarkedForDeletion(entity))
                    {
                        // Log this change
                        var log = entity.GetLog();
                        if (log != null)
                        {
                            logs[entity] = log;
                        }
                    }
                }
                catch (SqlException ex)
                {
                    throw new DbSqlException($"Cannot log the update of {entity.GetType().Name} ({ObjectUtils.PropertiesToString(entity)}): {ex.Message}", ex, entity, SqlQueryType.Update);
                }
            }
        }

        /// <summary>
        /// Call this on the UpdateSet
        /// </summary>
        /// <param name="visitor"></param>
        /// <param name="setDefaultValues"></param>
        private void SetDefaultUpdateValues(UpdateSetVisitor visitor, bool setDefaultValues)
        {
            if (setDefaultValues)
            {
                writeDefaultUpdateValues.SetDefaultValues(visitor);
            }
        }

        private static void SetDefaultUpdateValues<TEntity, TColumn>(UpdateSetVisitor visitor,
            string columnNameToUpdate,
            string columnDbDataTypeName,
            Action<TEntity, TColumn> columnSetter,
            Func<TColumn> columnValue)
        {
            foreach (TEntity entity in visitor.UpdateSet.OfType<TEntity>())
            {
                // We have to set the UpdateDate manually, since when the UpdateVisitor traversed all entities, the UpdateDate was not updated yet...
                visitor.SetCurrentEntity(entity as DbEntity);
                visitor.AddUpdatedValue(columnNameToUpdate, columnDbDataTypeName, columnValue());
                columnSetter(entity, columnValue());
            }
        }

        private void DeleteEntities(ISet<DbEntity> toDelete)
        {
            if (toDelete.Count > 0)
            {
                var currentChunk = new List<DbEntity>();
                // Merge all consecutive entities of the same type to make less delete commands to the database
                foreach (var entity in toDelete)
                {
                    if (currentChunk.Count > 0 && currentChunk[0].GetType() != entity.GetType())
                    {
                        DeleteAndClearChunk(currentChunk);
                    }

                    currentChunk.Add(entity);
                }

                DeleteAndClearChunk(currentChunk);
            }
        }

        private void DeleteAndClearChunk(List<DbEntity> currentChunk)
        {
            var type = currentChunk[0].GetType();
            try
            {
                // throw the OnDelete event...
                currentChunk.OfType<IRaiseDbSubmitEvent>().ForEach(e => e.RaiseBeforeDeleteEvent());

                var currentChunkIds = currentChunk.Cast<ILongId>().Select(e => e.Id).ToList();

                Execute("DELETE FROM " + QueryHelpers.GetFullTableName(type) + " WHERE Id IN (SELECT Value FROM @0)",
                    currentChunkIds);

                // Remove the entity from the cache too
                loadedEntityCache.TryRemoveFromCache(type);

                currentChunk.Clear();
            }
            catch (SqlException ex)
            {
                if (currentChunk.Count == 1)
                {
                    throw new DbSqlException($"Cannot delete {type.Name} ({ObjectUtils.PropertiesToString(currentChunk[0])}): {ex}, {ex.Message}", ex, currentChunk[0], SqlQueryType.Delete);
                }
                else if (currentChunk.Count < 1000)
                {
                    throw new DbSqlException($"Cannot delete {currentChunk.Count}x {type.Name} (Ids: { string.Join(", ", currentChunk.Select(c => ((ILongId)c).Id))}): {ex.Message}", ex);
                }
                else
                {
                    throw new DbSqlException($"Cannot delete {currentChunk.Count}x {type.Name}: {ex.Message}", ex);
                }
            }
        }

        private const string UpdateContentAuditSql = @"UPDATE ContentAudit SET MaxContentRowVersion = @MaxContentRowVersion, TriggerAction = @TriggerAction WHERE Id = @Id AND @MaxContentRowVersion > MaxContentRowVersion";
        private const string UpdateContentAuditNoRowVersionSql = @"UPDATE ContentAudit SET TriggerAction = @TriggerAction WHERE Id = @Id";

        private const string InsertContentAuditDeletedSql =
@"
DECLARE @maxContentAuditDeletedRowVersion binary(8)
INSERT INTO ContentAuditDeleted (ContentAuditId, DeletedIds, InsertDate) VALUES (@ContentAuditId, @DeletedIds, @InsertDate)
SELECT @maxContentAuditDeletedRowVersion = MAX(RowVersion) FROM ContentAuditDeleted WHERE ContentAuditId = @ContentAuditId
UPDATE ContentAudit SET TriggerAction = 'Delete', MaxContentAuditDeletedRowVersion = @maxContentAuditDeletedRowVersion WHERE Id = @ContentAuditId AND @maxContentAuditDeletedRowVersion > MaxContentAuditDeletedRowVersion";

        private void UpdateContentAudit(
            IEnumerable<IIsCached> allInserts,
            IEnumerable<IIsCached> allUpdates,
            IEnumerable<IIsCached> allDeletes,
            Dictionary<int, string> serializedEntitiesByContentAuditId)
        {
            IEnumerable<IIsCached> allItems = allInserts
                .Union(allUpdates)
                .Union(allDeletes
                    .Where(d => d is IDeleteDate))
                // Avoid high load on ContentAudit row due to high write rates...we had to take our shop offline because this caused a lot of locks on the db...wait for ERP-13900 to fix this properly...
                // For the moment we will install a SQL agent job that updates the content audit row regularly
                .Where(e => !e.GetType().Name.StartsWith("UserProfile"));
            if (serializedEntitiesByContentAuditId != null && serializedEntitiesByContentAuditId.Any())
            {
                foreach (var toDelete in allDeletes.GroupBy(i => i.ContentAuditId))
                {
                    string serializedEntities;
                    if (serializedEntitiesByContentAuditId.TryGetValue(toDelete.Key, out serializedEntities))
                    {
                        using (var sqlCommand = new DbSqlCommand(InsertContentAuditDeletedSql, currentOpenConnection))
                        {
                            sqlCommand.CommandTimeout = (int)Settings.CommandTimeout.TotalSeconds;
                            sqlCommand.Parameters.AddWithValue("@DeletedIds", serializedEntities);
                            sqlCommand.Parameters.AddWithValue("@ContentAuditId", toDelete.Key);
                            sqlCommand.Parameters.AddWithValue("@InsertDate", DateTime.Now);
                            sqlCommand.ExecuteNonQuery();
                        }
                    }
                }
            }

            foreach (var items in allItems.GroupBy(i => i.ContentAuditId))
            {
                Type type = items.First().GetType();
                byte[] maxContentRowVersion;
                using (var sqlCommand = new DbSqlCommand("", currentOpenConnection))
                {
                    sqlCommand.CommandText = "SELECT MAX(RowVersion) FROM " + QueryHelpers.GetFullTableName(type) + " WHERE Id IN (SELECT Value FROM @0)";
                    QueryHelpers.AddSqlParameters(sqlCommand, new object[] { items.GetIdsAsList() });

                    sqlCommand.CommandTimeout = (int)Settings.CommandTimeout.TotalSeconds;
                    maxContentRowVersion = sqlCommand.SelectSingleValue<byte[]>();
                }

                using (var sqlCommand = new DbSqlCommand())
                {
                    sqlCommand.Connection = currentOpenConnection;
                    if (maxContentRowVersion.Length < 8)
                    {
                        sqlCommand.CommandText = UpdateContentAuditNoRowVersionSql;
                    }
                    else
                    {
                        sqlCommand.CommandText = UpdateContentAuditSql;
                        sqlCommand.Parameters.AddWithValue("@MaxContentRowVersion", maxContentRowVersion);
                    }

                    sqlCommand.Parameters.AddWithValue("@TriggerAction",
                        allDeletes.Any(i => i.ContentAuditId == items.Key) ? "Delete" : (allInserts.Any(i => i.ContentAuditId == items.Key) ? "Insert" : "Update"));
                    sqlCommand.Parameters.AddWithValue("@Id", items.Key);
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        private void InsertOnDeletetionCompleteResults(IReadOnlyList<DbEntity> entitiesToInsert, TimeSpan transactionTimeout)
        {
            if (entitiesToInsert.Count > MinEntitiesOfSameTypeForBulkInsert)
            {
                var entitiesToInsertGroupedByType = entitiesToInsert.GroupBy(e => e.GetType()).ToList();
                InsertEntitiesWithBulkCopy(entitiesToInsertGroupedByType, currentOpenConnection, transactionTimeout);
            }
            else
            {
                InsertEntitiesOneByOne(entitiesToInsert, currentOpenConnection);
            }
        }

        private void InsertEntities(InsertSetVisitor insertSetVisitor,
            SqlConnection sqlConnection,
            TimeSpan transactionTimeout,
            Dictionary<DbEntity, DbEntity> logs)
        {
            var insertedEntities = new HashSet<DbEntity>(new ObjectReferenceEqualityComparer<DbEntity>());

            var entitiesToInsert = insertSetVisitor.GetInsertSet();

            while (true)
            {
                var entitiesToInsertInThisBatch = entitiesToInsert
                    .Where(e => !insertedEntities.Contains(e) && insertSetVisitor.CanBeInserted(e, insertedEntities))
                    .ToList();
                if (entitiesToInsertInThisBatch.Count == 0)
                {
                    break;
                }

                var entitiesToInsertWithBulkCopy = new List<IGrouping<Type, DbEntity>>();
                var entitiesToInsertOneByOne = new List<DbEntity>();
                foreach (var entitiesByType in entitiesToInsertInThisBatch.GroupBy(e => e.GetType()))
                {
                    if (!typeof(IDontInsertWithBulkCopy).IsAssignableFrom(entitiesByType.Key)
                        && entitiesByType.Skip(MinEntitiesOfSameTypeForBulkInsert - 1).Any())
                    {
                        entitiesToInsertWithBulkCopy.Add(entitiesByType);
                    }
                    else
                    {
                        entitiesToInsertOneByOne.AddRange(entitiesByType);
                    }
                }

                InsertEntitiesOneByOne(entitiesToInsertOneByOne, sqlConnection);
                InsertEntitiesWithBulkCopy(entitiesToInsertWithBulkCopy, sqlConnection, transactionTimeout);

                insertedEntities.UnionWith(entitiesToInsertInThisBatch);
            }

            foreach (var insertedEntity in insertedEntities)
            {
                // Log this change...we need to log upon insert because we always log the current state
                var log = insertedEntity.GetLog();
                if (log != null)
                {
                    logs[insertedEntity] = log;
                }

                OnEntitiesLoaded(new[] { insertedEntity });
            }
        }

        private void InsertEntitiesWithBulkCopy(
            IReadOnlyList<IGrouping<Type, DbEntity>> entitiesByType,
            SqlConnection sqlConnection,
            TimeSpan transactionTimeout)
        {
            if (entitiesByType.Count == 0)
            {
                return;
            }

            try
            {
                Stopwatch stopwatch = null;
                if (GlobalDbConfiguration.QueryLogger.DoWriteLog)
                {
                    stopwatch = Stopwatch.StartNew();
                }

                var typesToInsert = entitiesByType.Select(g => g.Key);
                var createDestinationTablesCommand = string.Join(Environment.NewLine, typesToInsert
                    .Select(t => QueryHelpers.GetCreateTempTableCommand(t)));

                new DbSqlCommand(createDestinationTablesCommand, sqlConnection).ExecuteNonQuery();

                foreach (var bulkBatch in entitiesByType)
                {
                    using (var sqlBulkCopy = new SqlBulkCopy(sqlConnection))
                    {
                        using (var reader = new SqlBulkCopyDataReaderWithRowIndex(bulkBatch.Key, bulkBatch, sqlBulkCopy))
                        {
                            sqlBulkCopy.DestinationTableName = "#" + bulkBatch.Key.Name;
                            sqlBulkCopy.WriteToServer(reader);
                        }
                    }
                }

                // We have to write the OUTPUT to @IdAndRowVersions because giving it back directly (without INTO) is not supported for tables with triggers like ItemPrice
                // We need IdForInsertOrder because from around 4000 entities on, the SELECT on @IdAndRowVersions returns the entries not in the insert order (but some other order)

                var insertBatchCommandText = new StringBuilder();
                insertBatchCommandText.AppendLine("DECLARE @IdAndRowVersions TABLE (EntityId INT NOT NULL, TableName VARCHAR(1000) NOT NULL, RowVersion binary(8))");
                foreach (var bulkBatch in entitiesByType)
                {
                    var type = bulkBatch.Key;

                    // Wenn die Ziel-Tabelle eine Identity-Spalte hat garantiert der SQL-Server die Insert-Reihenfolge. s. http://blogs.msdn.com/b/sqltips/archive/2005/07/20/441053.aspx
                    // "INSERT queries that use SELECT with ORDER BY to populate rows guarantees how identity values are computed but not the order in which the rows are inserted"
                    // According to the official documentation "Specifying an ORDER BY clause does not guarantee the rows are inserted in the specified order."
                    // http://msdn.microsoft.com/en-us/library/ms188029.aspx
                    // But you can force the insert order by specifiying a TOP clause according to http://stackoverflow.com/questions/14424929/preserving-order-by-in-select-into
                    // But according to this answer, this is not true for every case: http://stackoverflow.com/questions/11222043/table-valued-function-order-by-is-ignored-in-output/11231935#11231935
                    var queryHelper = QueryHelpers.GetHelper(type);
                    var columnsInInsertStatement = string.Join(", ", queryHelper.ColumnsInInsertStatement);
                    var rowVersionOrNull = typeof(ICheckConcurrentUpdates).IsAssignableFrom(type) ? "INSERTED.RowVersion" : "NULL";

                    insertBatchCommandText.AppendLine(
$@"INSERT INTO {queryHelper.FullTableName} ({columnsInInsertStatement})
OUTPUT INSERTED.Id AS [EntityId], '{queryHelper.FullTableName}' AS [TableName], {rowVersionOrNull} AS [RowVersion] INTO @IdAndRowVersions
SELECT TOP {bulkBatch.Count()} {columnsInInsertStatement} FROM #{type.Name} ORDER BY RowIndexForSqlBulkCopy
DROP TABLE #{type.Name}");
                }

                insertBatchCommandText.AppendLine("SELECT EntityId, TableName, RowVersion FROM @IdAndRowVersions ORDER BY TableName, EntityId");

                var insertBatchCommand = new DbSqlCommand(insertBatchCommandText.ToString(), sqlConnection);
                insertBatchCommand.CommandTimeout = Math.Max((int)transactionTimeout.TotalSeconds, (int)Settings.CommandTimeout.TotalSeconds);

                var entityIdsAndRowVersionsByTableName = new Dictionary<string, List<Tuple<long, byte[]>>>();
                using (var sqlDataReader = insertBatchCommand.ExecuteReader(CommandBehavior.SequentialAccess))
                {
                    string currentTableName = null;
                    List<Tuple<long, byte[]>> entityIds = null;
                    while (sqlDataReader.Read())
                    {
                        var entityId = Convert.ToInt64(sqlDataReader.GetValue(0));
                        var tableName = sqlDataReader.GetString(1);
                        byte[] rowVersion = null;
                        if (!sqlDataReader.IsDBNull(2))
                        {
                            rowVersion = (byte[])sqlDataReader.GetValue(2);
                        }

                        if (currentTableName == null || currentTableName != tableName)
                        {
                            entityIds = new List<Tuple<long, byte[]>>();
                            entityIdsAndRowVersionsByTableName.Add(tableName, entityIds);
                            currentTableName = tableName;
                        }

                        entityIds.Add(Tuple.Create(entityId, rowVersion));
                    }
                }

                if (GlobalDbConfiguration.QueryLogger.DoWriteLog)
                {
                    GlobalDbConfiguration.QueryLogger.WriteLog("Inserting " + string.Join(", ", entitiesByType.Select(e => $"{e.Count()} {e.Key.Name}s")));
                }

                foreach (var bulkBatch in entitiesByType)
                {
                    var fullTableName = QueryHelpers.GetHelper(bulkBatch.Key).FullTableName;
                    var entityIds = entityIdsAndRowVersionsByTableName[fullTableName];
                    int i = 0;
                    var doSetId = typeof(IHasIdentityColumn).IsAssignableFrom(bulkBatch.Key);
                    foreach (var entity in bulkBatch)
                    {
                        // ORDER BY is ignored during in INSERT on tables without IDENTITY column
                        // See http://stackoverflow.com/questions/11222043/table-valued-function-order-by-is-ignored-in-output/11231935#11231935
                        // => Don't set the id for tables without IDENTITY column because the id we read back might be wrong because the entities were not inserted in the order that we specified 
                        // (so we would actually override the entity's id with the id of another entity)
                        if (doSetId)
                        {
                            var idEntity = entity as ISettableId;
                            var longIdEntity = entity as ISettableLongId;
                            var value = entityIds[i].Item1;

                            if (value == 0 && (longIdEntity != null || idEntity != null))
                            {
                                throw new Exception("No Id was set after the insert statement. Did you specify the identity on the table?");
                            }

                            if (idEntity != null)
                            {
                                idEntity.Id = (int)value;
                            }

                            if (longIdEntity != null)
                            {
                                longIdEntity.Id = value;
                            }
                        }

                        if (entity is ICheckConcurrentUpdates)
                        {
                            ((ICheckConcurrentUpdates)entity).RowVersion = entityIds[i].Item2;
                        }

                        i++;
                    }
                }
            }
            catch (SqlException ex)
            {
                throw new DbSqlException($"Cannot insert {string.Join(", ", entitiesByType.Select(e => $"{e.Count()} {e.Key.Name}s"))}: {ex.Message}", ex);
            }
        }

        private void InsertEntitiesOneByOne(IReadOnlyList<DbEntity> entities, SqlConnection sqlConnection)
        {
            for (int i = 0; i < entities.Count; i++)
            {
                InsertEntity(entities[i], sqlConnection);
            }
        }

        private void InsertEntity(DbEntity entity, SqlConnection sqlConnection)
        {
            try
            {
                // Store doWriteLog in a local variable to prevent crash when calling WriteLog in case DoWriteLog changed in the mean time (unlikely but SonarQube complains about this)
                bool doWriteLog = GlobalDbConfiguration.QueryLogger.DoWriteLog;
                Stopwatch stopwatch = null;
                if (doWriteLog)
                {
                    stopwatch = Stopwatch.StartNew();
                }

                var sqlCommand = sqlConnection.CreateCommand();
                sqlCommand.CommandTimeout = (int)Settings.CommandTimeout.TotalSeconds;
                var queryHelper = QueryHelpers.GetHelper(entity.GetType());
                queryHelper.FillInsertCommand(sqlCommand, entity: entity);
                queryHelper.ExecuteInsertCommand(sqlCommand, entity: entity);

                var entityWithId = entity as IId;
                var entityWithLongId = entity as ILongId;

                if ((entityWithId != null && entityWithId.Id == 0) || (entityWithLongId != null && entityWithLongId.Id == 0))
                {
                    throw new Exception("No Id was set after the insert statement. Did you specify the identity on the table?");
                }

                if (doWriteLog)
                {
                    GlobalDbConfiguration.QueryLogger.WriteLog($"Inserting Entity {entity.GetType().Name}", stopwatch.ElapsedMilliseconds);
                }
            }
            catch (SqlException ex)
            {
                throw new DbSqlException($"Cannot insert {entity.GetType().Name} ({ObjectUtils.PropertiesToString(entity)}): {ex.Message}", ex, entity, SqlQueryType.Insert);
            }
        }

        private static void ResetUpdatedValues(IReadOnlyList<DbEntity> toDelete, IEnumerable<DbEntity> toInsert, IReadOnlyList<DbEntity> toUpdate)
        {
            // 2TK BDA
            //// Reset the values to prevent invalid data...
            //toInsert.OfType<ISettableId>().ForEach(i => i.Id = 0);
            //toInsert.OfType<IInsertDate>().ForEach(i => i.InsertDate = default(DateTime));
            //toInsert.OfType<IUpdateDate>().ForEach(i => i.UpdateDate = default(DateTime));
            //toInsert.OfType<IInsertUserId>().ForEach(i => i.InsertUserId = null);
            //toInsert.OfType<IUpdateUserId>().ForEach(i => i.UpdateUserId = null);
            //toInsert.OfType<IInsertUserSessionId>().ForEach(i => i.InsertUserSessionId = null);
            //toInsert.OfType<IUpdateUserSessionId>().ForEach(i => i.UpdateUserSessionId = null);
            //toInsert.OfType<IInsertSystemTaskId>().ForEach(i => i.InsertSystemTaskId = null);
            //toInsert.OfType<IUpdateSystemTaskId>().ForEach(i => i.UpdateSystemTaskId = null);
            //// toInsert.OfType<IUpdateInfo>().ForEach(i => i.StampId = 0);

            //toUpdate.OfType<IUpdateDate>().ForEach(u => u.UpdateDate = default(DateTime));
            //toUpdate.OfType<IUpdateUserId>().ForEach(i => i.UpdateUserId = null);
            //toUpdate.OfType<IUpdateUserSessionId>().ForEach(i => i.UpdateUserSessionId = null);
            //toUpdate.OfType<IUpdateSystemTaskId>().ForEach(i => i.UpdateSystemTaskId = null);

            //toDelete.OfType<IUpdateDate>().ForEach(u => u.UpdateDate = default(DateTime));
            //toDelete.OfType<IUpdateUserId>().ForEach(i => i.UpdateUserId = null);
            //toDelete.OfType<IUpdateUserSessionId>().ForEach(i => i.UpdateUserSessionId = null);
            //toDelete.OfType<IUpdateSystemTaskId>().ForEach(i => i.UpdateSystemTaskId = null);
        }

        private void CheckForConcurrentChanges(
            IReadOnlyList<ICheckConcurrentUpdates> checkConcurrentUpdateEntities,
            Dictionary<(Type, long), byte[]> newRowVersions)
        {
            const string ExceptionEntityFormat = "{0} - {1}\r\n";

            if (checkConcurrentUpdateEntities.Count == 0)
            {
                return;
            }

            var entitiesToCheckByTableNames = checkConcurrentUpdateEntities
                .ToLookup(k1 => QueryHelpers.GetFullTableName(k1.GetType()));
            var rowVersionInfos = GetRowVersionInfos(entitiesToCheckByTableNames)
                .ToDictionary(t => (t.TableName, t.Id));

            // find entities with mismatched row versions
            var mismatchedEntities = new List<TableNameRowVersionAndId>();
            var deletedEntities = new List<TableNameRowVersionAndId>();
            foreach (var entitiesToCheckByTableName in entitiesToCheckByTableNames)
            {
                foreach (var entity in entitiesToCheckByTableName)
                {
                    byte[] entityRowVersion;
                    if (!newRowVersions.TryGetValue((entity.GetType(), entity.Id), out entityRowVersion))
                    {
                        entityRowVersion = entity.RowVersion;
                    }
                    TableNameRowVersionAndId tableNameRowVersionAndId;
                    if (!rowVersionInfos.TryGetValue((entitiesToCheckByTableName.Key, entity.Id), out tableNameRowVersionAndId))
                    {
                        deletedEntities.Add(tableNameRowVersionAndId);
                    }
                    else if (entityRowVersion != tableNameRowVersionAndId.RowVersion)
                    {
                        mismatchedEntities.Add(tableNameRowVersionAndId);
                    }
                }
            }

            var exceptionMessage = new StringBuilder();
            if (mismatchedEntities.Count > 0)
            {
                exceptionMessage.AppendLine("The following entities have been concurrently updated since their last retrieval:");
                mismatchedEntities.ForEach(e => exceptionMessage.AppendFormat(ExceptionEntityFormat, e.TableName, e.Id));
            }

            if (deletedEntities.Count > 0)
            {
                exceptionMessage.AppendLine("The following entities have been concurrently hard-deleted since their last retrieval:");
                deletedEntities.ForEach(e => exceptionMessage.AppendFormat(ExceptionEntityFormat, e.TableName, e.Id));
            }

            if (exceptionMessage.Length > 0)
            {
                throw new DBConcurrencyException(exceptionMessage.ToString());
            }
        }

        private class TableNameRowVersionAndId
        {
            public string TableName { get; set; }

            public int Id { get; set; }

            public byte[] RowVersion { get; set; }
        }

        private IReadOnlyList<TableNameRowVersionAndId> GetRowVersionInfos(ILookup<string, ICheckConcurrentUpdates> tableNamesAndIds)
        {
            // building the rowversion queries by entity type results in large SQL-side performance gains as the number of items increases
            // without introducing significant local performance losses
            const string sqlFormat = "SELECT '{0}' AS TableName, [Id], [RowVersion] FROM {0} WHERE [Id] IN (SELECT Value FROM {1})\r\n";

            var sql = new StringBuilder();
            int i = 0;
            foreach (var tableNameAndIds in tableNamesAndIds)
            {
                if (sql.Length > 0)
                {
                    sql.AppendLine("UNION ALL");
                }

                sql.AppendFormat(sqlFormat, tableNameAndIds.Key, "@" + i.ToString());
                i++;
            }

            object[] parameters = tableNamesAndIds.Select(g => g.GetIdsAsList()).ToArray();

            var results = Load<TableNameRowVersionAndId>(sql.ToString(), parameters);
            return results;
        }

        private int ExecuteOnSubmitCommands(List<(DbSqlCommand, Action<int>)> onSubmitCommands, TimeSpan transactionTimeout)
        {
            if (onSubmitCommands == null)
            {
                return 0;
            }

            int onSubmitCommandCount = onSubmitCommands.Count;
            // First execute the added onSubmit commands
            foreach (var onSubmitCommand in onSubmitCommands)
            {
                using (var sqlCommand = onSubmitCommand.Item1)
                {
                    sqlCommand.Connection = currentOpenConnection;
                    sqlCommand.CommandTimeout = Math.Max((int)transactionTimeout.TotalSeconds, (int)Settings.CommandTimeout.TotalSeconds);

                    int tmp = sqlCommand.ExecuteNonQuery();
                    onSubmitCommand.Item2?.Invoke(tmp);
                }
            }

            onSubmitCommands.Clear();
            return onSubmitCommandCount;
        }

        private void ThrowIfAnyHasValidationError(IEnumerable<DbEntity> entities)
        {
            foreach (var entity in entities)
            {
                if (entity.HasValidationErrors)
                {
                    throw new ValidationException(entity);
                }
            }
        }

        internal UpdateSetVisitor RunAndGetUpdateSetVisitor()
        {
            var updateSetVisitor = new UpdateSetVisitor(doReset: false, columnsToReset: null, doProcessChildren: true);
            updateSetVisitor.Process(loadedSet);
            return updateSetVisitor;
        }

        private InsertSetVisitor GetInsertSetVisitor(bool ignoreLoadedSet = false)
        {
            InsertSetVisitor visitor = new InsertSetVisitor(false);
            GetInserts(visitor, ignoreLoadedSet);
            return visitor;
        }

        private void GetInserts(InsertSetVisitor visitor, bool ignoreLoadedSet)
        {
            // Check the loaded values too...maybe there is a new entity attached to an existing item that was not inserted explicitly
            // Do this before checking the insertOnSubmitSet in order to avoid problems with the insert order...no idea why
            if (!ignoreLoadedSet)
            {
                visitor.Process(loadedSet);
            }

            visitor.Process(insertOnSubmitSet);
        }

        public event Action<int> OnLoadedSetExceedsThreshold;
        protected override void OnEntitiesLoaded(IReadOnlyList<DbEntity> loadedEntities)
        {
            if (Settings.SupportSubmitChanges_Obsolete)
            {
                loadedSet.UnionWith(loadedEntities);
            }

            if (loadedSet.Count >= LoadedSetThreshold)
            {
                OnLoadedSetExceedsThreshold?.Invoke(loadedSet.Count);
            }

            base.OnEntitiesLoaded(loadedEntities);
        }


        public void DeleteOnSubmit(DbEntity entity, DeletionInstant deletionInstant = DeletionInstant.OnEndOfSubmit, bool deleteIDeleteDate = false)
        {
            if (entity != null)
            {
                if (!Settings.SupportSubmitChanges_Obsolete)
                {
                    //throw new NotSupportedException("Settings.SupportSubmitChanges == false!");
                }

                // It's not possible at the moment to delete a DbContentValue and set the DbContent.DefaultValue
                // e.g. https://erp.devinite.com/de/DbContent/Edit/97566
                // But we cannot move deleteOnEndOfSubmitSet.Add(entity); below if (!(entity is IDoNotDeleteAssociatedIDeleteWithRelatedEntities)
                // because other logic does not work anymore with this. E.g. you cannot delete an entry on http://erp.devinite.localhost/advertisementtype
                if (deletionInstant == DeletionInstant.OnEndOfSubmit)
                {
                    deleteOnEndOfSubmitSet.Add(entity);
                }
                else
                {
                    deleteOnBeginOfSubmitSet.Add(entity);
                }

                if (deleteIDeleteDate)
                {
                    if (iDeleteDateEntitiesToHardDelete == null)
                    {
                        iDeleteDateEntitiesToHardDelete = new HashSet<DbEntity>();
                    }
                    iDeleteDateEntitiesToHardDelete.Add(entity);
                }

                ItemMarkedForDeletion?.Invoke(entity);

                ((IDbEntityInternal)entity).MarkForDeletion();
                if (!(entity is IDoNotDeleteAssociatedIDeleteWithRelatedEntities) && DoHardDeleteEntity(entity))
                {
                    // Get all IDeleteWithRelatedEntities and delete them too (after the given entity has been deleted)
                    PropertyInfo[] properties;
                    if (!propertyInfosByType.TryGetValue(entity.GetType(), out properties))
                    {
                        // Cache the properties for performance, if a lot of entities of the same type are deleted
                        properties = ObjectUtils.GetPublicProperties(entity);
                        propertyInfosByType[entity.GetType()] = properties;
                    }

                    foreach (var property in properties)
                    {
                        if (property.PropertyType.GetInterfaces().Contains(typeof(IDeleteWithRelatedEntity)))
                        {
                            var propertyToDelete = property.GetValue(entity) as IDeleteWithRelatedEntity;
                            DeleteOnSubmit(propertyToDelete?.GetEntitiesToDeleteTogether(entity) ?? ImmutableHashSet<DbEntity>.Empty);
                        }
                    }
                }
            }
        }

        public void DeleteOnSubmit<T>(IEnumerable<T> entities, DeletionInstant deletionInstant = DeletionInstant.OnEndOfSubmit, bool deleteIDeleteDate = false) where T : DbEntity
        {
            foreach (T entity in entities)
            {
                DeleteOnSubmit(entity, deletionInstant, deleteIDeleteDate);
            }
        }

        public void ReactivateOnSubmit<TDbEntity>(TDbEntity entity) where TDbEntity : DbEntity, IDeleteDate
        {
            if (entity != null)
            {
                deleteOnBeginOfSubmitSet.Remove(entity);
                deleteOnEndOfSubmitSet.Remove(entity);
                ((IDbEntityInternal)entity).UnmarkForDeletion();

                var entityAsIDeleteDate = entity as IDeleteDate;

                if (entityAsIDeleteDate != null && entityAsIDeleteDate.DeleteDate.HasValue)
                {
                    entityAsIDeleteDate.DeleteDate = null;
                }
            }
        }

        public void ReactivateOnSubmit<T>(IEnumerable<T> entities) where T : DbEntity, IDeleteDate
        {
            foreach (T entity in entities)
            {
                ReactivateOnSubmit(entity);
            }
        }

        public delegate void AfterSubmitChangesEventHandler(IDbWrite db, IReadOnlyList<DbEntity> allInserts, IReadOnlyList<DbEntity> allUpdates, IReadOnlyList<DbEntity> allDeletes);
        public delegate void DeletionCompleteEventHandler(IReadOnlyList<INotifyEntityDelete> allDeleted, AfterDeleteEventArgs afterDeleteEventArgs);
    }
}