using Dg.Deblazer.Api;
using Dg.Deblazer.Comparer;
using Dg.Deblazer.Extensions;
using Dg.Deblazer.Internal;
using Dg.Deblazer.Read;
using Dg.Deblazer.SqlGeneration;
using Dg.Deblazer.SqlUtils;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Linq;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace Dg.Deblazer.Visitors
{
    internal class UpdatedColumnsAndEntities
    {
        public readonly Dictionary<string, object> UpdatedValuesByColumnName = new Dictionary<string, object>();
        public readonly Dictionary<string, string> DbDataTypeByColumnName = new Dictionary<string, string>();
        public readonly HashSet<DbEntity> UpdateSet = new HashSet<DbEntity>(new ObjectReferenceEqualityComparer<DbEntity>());
    }

    internal class UpdateCommandWithAffectedEntities
    {
        public readonly DbSqlCommand DbSqlCommand;
        public readonly IReadOnlyList<DbEntity> UpdatedDbEntityInstances;

        public UpdateCommandWithAffectedEntities(DbSqlCommand dbSqlCommand, IReadOnlyList<DbEntity> updatedDbEntityInstances)
        {
            DbSqlCommand = dbSqlCommand;
            UpdatedDbEntityInstances = updatedDbEntityInstances;
        }
    }

    public class UpdateSetVisitor : DbEntityVisitorBase, IUpdateVisitor
    {
        private Dictionary<(Type, long), UpdatedColumnsAndEntities> updatedValuesByTypeAndId = new Dictionary<(Type, long), UpdatedColumnsAndEntities>();

        private DbEntity currentEntity;

        public bool DoProcessChildren { get; set; }
        protected override bool DoHandleChildren
        {
            get
            {
                return DoProcessChildren;
            }
        }

        internal bool ProcessOnlyLoadedEntities { get; private set; }

        public string[] ColumnsToReset { get; private set; }

        public bool DoReset { get; private set; }

        public UpdateSetVisitor(bool doReset, string[] columnsToReset, bool doProcessChildren)
        {
            DoReset = doReset;
            ColumnsToReset = columnsToReset;
            this.DoProcessChildren = doProcessChildren;
            ProcessOnlyLoadedEntities = true;
        }

        internal void Reprocess(IReadOnlyList<DbEntity> entities, bool processOnlyLoadedEntities)
        {
            ProcessOnlyLoadedEntities = processOnlyLoadedEntities;
            ProcessedSet.Clear();
            Process(entities);
            ProcessOnlyLoadedEntities = true;
        }

        internal void ReprocessSingleEntity<TEntity>(TEntity entity, bool processOnlyLoadedEntities) where TEntity : DbEntity
        {
            ProcessOnlyLoadedEntities = processOnlyLoadedEntities;
            ProcessSingleEntity(entity);
            ProcessOnlyLoadedEntities = true;
        }

        public void AddUpdatedValue(string columnName, string columnDbDataTypeName, object value)
        {
            UpdatedColumnsAndEntities updatedColumnsAndEntities;
            if (!updatedValuesByTypeAndId.TryGetValue((currentEntity.GetType(), ((ILongId)currentEntity).Id), out updatedColumnsAndEntities))
            {
                updatedColumnsAndEntities = new UpdatedColumnsAndEntities();
                updatedValuesByTypeAndId[(currentEntity.GetType(), ((ILongId)currentEntity).Id)] = updatedColumnsAndEntities;
            }

            if (value == null)
            {
                value = DBNull.Value;
            }

            updatedColumnsAndEntities.UpdatedValuesByColumnName[columnName] = value;
            updatedColumnsAndEntities.DbDataTypeByColumnName[columnName] = columnDbDataTypeName;
            updatedColumnsAndEntities.UpdateSet.Add(currentEntity);
        }

        public void SetCurrentEntity(DbEntity entity)
        {
            currentEntity = entity;
        }

        public IReadOnlyList<DbEntity> UpdateSet
        {
            get { return updatedValuesByTypeAndId.SelectMany(e => e.Value.UpdateSet).ToList(); }
        }

        internal List<UpdateCommandWithAffectedEntities> GetCommands(SqlConnection sqlConnection, Dictionary<(Type typeid, long id), Binary> rowVersionsByEntityTypeAndId)
        {
            var sqlCommands = new List<UpdateCommandWithAffectedEntities>();
            foreach (var updatedValuesRecord in updatedValuesByTypeAndId)
            {
                var updatedValuesByColumnName = updatedValuesRecord.Value.UpdatedValuesByColumnName;
                if (updatedValuesByColumnName.Count > 0)
                {
                    // var updatedColumnsAndEntities = updatedValuesRecord.Value;
                    var entityType = updatedValuesRecord.Key.Item1;
                    var entityId = updatedValuesRecord.Key.Item2;
                    var updateSet = updatedValuesRecord.Value.UpdateSet;

                    using (var sqlCommand = new DbSqlCommand())
                    {
                        sqlCommand.Connection = sqlConnection;
                        var fullTableName = QueryHelpers.GetFullTableName(entityType);
                        if (typeof(ICheckConcurrentUpdates).IsAssignableFrom(entityType))
                        {
                            // The following script uses nested IFs to prevent that the statement in the second IF is executed unnecessarily
                            sqlCommand.CommandText =
$@"UPDATE {fullTableName} 
SET {string.Join(", ", updatedValuesByColumnName.Select((v, i) => $"{v.Key} = @p{i}"))} 
WHERE Id = @id AND RowVersion = @rowVersion

IF (@@ROWCOUNT = 0 AND @@ERROR = 0)
  IF (EXISTS (SELECT 1 FROM {fullTableName} WHERE Id = @id))
    BEGIN
      declare @err as nvarchar(110) = CONCAT('Cannot update {fullTableName} (', @id, ') because of a concurrent interfering update');
      THROW 33219281, @err, 1
    END";
                            Binary currentRowVersion = GetRowVersion(rowVersionsByEntityTypeAndId, entityType, entityId, ((ICheckConcurrentUpdates)updateSet.First()));

                            sqlCommand.Parameters.AddWithValue("@rowVersion", currentRowVersion.ToArray());
                        }
                        else
                        {
                            sqlCommand.CommandText = $"UPDATE {fullTableName} SET {string.Join(", ", updatedValuesByColumnName.Select((v, i) => $"{v.Key} = @p{i}"))} WHERE Id = @id";
                        }

                        sqlCommand.Parameters.AddWithValue("@id", entityId);
                        QueryHelpers.AddSqlParameters(sqlCommand, "p", updatedValuesByColumnName.Values.ToArray());
                        sqlCommands.Add(new UpdateCommandWithAffectedEntities(sqlCommand, updateSet.ToList()));
                    }
                }
            }

            return sqlCommands;
        }

        private static Binary GetRowVersion(Dictionary<(Type typeid, long id), Binary> rowVersionsByEntityTypeAndId, Type entityType, long entityId, ICheckConcurrentUpdates dbEntity)
        {
            Binary currentRowVersion;
            if (!rowVersionsByEntityTypeAndId.TryGetValue((entityType, entityId), out currentRowVersion))
            {
                currentRowVersion = dbEntity.RowVersion;
            }

            return currentRowVersion;
        }

        private struct EntityIdAndUpdatedColumns
        {
            public readonly string UpdatedColumns;
            public readonly Type EntityType;

            public EntityIdAndUpdatedColumns(Type entityType, string updatedColumns)
            {
                UpdatedColumns = updatedColumns;
                EntityType = entityType;
            }
        }

        public void UpdateEntitiesUsingSqlBulkCopy(SqlConnection sqlConnection,
            int commandTimeoutSeconds,
            Dictionary<(Type, long), Binary> rowVersionsByEntityTypeAndId)
        {
            var updatedValuesByTypeAndColumnsToUpdate = updatedValuesByTypeAndId.GroupBy(
                v => new EntityIdAndUpdatedColumns(v.Key.Item1, string.Join(",", v.Value.UpdatedValuesByColumnName.Keys.OrderBy(cn => cn))));

            foreach (var updatedValuesRecordsForTypeAndColumnsToUpdate in updatedValuesByTypeAndColumnsToUpdate)
            {
                var entityIdAndUpdatedColumns = updatedValuesRecordsForTypeAndColumnsToUpdate.Key;
                var firstUpdatedValuesRecord = updatedValuesRecordsForTypeAndColumnsToUpdate.First();
                var dbDataTypeByColumnName = firstUpdatedValuesRecord.Value.DbDataTypeByColumnName;
                var orderedColumnNames = firstUpdatedValuesRecord
                    .Value
                    .UpdatedValuesByColumnName
                    .Keys
                    .OrderBy(cn => cn)
                    .ToArray();

                if (!string.IsNullOrEmpty(entityIdAndUpdatedColumns.UpdatedColumns))
                {
                    var entityType = entityIdAndUpdatedColumns.EntityType;
                    //var entityId = entityIdAndUpdatedColumns.EntityId;

                    var addRowVersionStatement = typeof(ICheckConcurrentUpdates).IsAssignableFrom(entityType);
                    if (addRowVersionStatement)
                    {
                        throw new NotImplementedException("UpdateEntitiesUsingSqlBulkCopy does not work yet for entities implementing ICheckConcurrentUpdates! Check Db_UpdateItemProductStaticsConcurrently_SecondUpdateMakesNoChanges to test it");
                    }
                    var tableName = entityType.Name;// QueryHelpers.GetFullTableName(entityType);
                    var sqlCommandText = new StringBuilder();
                    sqlCommandText.Append("CREATE TABLE #");
                    sqlCommandText.Append(tableName);
                    sqlCommandText.Append(" ([Id] INT");
                    foreach (var updatedColumn in orderedColumnNames)
                    {
                        var dbDataType = dbDataTypeByColumnName[updatedColumn];
                        sqlCommandText.Append(", [" + updatedColumn + "] " + dbDataType);
                    }
                    if (addRowVersionStatement)
                    {
                        sqlCommandText.Append(", RowVersion timestamp");
                    }
                    sqlCommandText.Append(")");

                    new DbSqlCommand(sqlCommandText.ToString(), sqlConnection).ExecuteNonQuery();

                    updatedValuesRecordsForTypeAndColumnsToUpdate.ForEach(c => c.Value.UpdatedValuesByColumnName["Id"] = c.Key.Item2);
                    if (addRowVersionStatement)
                    {
                        updatedValuesRecordsForTypeAndColumnsToUpdate.ForEach(
                            c => c.Value.UpdatedValuesByColumnName["RowVersion"] =
                                GetRowVersion(rowVersionsByEntityTypeAndId, entityType, c.Key.Item2, (ICheckConcurrentUpdates)c.Value.UpdateSet.First()));
                    }
                    var valuesToUpdate = updatedValuesRecordsForTypeAndColumnsToUpdate.Select(c => c.Value.UpdatedValuesByColumnName);
                    using (var sqlBulkCopy = new SqlBulkCopy(sqlConnection))
                    {
                        var orderedColumnNamesWithId = orderedColumnNames.ToList();
                        orderedColumnNamesWithId.Insert(0, "Id");
                        if (addRowVersionStatement)
                        {
                            orderedColumnNamesWithId.Add("RowVersion");
                        }
                        using (var reader = new SqlBulkCopyDataReader(entityIdAndUpdatedColumns.EntityType, valuesToUpdate, orderedColumnNamesWithId, sqlBulkCopy))
                        {
                            sqlBulkCopy.DestinationTableName = "#" + tableName;
                            sqlBulkCopy.WriteToServer(reader);
                        }
                    }

                    string mergeSql = "";
                    if (addRowVersionStatement)
                    {
                        mergeSql += "DECLARE @IdAndRowVersions TABLE (EntityId INT NOT NULL, RowVersion binary(8));";
                    }
                    mergeSql += @"
merge into [{0}] as Target
using #{0} as Source
on 
Target.Id = Source.Id";
                    if (addRowVersionStatement)
                    {
                        mergeSql += " AND Target.RowVersion = Source.RowVersion";
                    }
                    mergeSql +=
@"
when matched then
update set {1}";
                    if (addRowVersionStatement)
                    {
                        mergeSql += @"
OUTPUT INSERTED.Id, INSERTED.RowVersion INTO @IdAndRowVersions;
";
                        mergeSql += @"
IF @@ERROR = 0 AND @@ROWCOUNT <> (SELECT COUNT(*) FROM #{0} JOIN {0} ON #{0}.Id = {0}.Id)
BEGIN    
    DECLARE @updateEntityCount INT = (SELECT COUNT(*) FROM @IdAndRowVersions)
    DECLARE @totalEntityCount INT = (SELECT COUNT(*) FROM #{0} JOIN {0} ON #{0}.Id = {0}.Id)
    DECLARE @errorMessage NVARCHAR(MAX) = 'Only updated ' + CAST(@updateEntityCount AS VARCHAR(50)) + ' of ' + CAST(@totalEntityCount AS VARCHAR(50)) + ' {0}s (Ids {2}) because of a concurrent interfering update'; 
    THROW 33219281, @errorMessage, 1
END;";
                    }
                    else
                    {
                        mergeSql += ";";
                    }
                    mergeSql +=
@"
DROP TABLE #{0};
";
                    if (addRowVersionStatement)
                    {
                        mergeSql += "SELECT EntityId, RowVersion FROM @IdAndRowVersions;";
                    }

                    mergeSql = string.Format(mergeSql,
                        tableName,
                        string.Join(", ", orderedColumnNames.Select(cn => $"Target.[{cn}] = Source.[{cn}]"),
                        string.Join(", ", updatedValuesRecordsForTypeAndColumnsToUpdate.Select(c => c.Key.Item2))));

                    var mergeSqlCommand = new DbSqlCommand(mergeSql, sqlConnection);
                    mergeSqlCommand.CommandTimeout = commandTimeoutSeconds;
                    if (addRowVersionStatement)
                    {
                        using (var sqlDataReader = mergeSqlCommand.ExecuteReader(CommandBehavior.SequentialAccess))
                        {
                            while (sqlDataReader.Read())
                            {
                                var entityId = Convert.ToInt32(sqlDataReader.GetValue(0));
                                Binary rowVersion = new Binary((byte[])sqlDataReader.GetValue(1));

                                rowVersionsByEntityTypeAndId[(entityType, entityId)] = rowVersion;
                            }
                        }
                    }
                    else
                    {
                        mergeSqlCommand.ExecuteNonQuery();
                    }
                }
            }
        }

        internal override void ProcessSingleEntity(IDbEntityInternal entity) => entity.ModifyInternalState(this);
    }
}