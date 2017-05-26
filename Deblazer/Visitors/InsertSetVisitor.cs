using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Dg.Deblazer.Cache;
using Dg.Deblazer.Internal;
using Dg.Deblazer.Comparer;

namespace Dg.Deblazer.Visitors
{
    internal class InsertSetVisitor : DbEntityVisitorBase
    {
        private readonly Dictionary<DbEntity, HashSet<DbEntity>> insertDependencies = new Dictionary<DbEntity, HashSet<DbEntity>>(new ObjectReferenceEqualityComparer<DbEntity>());

        private readonly HashSet<DbEntity> insertSet = new HashSet<DbEntity>(new ObjectReferenceEqualityComparer<DbEntity>());

        public bool DoReset { get; private set; }

        internal InsertSetVisitor(bool doReset)
        {
            DoReset = doReset;
        }

        internal void InsertEntity(DbEntity entity)
        {
            insertSet.Add(entity);
        }

        internal void Reprocess(IReadOnlyList<DbEntity> entities)
        {
            ProcessedSet.Clear();
            Process(entities);
        }

        internal void AddRelation(DbEntity firstEntity, DbEntity secondEntity, bool secondEntityIsPrimary)
        {
            if (insertSet.Contains(firstEntity)
                && insertSet.Contains(secondEntity))
            {
                // If both entities are to be inserted we store their relation to another (to determine which one has to be inserted first)
                DbEntity e1 = secondEntityIsPrimary ? firstEntity : secondEntity;
                DbEntity e2 = !secondEntityIsPrimary ? firstEntity : secondEntity;
                HashSet<DbEntity> dependenciesForE1;
                if (!insertDependencies.TryGetValue(e1, out dependenciesForE1))
                {
                    dependenciesForE1 = new HashSet<DbEntity>(new ObjectReferenceEqualityComparer<DbEntity>());
                    insertDependencies[e1] = dependenciesForE1;
                }

                HashSet<DbEntity> dependenciesInOtherDirection;
                if (insertDependencies.TryGetValue(e2, out dependenciesInOtherDirection)
                    && dependenciesInOtherDirection.Contains(e1))
                {
                    throw new InvalidOperationException("Entities " + firstEntity.GetType().Name + " and "
                        + secondEntity.GetType().Name + " each depend on the other entity to be inserted first. "
                        + "One case where developers run into this is when they try to add the MainItem of an ItemSet "
                        + " as Item into the set along with the child Items. This is wrong and should not be done.");
                }

                dependenciesForE1.Add(e2);
            }

        }

        internal bool CanBeInserted(DbEntity entity, HashSet<DbEntity> alreadyInsertedEntities)
        {
            HashSet<DbEntity> entitiesWhichMustBeInsertedFirst;
            if (insertDependencies.TryGetValue(entity, out entitiesWhichMustBeInsertedFirst))
            {
                return entitiesWhichMustBeInsertedFirst.All(e => alreadyInsertedEntities.Contains(e));
            }

            return true;
        }

        internal IEnumerable<DbEntity> GetInsertSet()
        {
            return insertSet;
        }

        private readonly static IDictionary<Type, Action<DbEntity>> dbValuesResetters = new ConcurrentDictionary<Type, Action<DbEntity>>();

        internal void ResetDbValues(DbEntity dbEntity)
        {
            var dbEntityType = dbEntity.GetType();
            Action<DbEntity> dbValuesResetter;
            if (!dbValuesResetters.TryGetValue(dbEntityType, out dbValuesResetter))
            {
                dbValuesResetter = CreateDbValuesResetter(dbEntityType);
                dbValuesResetters[dbEntityType] = dbValuesResetter;
            }
            dbValuesResetter(dbEntity);
        }

        internal override void ProcessAssociation(DbEntity parentEntity, IDbEntitySetInternal dbEntitySet)
        {
            if (dbEntitySet != null && dbEntitySet.EntitiesInternal != null)
            {
                foreach (var entity in dbEntitySet.EntitiesInternal)
                {
                    AddRelation(parentEntity, entity, dbEntitySet.IsForeignKey);
                }
            }

            base.ProcessAssociation(parentEntity, dbEntitySet);
        }

        internal override void ProcessAssociation(DbEntity parentEntity, IDbEntityRefInternal dbEntityRef)
        {
            if (dbEntityRef != null && dbEntityRef.EntityInternal != null && !(dbEntityRef is DbEntityRefCached))
            {
                var entity = dbEntityRef.EntityInternal;

                AddRelation(parentEntity, entity, dbEntityRef.IsForeignKey);
            }

            base.ProcessAssociation(parentEntity, dbEntityRef);
        }

        private static Action<DbEntity> CreateDbValuesResetter(Type dbEntityType)
        {
            var dbEntityExpression = Expression.Parameter(typeof(DbEntity));

            var trueExpression = Expression.Constant(true);
            var falseExpression = Expression.Constant(false);

            var dbValueFieldInfos = GetAllFields(dbEntityType, BindingFlags.NonPublic | BindingFlags.Instance)
                .Where(fi => fi.FieldType.IsGenericType && fi.FieldType.GetGenericTypeDefinition() == DbEntity.DbValueGenericType)
                .ToList();
            var body = new List<Expression>();
            var convertExpression = Expression.Convert(dbEntityExpression, dbEntityType);
            body.Add(convertExpression);
            for (int i = 0; i < dbValueFieldInfos.Count; i++)
            {
                var dbValueFieldExpression = Expression.Field(convertExpression, dbValueFieldInfos[i]);

                var hasLoadedValueExpression = Expression.Field(dbValueFieldExpression, "hasLoadedValue");
                var hasLoadedValueAssignment = Expression.Assign(hasLoadedValueExpression, trueExpression);
                body.Add(hasLoadedValueAssignment);

                var hasAssignedValueExpression = Expression.Field(dbValueFieldExpression, "hasAssignedValue");
                var hasAssignedValueAssignment = Expression.Assign(hasAssignedValueExpression, falseExpression);
                body.Add(hasAssignedValueAssignment);
            }

            var expr = Expression.Lambda<Action<DbEntity>>(Expression.Block(body.ToArray()), dbEntityExpression);

            return expr.Compile();
        }

        /// <summary>
        /// Gets all FieldInfos including fields from the base types
        /// </summary>
        private static IReadOnlyList<FieldInfo> GetAllFields(Type type, BindingFlags bindingFlags)
        {
            var fields = new List<FieldInfo>();
            var currentType = type;
            while (currentType != null)
            {
                fields.AddRange(currentType.GetFields(bindingFlags));
                currentType = currentType.BaseType;
            }
            return fields;
        }

        internal override void ProcessSingleEntity(IDbEntityInternal entity) => entity.ModifyInternalState(this);
    }
}