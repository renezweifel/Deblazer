using System.Collections.Generic;
using Dg.Deblazer.Api;
using Dg.Deblazer.Cache;
using Dg.Deblazer.Internal;
using Dg.Deblazer.Comparer;

namespace Dg.Deblazer.Visitors
{
    public abstract class DbEntityVisitorBase
    {
        private readonly Queue<DbEntity> toProcessQueue = new Queue<DbEntity>();
        protected readonly HashSet<DbEntity> ProcessedSet = new HashSet<DbEntity>(new ObjectReferenceEqualityComparer<DbEntity>());

        protected virtual bool DoHandleChildren
        {
            get
            {
                return true;
            }
        }

        public void Process(DbEntity entity)
        {
            AddToProcessingQueue(entity);
            ProcessQueue();
        }

        public void Process(IEnumerable<DbEntity> entities)
        {
            foreach (var entity in entities)
            {
                AddToProcessingQueue(entity);
            }
            ProcessQueue();
        }

        private void ProcessQueue()
        {
            while (toProcessQueue.Count > 0)
            {
                var entity = toProcessQueue.Dequeue();

                if (!ProcessedSet.Contains(entity))
                {
                    ProcessSingleEntity(entity);
                    ProcessedSet.Add(entity);

                    if (DoHandleChildren)
                    {
                        // Implementation of HandleChildren is generated in the artifacts
                        ((IHandleChildren)entity).HandleChildren(this);
                    }
                }
            }
        }

        internal abstract void ProcessSingleEntity(IDbEntityInternal entity);

        internal virtual void ProcessSingleEntity(IDbEntityRefInternal entity) { }

        internal virtual void ProcessSingleEntity(IDbEntitySetInternal entity) { }

        protected void AddToProcessingQueue(DbEntity entity)
        {
            if (entity != null
                && !ProcessedSet.Contains(entity))
            {
                toProcessQueue.Enqueue(entity);
            }
        }

        public void ProcessAssociation(DbEntity parentEntity, IDbEntityRef dbEntityRef) => ProcessAssociation(parentEntity, (IDbEntityRefInternal)dbEntityRef);
        internal virtual void ProcessAssociation(DbEntity parentEntity, IDbEntityRefInternal dbEntityRef)
        {
            if (dbEntityRef != null && !(dbEntityRef is DbEntityRefCached))
            {
                ProcessSingleEntity(dbEntityRef);

                var childEntity = dbEntityRef.EntityInternal;
                AddToProcessingQueue(childEntity);
            }
        }

        public void ProcessAssociation(DbEntity parentEntity, IDbEntitySet dbEntitySet) => ProcessAssociation(parentEntity, (IDbEntitySetInternal)dbEntitySet);
        internal virtual void ProcessAssociation(DbEntity parentEntity, IDbEntitySetInternal dbEntitySet)
        {
            if (dbEntitySet != null)
            {
                ProcessSingleEntity(dbEntitySet);
                if (dbEntitySet.EntitiesInternal != null)
                {
                    foreach (var entity in dbEntitySet.EntitiesInternal)
                    {
                        AddToProcessingQueue(entity);
                    }
                }
            }
        }
    }
}