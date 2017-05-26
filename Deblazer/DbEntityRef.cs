using Dg.Deblazer.Api;
using Dg.Deblazer.Cache;
using Dg.Deblazer.Configuration;
using Dg.Deblazer.Internal;
using Dg.Deblazer.SqlGeneration;
using Dg.Deblazer.Visitors;
using Dg.Deblazer.Write;
using JetBrains.Annotations;
using System;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.Serialization;

namespace Dg.Deblazer
{
    [Serializable]
    public abstract class DbEntityRef : ISerializable
    {
        internal IDbInternal db;
        [NotNull]
        [ItemNotNull]
        protected Func<long?>[] entityIds;
        protected bool getChildrenFromCache;
        protected string[] idColumnNames;
        protected bool lazyLoadValue;
        protected bool triedToLoadValue;

        internal DbEntityRef()
        {
        }

        public DbEntityRef(IDb db, bool isForeignKey, bool lazyLoadValue, bool getChildrenFromCache)
        {
            this.lazyLoadValue = lazyLoadValue;
            this.getChildrenFromCache = getChildrenFromCache;
            HasAssignedValue = false;
            triedToLoadValue = false;
            Init(db, isForeignKey, idColumnNames);
        }

        protected DbEntityRef(SerializationInfo info, StreamingContext context)
        {
            lazyLoadValue = info.GetBoolean("llv");
            getChildrenFromCache = info.GetBoolean("gcfrc");
            HasAssignedValue = info.GetBoolean("hav");
            triedToLoadValue = info.GetBoolean("ttlv");
        }

        protected bool isForeignKey;

        public bool HasLoadedValue
        {
            get { return hasEntity && HasAssignedValue == false; }
        }

        public bool HasAssignedValue { get; protected set; }

        protected abstract bool hasEntity { get; }

        public virtual void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("llv", lazyLoadValue);
            info.AddValue("gcfrc", getChildrenFromCache);
            info.AddValue("hav", HasAssignedValue);
            info.AddValue("ttlv", triedToLoadValue);
        }

        protected void Init(IDb db, bool isForeignKey, string[] idColumnNames)
        {
            this.isForeignKey = isForeignKey;
            this.idColumnNames = idColumnNames;
            this.db = (IDbInternal)db;
        }
    }

    [Serializable]
    public class DbEntityRef<TMember> : DbEntityRef, IDbEntityRef<TMember>, IDbEntityRefInternal, ISerializable where TMember : DbEntity
    {
        private TMember entity;

        public DbEntityRef(IDb db, bool isForeignKey, string[] idColumnNames, Func<long?>[] entityIds, bool lazyLoadValue, bool getChildrenFromCache)
            : base(db, isForeignKey, lazyLoadValue, getChildrenFromCache)
        {
            Init(db, entityIds, isForeignKey, idColumnNames);
        }

        protected DbEntityRef(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            entity = info.GetValue("e", typeof(TMember)) as TMember;
        }

        protected override bool hasEntity
        {
            get { return entity != null; }
        }

        [CanBeNull]
        internal TMember Entity
        {
            get
            {
                if (entity == null // Check this because Load() does not set the _hasAssignedValue nor the _triedToLoadValue flag
                    && HasAssignedValue == false
                    && triedToLoadValue == false
                    && entityIds.All(i => i() > 0))
                {
                    // Try to load the entity from the db
                    entity = GetQuery().SingleOrDefault();
                    triedToLoadValue = true;
                    HasAssignedValue = false;
                }

                return entity;
            }

            set { SetEntity(value); }
        }

        public void ResetValue()
        {
            entity = null;
            HasAssignedValue = false;
            triedToLoadValue = false;
        }

        public void Load(TMember newEntity)
        {
            entity = newEntity;
        }

        public TMember GetEntity(Action<TMember> beforeRightsCheckAction)
        {
            if (entity == null // Check this because Load() does not set the _hasAssignedValue nor the _triedToLoadValue flag
                && !HasAssignedValue
                && !triedToLoadValue
                && entityIds.All(i => i() > 0))
            {
                if (!lazyLoadValue)
                {
                    throw new InvalidOperationException(
                        $"Cannot load {typeof(TMember).Name} ({string.Join(",", idColumnNames)} {string.Join(",", entityIds.Select(i => i().ToString() ))}) because lazy loading of value is disabled");
                }

                string columnKey = string.Join("|", idColumnNames);
                if (db == null
                    || !TryGetFromCache(columnKey, out entity)) // First try to get the item from the db's cache
                {
                    var query = GetQuery();
                    query.ValueLoadedBeforeRightsCheck += (DbEntity value) => beforeRightsCheckAction(value as TMember);
                    // Try to load the entity from the db
                    entity = query.SingleOrDefault();
                    entity = GetFromOrAddToCache(columnKey, entity);
                    if (entity != null && getChildrenFromCache)
                    {
                        new GetChildrenFromCacheVisitor().Process(entity);
                    }
                }

                triedToLoadValue = true;
                HasAssignedValue = false;
            }

            return entity;
        }

        public void SetEntity(TMember value)
        {
            if (db != null
                && value?.DbInternal != null
                && (db is IDbWrite && value.DbInternal is IDbWrite)
                && !ReferenceEquals(db, value.DbInternal))
            {
                // 2TK BDA catch this event somewhere
                db.RaiseNotifyMixedDb(new MixedDbEventArgs(value));
            }

            entity = value;
            HasAssignedValue = true;
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("e", entity);
            base.GetObjectData(info, context);
        }

        internal void Init(IDb db, Func<long?>[] entityIds, bool isForeignKey, string[] idColumnNames)
        {
            Init(db, isForeignKey, idColumnNames);
            this.entityIds = entityIds;
        }

        private bool TryGetFromCache(string memberName, out TMember value)
        {
            if (entityIds.Length == 1)
            {
                return db.LoadedEntityCache.TryGet(memberName, entityIds[0]().Value, out value);
            }

            string idKey = string.Join(",", entityIds.Select(i => i().ToString()));
            return db.LoadedEntityCache.TryGet(memberName, idKey, out value);
        }

        private TMember GetFromOrAddToCache(string memberName, TMember value)
        {
            if (entityIds.Length == 1)
            {
                return db.LoadedEntityCache.GetOrAdd(memberName, entityIds[0]().Value, value);
            }

            string idKey = string.Join(",", entityIds.Select(i => i().ToString()));
            return db.LoadedEntityCache.GetOrAdd(memberName, idKey, value);
        }

        protected virtual QuerySql<TMember, TMember, TMember> GetQuery()
        {
            if (db == null)
            {
                InitDb();
            }

            var intIds = entityIds
                .Select(f => f())
                .Where(x => x.HasValue)
                .Select(i => (int)i.Value)
                .ToList();

            return db.LoadBy<TMember>(idColumnNames, intIds);
        }

        private void InitDb()
        {
            var dbConfiguration = GlobalDbConfiguration.GetConfigurationOrEmpty(typeof(TMember));

            if (dbConfiguration.GetDbForEvilLazyLoad != null)
            {
                db = dbConfiguration.GetDbForEvilLazyLoad();
            }
            else
            {
                throw new LazyLoadUnattachedEntityException("Lazy loading on a newly created entity which was not loaded from database is not allowed!");
            }
        }

        public override string ToString()
        {
            return entity != null ? entity.ToString() : (triedToLoadValue || HasAssignedValue ? "null" : "not yet loaded");
        }

        DbEntity IDbEntityRefInternal.EntityInternal => entity;

        bool IDbEntityRefInternal.IsForeignKey => isForeignKey;

        void IDbEntityRefInternal.SetDb(BaseDb db) => this.db = db;

        void IDbEntityRefInternal.DisableLazyLoadChildren() => lazyLoadValue = false;

        void IDbEntityRefInternal.EnableLoadingChildrenFromCache() => getChildrenFromCache = true;

        void IDbEntityRefInternal.MakeReadOnly()
        {
            if (triedToLoadValue || HasAssignedValue || entity != null)
            {
                entityIds = null;
                idColumnNames = null;
                db = null;
            }
        }

        bool IRaiseDbSubmitEvent.RaiseBeforeInsertEvent() => false;

        IImmutableSet<DbEntity> IRaiseDbSubmitEvent.RaiseAfterInsertEvent() => ImmutableHashSet<DbEntity>.Empty;

        bool IRaiseDbSubmitEvent.RaiseBeforeDeleteEvent() => false;

        bool IRaiseDbSubmitEvent.RaiseBeforeUpdateEvent() => false;

        bool IRaiseDbSubmitEvent.RaiseAfterDeleteEvent() => false;

        bool IRaiseDbSubmitEvent.RaiseOnSubmitTransactionAbortedEvent() => false;
    }
}