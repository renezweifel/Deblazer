using System;
using System.Collections.Immutable;
using Dg.Deblazer.Api;
using Dg.Deblazer.Internal;

namespace Dg.Deblazer.Cache
{
    public abstract class DbEntityRefCached : IDbEntityRefInternal
    {
        public abstract void Load(DbEntity newEntity);

        DbEntity IDbEntityRefInternal.EntityInternal { get { return EntityInternal; } }
        protected abstract DbEntity EntityInternal { get; }

        bool IDbEntityRefInternal.IsForeignKey
        {
            get { throw new NotSupportedException(); }
        }

        void IDbEntityRefInternal.SetDb(BaseDb db) { }

        void IDbEntityRefInternal.DisableLazyLoadChildren() { }

        void IDbEntityRefInternal.EnableLoadingChildrenFromCache() { }

        void IDbEntityRefInternal.MakeReadOnly() { }

        bool IRaiseDbSubmitEvent.RaiseBeforeInsertEvent()
        {
            return false;
        }

        IImmutableSet<DbEntity> IRaiseDbSubmitEvent.RaiseAfterInsertEvent()
        {
            return ImmutableHashSet<DbEntity>.Empty;
        }

        bool IRaiseDbSubmitEvent.RaiseBeforeDeleteEvent()
        {
            return false;
        }

        bool IRaiseDbSubmitEvent.RaiseBeforeUpdateEvent()
        {
            return false;
        }

        bool IRaiseDbSubmitEvent.RaiseAfterDeleteEvent()
        {
            return false;
        }

        bool IRaiseDbSubmitEvent.RaiseOnSubmitTransactionAbortedEvent()
        {
            return false;
        }
    }

    public class DbEntityRefCached<TMember> : DbEntityRefCached, IDbEntityRef<TMember>, IDbEntityRefInternal where TMember : DbEntity
    {
        private TMember entity;

        public DbEntityRefCached(TMember entity)
        {
            this.entity = entity;
        }

        public bool HasLoadedValue
        {
            get { throw new NotSupportedException(); }
        }

        public bool HasAssignedValue
        {
            get { throw new NotSupportedException(); }
        }

        public override void Load(DbEntity newEntity)
        {
            if (!(newEntity is TMember))
            {
                throw new ArgumentException("newEntity must be of type " + typeof(TMember).Name);
            }

            Load((TMember)newEntity);
        }

        void IDbEntityRef<TMember>.Load(TMember newEntity)
        {
            Load(newEntity);
        }

        private void Load(TMember newEntity)
        {
            entity = newEntity;
        }

        public void ResetValue()
        {
            throw new NotSupportedException();
        }

        public TMember GetEntity(Action<TMember> beforeRightsCheckAction)
        {
            return entity;
        }

        public void SetEntity(TMember value)
        {
            throw new NotSupportedException();
        }

        protected override DbEntity EntityInternal
        {
            get
            {
                return entity;
            }
        }
    }
}