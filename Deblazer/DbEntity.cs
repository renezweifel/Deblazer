using Dg.Deblazer.Api;
using Dg.Deblazer.Cache;
using Dg.Deblazer.Configuration;
using Dg.Deblazer.ContextValues.DgSpecific;
using Dg.Deblazer.Internal;
using Dg.Deblazer.SqlGeneration;
using Dg.Deblazer.Utils;
using Dg.Deblazer.Validation;
using Dg.Deblazer.Visitors;
using Dg.Deblazer.Write;
using JetBrains.Annotations;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;

namespace Dg.Deblazer
{
    public abstract partial class DbEntity : ValidationBase, IComparable, IDbEntityInternal, IRaiseDbSubmitEvent, IHandleChildren, IQueryReturnType
    {
        /// <summary>
        /// Use japanese letters because it seems that an underscore as prefix is not enough to prevent developers from accessing this
        /// </summary>
        protected IDb _db;

        internal IDbInternal DbInternal => (IDbInternal)_db;

        protected bool isLoaded = false;

        protected internal bool _lazyLoadChildren = true;
        protected bool _getChildrenFromCache = false;
        private bool _allowSettingColumns = true;

        public event EventHandler IdChanging;

        protected virtual void SendIdChanging()
        {
            IdChanging?.Invoke(this, EventArgs.Empty);
        }

        public event EventHandler IdChanged;

        protected virtual void SendIdChanged()
        {
            IdChanged?.Invoke(this, EventArgs.Empty);
        }

        void IDbEntityInternal.MarkForDeletion() => IsMarkedForDeletion = true;

        void IDbEntityInternal.UnmarkForDeletion() => IsMarkedForDeletion = false;

        void IDbEntityInternal.SetDb(IDbInternal db) => _db = db;

        void IDbEntityInternal.DisableLazyLoadChildren() => _lazyLoadChildren = false;

        void IDbEntityInternal.SetAllowSettingColumns(bool allowSettingColumns) => _allowSettingColumns = allowSettingColumns;

        void IDbEntityInternal.EnableLoadingChildrenFromCache() => _getChildrenFromCache = true;

        void IDbEntityInternal.MakeReadOnly()
        {
            IdChanged = null;
            BeforeInsert = null;
            BeforeUpdate = null;
            BeforeDelete = null;
            _db = null;
        }

        void IDbEntityInternal.ModifyInternalState(UpdateSetVisitor visitor)
        {
            if ((!IsMarkedForDeletion || this is IDeleteDate)
                && (isLoaded || !visitor.ProcessOnlyLoadedEntities))
            {
                visitor.SetCurrentEntity(this);
                CheckProperties(visitor);
            }
        }

        void IDbEntityInternal.ModifyInternalState(InsertSetVisitor visitor)
        {
            if (visitor.DoReset)
            {
                isLoaded = true;
                visitor.ResetDbValues(this);
            }
            else
            {
                if (!IsMarkedForDeletion && (((ILongId)this).Id == 0 || !isLoaded))
                {
                    visitor.InsertEntity(this);
                }
            }
        }

        void IDbEntityInternal.ModifyInternalState(FillVisitor visitor) => ModifyInternalState(visitor);

        protected abstract void ModifyInternalState(FillVisitor visitor);

        [CanBeNull]
        protected internal virtual DbEntity GetLog()
        {
            return null;
        }

        protected abstract void CheckProperties(IUpdateVisitor visitor);

        bool IRaiseDbSubmitEvent.RaiseBeforeInsertEvent()
        {
            if (BeforeInsert != null)
            {
                BeforeInsert(this, EventArgs.Empty);
                return true;
            }
            return false;
        }

        IImmutableSet<DbEntity> IRaiseDbSubmitEvent.RaiseAfterInsertEvent()
        {
            if (AfterInsert != null)
            {
                var eventArgs = new AfterInsertEventArgs();
                AfterInsert(this, eventArgs);
                return eventArgs.GetEntitiesToUpdate();
            }

            return ImmutableHashSet<DbEntity>.Empty;
        }

        bool IRaiseDbSubmitEvent.RaiseOnSubmitTransactionAbortedEvent()
        {
            if (OnSubmitTransactionAborted != null)
            {
                OnSubmitTransactionAborted(this, EventArgs.Empty);
                return true;
            }
            return false;
        }

        bool IRaiseDbSubmitEvent.RaiseAfterDeleteEvent()
        {
            if (AfterDelete != null)
            {
                AfterDelete(this, EventArgs.Empty);
                return true;
            }
            return false;
        }

        public bool IsMarkedForDeletion { get; private set; }

        public sealed override bool DoValidateForDelete()
        {
            return IsMarkedForDeletion;
        }

        protected event EventHandler BeforeInsert;

        public event EventHandler<AfterInsertEventArgs> AfterInsert;

        public event EventHandler AfterDelete;

        public event EventHandler BeforeUpdate;

        public event EventHandler OnSubmitTransactionAborted;

        bool IRaiseDbSubmitEvent.RaiseBeforeUpdateEvent()
        {
            if (BeforeUpdate != null)
            {
                BeforeUpdate(this, EventArgs.Empty);
                return true;
            }
            return false;
        }

        public event EventHandler BeforeDelete;

        bool IRaiseDbSubmitEvent.RaiseBeforeDeleteEvent()
        {
            if (BeforeDelete != null)
            {
                BeforeDelete(this, EventArgs.Empty);
                return true;
            }

            return false;
        }

        [CanBeNull]
        protected IDbEntityRef<TMember> GetDbEntityRef<TMember>(
            bool isForeignKey,
            string[] idColumnNames,
            Func<long?>[] entityIds,
            Action<TMember> beforeRightsCheckAction) where TMember : DbEntity
        {
            IDbEntityRef<TMember> dbEntityRef = null;

            if (_getChildrenFromCache && entityIds.Length == 1)
            {
                var entityId = entityIds[0]();
                if (entityId.HasValue)
                {
                    if (idColumnNames.Length != 1
                        || idColumnNames[0] != "[Id]")
                    {
                        throw new InvalidOperationException("Getting the reference by foreign key is broken. If you get here, this is probably a follow up problem because of an error that happened earlier.");
                    }

                    if (typeof(IId).IsAssignableFrom(typeof(TMember)) && entityId.Value <= int.MaxValue)
                    {
                        dbEntityRef = GlobalDbConfiguration.GetConfigurationOrEmpty(GetType()).CacheService.GetDbEntityRefCached<TMember>((int)entityId.Value);
                    }
                }
                else
                {
                    return null;
                }
            }

            if (dbEntityRef == null)
            {
                dbEntityRef = new DbEntityRef<TMember>(DbInternal, isForeignKey, idColumnNames, entityIds, _lazyLoadChildren, _getChildrenFromCache);
            }

            return dbEntityRef;
        }

#if DEBUG

        /// <summary>
        /// When enabled breaks in to the Debugger when trying to Attach an Entity-Set after there is already some Data loaded. This can easily happen accidentally when
        /// Attaching/Joining in complex queries. For Example:
        /// <code>
        /// var order = db.SingleDb&lt;Order&gt;(orderId)
        /// var items = db.Items()
        ///                 .WhereDb(i => i.OrderId == orderId)
        ///                 .JoinItemAccounting(Left).Back()
        ///                 .JoinItemProduct(Left).Back()
        ///                 .ToList();
        ///
        /// var accidentallyAccessingItems = order.Items;
        ///
        /// order.Items.Attach(items); // Items will NOT be attached, the accidentally lazy loaded items remain. When accessing ItemAccounting or ItemProduct, there will be Lazy-Loads!
        ///                            // If WarnDeveloperIfEntitiesAlreadyConnected is set to TRUE we break here and the Developer can fix the Problem more easily.
        /// </code>
        /// Related: The Attach-Behavior will change in https://jira.devinite.com/browse/ERP-11495 but this Issue still remains.
        /// </summary>
        public static bool WarnDeveloperIfEntitiesAlreadyConnected = false;
#endif

        /// <summary>
        /// DbEntity Assignment
        /// </summary>
        /// <typeparam name="TEntityToSet">Type of entity to assign ("Item" if i set ItemProduct.Item)</typeparam>
        /// <typeparam name="TThisEntity">Type of this entity entity ("ItemProduct" if i set ItemProduct.Item)</typeparam>
        /// <param name="newValue">This entity gets assigned to the object (righthand side of the =)</param>
        /// <param name="newValueId">Id or Null of newValue</param>
        /// <param name="reference">This newValue gets assigned to this object (lefthand side of the =)</param>
        /// <param name="referenceIdSetter"></param>
        /// <param name="otherSideSet"></param>
        /// <param name="setOtherSideEntity">Method to be applied on the new value</param>
        /// <param name="idChangedAction">Method to be applied if id Changed</param>
        /// <param name="referenceIdValue"></param>
        protected void AssignDbEntity<TEntityToSet, TThisEntity>(
            TEntityToSet newValue,
            long?[] newValueId,
            IDbEntityRef<TEntityToSet> reference,
            long?[] referenceIdValue,
            Action<long?>[] referenceIdSetter,
            Func<TEntityToSet, IDbEntitySet<TThisEntity>> otherSideSet,
            Action<TEntityToSet, TThisEntity> setOtherSideEntity,
            EventHandler idChangedAction)
            where TEntityToSet : DbEntity
            where TThisEntity : DbEntity
        {
            if (!_lazyLoadChildren && newValue != null && newValue._lazyLoadChildren)
            {
                throw new InvalidOperationException($"Lazy loading of {typeof(TEntityToSet).Name} is disabled, so is the assignment");
            }

            if (newValue != null && !newValue._lazyLoadChildren && _lazyLoadChildren)
            {
                throw new InvalidOperationException(
                    "Cannot assign an entity of type {typeof(TEntityToSet).Name} which has _lazyLoadChildren == false");
            }

            TEntityToSet previousValue = (reference.HasLoadedValue || reference.HasAssignedValue) ? reference.GetEntity(null) : null;

            // 2TK BDA INotifyDeveloper Event o.Ä
//#if DEBUG
//            if (WarnDeveloperIfEntitiesAlreadyConnected
//                && MachineInfo.IsDevelopmentMachine
//                && previousValue != null
//                && !ReferenceEquals(previousValue, newValue))
//            {
//                if (Debugger.IsAttached)
//                {
//                    // Entity ist bereits da. Es wurde bereits vorher zugegriffen was nicht gut ist
//                    Debugger.Break();
//                }
//            }

//#endif
            var idEntity = previousValue as ILongId;
            var newIdEntity = newValue as ILongId;
            if (previousValue != null
                && newValue != null
                && idEntity != null
                && newValue != null
                && (((idEntity).Id > 0 && (idEntity).Id == (newIdEntity)?.Id) // Entity from the database; even if it is loaded again, we assume it is the same
                    || ((idEntity).Id == 0 && ReferenceEquals(previousValue, newValue)))) // Newly created entity
            {
                return;
            }

            var referenceIsForeignKey = ((IDbEntityRefInternal)reference).IsForeignKey;
            // Special case. Entity is explicitly set to null (we must know that the entity was set)
            if (!reference.HasLoadedValue && !reference.HasAssignedValue && newValue == null)
            {
                reference.SetEntity(null);
                if (referenceIsForeignKey)
                {
                    for (int i = 0; i < referenceIdValue.Length; i++)
                    {
                        if (referenceIdValue[i].HasValue)
                        {
                            // In order that IdChanged() or similar events are called, we assign the property instead of directly the referenceIdValue
                            referenceIdSetter[i](null);
                        }
                    }
                }

                return;
            }

            // Special case. Foreign key set to null for 1-1 relations
            if ((reference.HasLoadedValue || reference.HasAssignedValue)
                && newValue == null
                && !referenceIsForeignKey
                && !ReferenceEquals(previousValue, newValue)
                && otherSideSet == null)
            {
                if (setOtherSideEntity != null)
                {
                    setOtherSideEntity(previousValue, null);
                }
                reference.SetEntity(null);
                return;
            }

            // 1-N relations
            if (!ReferenceEquals(previousValue, newValue)
                || (otherSideSet != null && (!(reference.HasLoadedValue || reference.HasAssignedValue))))
            {
                if (otherSideSet != null && previousValue != null /* && !reference.HasAssignedValue*/)
                {
                    reference.SetEntity(null);
                    otherSideSet(previousValue).Remove(this as TThisEntity);
                    if (idChangedAction != null)
                    {
                        previousValue.IdChanged -= idChangedAction;
                    }
                }

                bool loadReference = isLoaded && newValue != null && reference.HasLoadedValue == false && reference.HasAssignedValue == false;
                if (loadReference)
                {
                    for (int i = 0; i < referenceIdValue.Length; i++)
                    {
                        if (referenceIdValue[i] != newValueId[i])
                        {
                            loadReference = false;
                            break;
                        }
                    }

                    if (loadReference)
                    {
                        reference.Load(newValue);
                        if (newValue != null && newValue._lazyLoadChildren)
                        {
                            if (otherSideSet != null)
                            {
                                var otherSide = otherSideSet(newValue);
                                if (otherSide is DbEntitySet<TThisEntity> && ((DbEntitySet<TThisEntity>)otherSide).lazyLoadValues)
                                {
                                    otherSide.Add(this as TThisEntity);
                                }
                            }
                            else if (setOtherSideEntity != null)
                            {
                                setOtherSideEntity(newValue, (TThisEntity)this);
                            }
                        }
                    }
                }

                if (!loadReference)
                {
                    // Only update the referenceId if this entity was not loaded from the DB
                    // or when it is no primary key, or when the referenceId is set to null (i.e. the other item is going to be deleted)
                    // or the opposite: referenceId is currently null and is now set to another value
                    // added && referenceIsForeignKey because of illegal set action to null to parent entity (child was set to null)
                    if ((newValue == null && referenceIsForeignKey)
                        || referenceIdValue[0] == null
                        || (newValue != null && (!isLoaded || referenceIsForeignKey)))
                    {
                        for (int i = 0; i < referenceIdSetter.Length; i++)
                        {
                            // In order that IdChanged() or similar events are called, we assign the property instead of directly the referenceIdValue
                            // Set the id explicitly to 0 (zero), such that e.g. InvoicingId.HasValue returns true, so by looking at InvoicingId you know that Invoicoing was set to an instance
                            referenceIdSetter[i](newValue == null ? null : newValueId[i]);
                            // referenceIdSetter[i](newValue == null || newValueId[i] == 0 ? null : newValueId[i]);
                        }
                    }

                    if (newValue != null
                        &&
                        ((!reference.HasLoadedValue && !reference.HasAssignedValue) || reference.GetEntity(null) == null ||
                         ((ILongId)reference.GetEntity(null)).Id != ((ILongId)newValue).Id)
                        || (newValue == null && reference.GetEntity(null) != null))
                    {
                        reference.SetEntity(newValue);
                        if (newValue != null)
                        {
                            if (otherSideSet != null)
                            {
                                var otherSide = otherSideSet(newValue);
                                if (otherSide is DbEntitySet<TThisEntity> && ((DbEntitySet<TThisEntity>)otherSide).lazyLoadValues &&
                                    (!isLoaded || previousValue != null || reference.HasAssignedValue))
                                {
                                    otherSide.Add(this as TThisEntity);
                                }
                            }
                            else if (setOtherSideEntity != null)
                            {
                                if (previousValue != null
                                    && !referenceIsForeignKey
                                    && !ReferenceEquals(previousValue, newValue)
                                    && otherSideSet == null)
                                {
                                    setOtherSideEntity(previousValue, null);
                                }
                                setOtherSideEntity(newValue, (TThisEntity)this);
                            }

                            if (idChangedAction != null)
                            {
                                newValue.IdChanged += idChangedAction;
                            }
                        }
                    }
                }
            }
        }

        protected void UpdateColumnIfDifferent<TDbEntity, TId>(ref DbValue<TId> column, TId value, ref IDbEntityRef<TDbEntity> dbEntityRef)
            where TDbEntity : DbEntity
            where TId : struct
        {
            if (ShouldUpdateColumn(ref column, value, ref dbEntityRef, () => (long)(object)value))
            {
                column.Entity = value;
            }
        }

        private bool ShouldUpdateColumn<TDbEntity, TId>(
            ref DbValue<TId> column,
            TId value,
            ref IDbEntityRef<TDbEntity> dbEntityRef,
            Func<long> getValueAsLongOrZero)
            where TDbEntity : DbEntity
        {
            if (EqualityComparer<TId>.Default.Equals(column.Entity, value))
            {
                return false;
            }

            if (!_allowSettingColumns)
            {
                var writeDb = _db as WriteDb;
                if (writeDb != null && writeDb.Settings.ReturnPreviouslyLoadedEntity_Obsolete)
                {
                    if (column.ValueWasAssigned() || (dbEntityRef != null && dbEntityRef.HasAssignedValue))
                    {
                        // we do not want to modify updated entities by returning them again with a query
                        return false;
                    }
                }

                throw new InvalidOperationException(
                    "Review your query. It smells wrong! Most probably you used UnionDb() with queries that have different columns.");
            }

            if (dbEntityRef != null)
            {
                if (dbEntityRef is DbEntityRefCached<TDbEntity>)
                {
                    dbEntityRef = GlobalDbConfiguration.GetConfigurationOrEmpty(GetType()).CacheService.GetDbEntityRefCached<TDbEntity>((int)getValueAsLongOrZero());
                }
                else
                {
                    dbEntityRef.ResetValue();
                }
            }

            return true;
        }

        protected void UpdateColumnIfDifferent<TDbEntity, TId>(ref DbValue<TId?> column, TId? value, ref IDbEntityRef<TDbEntity> dbEntityRef)
            where TDbEntity : DbEntity
            where TId : struct
        {
            if (ShouldUpdateColumn(ref column, value, ref dbEntityRef, () => (value as long?) ?? 0))
            {
                column.Entity = value;
            }
        }

        public override bool Equals(object other)
        {
            if (other != null && other.GetType() == this.GetType())
            {
                var idEntity = this as ILongId;
                var otherIdEntity = other as ILongId;
                if (idEntity != null && otherIdEntity != null && idEntity.Id != 0)
                {
                    return idEntity.Id == otherIdEntity.Id;
                }
                else
                {
                    // If item does not come from the database, we compare the reference
                    return ReferenceEquals(this, other);
                }
            }

            return false;
        }

        public override int GetHashCode()
        {
            var idEntity = this as ILongId;
            if (idEntity != null && idEntity.Id > 0)
            {
                return idEntity.Id.GetHashCode();
            }

            return System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(this);
        }

        public override string ToString()
        {
            return "[" + GetType().Name + "] " + ObjectUtils.PropertiesToString(this, p => p.Name == "Id" || p.IsDefined(typeof(ValidateAttribute), false));
        }

        int IComparable.CompareTo(object obj)
        {
            if (obj == null)
            {
                return -1;
            }

            var idEntity = this as ILongId;
            var otherIdEntity = obj as ILongId;
            if (obj.GetType() != GetType() || idEntity == null || otherIdEntity == null)
            {
                throw new ArgumentException($"object is not a {GetType().Name}");
            }

            return idEntity.Id.CompareTo(otherIdEntity.Id);
        }

        void IHandleChildren.HandleChildren(DbEntityVisitorBase visitor)
        {
            HandleChildren(visitor);
        }

        protected abstract void HandleChildren(DbEntityVisitorBase visitor);

        /// <summary>
        /// OBSOLETE: Call "visitor.Process(dbEntity)" instead.
        /// </summary>
        public void Welcome(DoNotLazyLoadChildrenVisitor visitor)
        {
            visitor.Process(this);
        }

        /// <summary>
        /// OBSOLETE: Call "visitor.Process(dbEntity)" instead.
        /// </summary>
        public void Welcome(GetChildrenFromCacheVisitor visitor)
        {
            visitor.Process(this);
        }

        protected override void ValidateForDelete()
        {
            var validation = GetValidationHandler();
            validation.ValidateForDelete(new[] { this });
            base.ValidateForDelete();
        }

        private IDbEntityValidator GetValidationHandler()
        {
            return GlobalDbConfiguration.GetConfigurationOrEmpty(GetType()).Validator;
        }

        protected override void ValidateAutoForInsertOrUpdate()
        {
            var validation = GetValidationHandler();
            validation.ValidateForInsertOrUpdate(new[] { this });

            base.ValidateAutoForInsertOrUpdate();
        }
    }
}