using Dg.Deblazer.Api;
using Dg.Deblazer.Comparer;
using Dg.Deblazer.Extensions;
using JetBrains.Annotations;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Dg.Deblazer.Utils
{
    public static class ObjectUtils
    {
        /// <summary>
        /// Copies all simple properties from source to toUpdate. Updates all children of source and toUpdate recursively.
        /// Only this overload is public because the alreadyCheckedEntities is dangerous if the wrong comparer is used.
        /// </summary>
        /// <param name="copyFrom"></param>
        /// <param name="copyTo"></param>
        public static void UpdateCachedEntity(DbEntity copyFrom, DbEntity copyTo)
        {
            UpdateCachedEntity(copyFrom, copyTo, null);
        }

        private static void UpdateCachedEntity(DbEntity copyFrom, DbEntity copyTo, HashSet<DbEntity> alreadyCheckedEntities)
        {
            if (copyTo.GetType() != copyFrom.GetType())
            {
                throw new Exception("cachedEntity and newEntity must be of the same type");
            }

            if (alreadyCheckedEntities == null)
            {
                alreadyCheckedEntities = new HashSet<DbEntity>(new ObjectReferenceEqualityComparer<DbEntity>());
            }

            if (alreadyCheckedEntities.Contains(copyTo))
            {
                return;
            }

            alreadyCheckedEntities.Add(copyTo);

            foreach (var fieldInfo in copyTo.GetType().GetFields(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance))
            {
                if (fieldInfo.FieldType.IsGenericType && fieldInfo.FieldType.GetGenericTypeDefinition() == typeof(IDbEntityRef<>))
                {
                    var iDbEntityRef = fieldInfo.GetValue(copyTo);
                    var newIDbEntityRef = fieldInfo.GetValue(copyFrom);
                    if (iDbEntityRef == null && newIDbEntityRef != null)
                    {
                        fieldInfo.SetValue(copyTo, newIDbEntityRef);
                    }
                    else if (iDbEntityRef != null && newIDbEntityRef == null)
                    {
                        fieldInfo.SetValue(copyTo, null);
                    }
                    else if (iDbEntityRef != null && newIDbEntityRef != null && iDbEntityRef.GetType().GetGenericTypeDefinition() == typeof(DbEntityRef<>))
                    {
                        var entityRef =
                            (DbEntity)iDbEntityRef.GetType().GetField("entity", BindingFlags.NonPublic | BindingFlags.Instance)?.GetValue(iDbEntityRef);
                        var newEntityRef =
                            (DbEntity)
                                fieldInfo.GetValue(copyFrom)
                                    .GetType()
                                    .GetField("entity", BindingFlags.NonPublic | BindingFlags.Instance)
                                    .GetValue(newIDbEntityRef);
                        if (entityRef == null && newEntityRef != null)
                        {
                            fieldInfo.SetValue(copyTo, newIDbEntityRef);
                        }
                        else if (entityRef != null && newEntityRef == null)
                        {
                            fieldInfo.SetValue(copyTo, null);
                        }
                        else if (entityRef != null && newEntityRef != null)
                        {
                            UpdateCachedEntity(newEntityRef, entityRef, alreadyCheckedEntities);
                        }
                    }
                }
                else if (fieldInfo.FieldType.IsGenericType && fieldInfo.FieldType.GetGenericTypeDefinition() == typeof(IDbEntitySet<>))
                {
                    var iDbEntitySet = fieldInfo.GetValue(copyTo);
                    var newIDbEntitySet = fieldInfo.GetValue(copyFrom);
                    if (iDbEntitySet == null && newIDbEntitySet != null)
                    {
                        fieldInfo.SetValue(copyTo, newIDbEntitySet);
                    }
                    else if (iDbEntitySet != null && newIDbEntitySet == null)
                    {
                        fieldInfo.SetValue(copyTo, null);
                    }
                    else if (iDbEntitySet != null && newIDbEntitySet != null && iDbEntitySet.GetType().GetGenericTypeDefinition() == typeof(DbEntitySet<>))
                    {
                        var dbEntitiesField = iDbEntitySet.GetType().GetField("entities", BindingFlags.NonPublic | BindingFlags.Instance);
                        var dbEntitiesFieldValue = dbEntitiesField.GetValue(iDbEntitySet);
                        var newDbEntitiesField = newIDbEntitySet.GetType().GetField("entities", BindingFlags.NonPublic | BindingFlags.Instance);
                        var newDbEntitiesFieldValue = newDbEntitiesField.GetValue(newIDbEntitySet);
                        var dbEntities = (IList)dbEntitiesFieldValue;
                        var newDbEntities = (IList)newDbEntitiesFieldValue;
                        for (int i = 0; i < Math.Max(newDbEntities.Count, dbEntities.Count); i++)
                        {
                            if (newDbEntities.Count <= i)
                            {
                                while (dbEntities.Count > newDbEntities.Count)
                                {
                                    dbEntities.RemoveAt(i);
                                }

                                break;
                            }

                            if (dbEntities.Count <= i)
                            {
                                dbEntities.Add(newDbEntities[i]);
                            }
                            else
                            {
                                UpdateCachedEntity((DbEntity)newDbEntities[i], (DbEntity)dbEntities[i], alreadyCheckedEntities);
                            }
                        }

                        // if (dbEntities.Select(e => ((IId)e).Id).Distinct().Count() != dbEntities.Count)
                        // {
                        //    Console.WriteLine();
                        // }
                    }
                }
                else
                {
                    fieldInfo.SetValue(copyTo, fieldInfo.GetValue(copyFrom));
                }
            }
        }

        // private void SetValue(FieldInfo fieldInfo, DbEntity cachedEntity, object obj1, object obj2, Type genericTypeDefinition)
        // {
        //    if (obj1 == null && obj2 != null)
        //    {
        //        fieldInfo.SetValue(cachedEntity, obj2);
        //    }

        //    else if (obj1 != null && obj2 == null)
        //    {
        //        fieldInfo.SetValue(cachedEntity, null);
        //    }

        //    else if (obj1 != null && obj2 != null && obj1.GetType().GetGenericTypeDefinition() == genericTypeDefinition)
        //    {
        //    }

        // }

        /// <summary>
        /// Clone one object with reflection
        /// </summary>
        public static void Copy<T>(T copyFrom, T copyTo, bool onlySimpleProperties = true, bool copyIdField = true)
        {
            Copy(copyFrom, copyTo, onlySimpleProperties, !copyIdField ? new[] { "Id" } : null);
        }

        /// <summary>
        /// Clone one object with reflection
        /// </summary>
        public static void Copy<T>(T copyFrom, T copyTo, bool onlySimpleProperties = true, params string[] ignoreProperties)
        {
            // Get all the fields of the type, also the privates.
            PropertyInfo[] pis = copyFrom.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public);
            // Loop through all the fields and copy the information from the parameter class
            // to the newPerson class.
            foreach (PropertyInfo pi in pis)
            {
                if (ignoreProperties == null || !ignoreProperties.Contains(pi.Name))
                {
                    if (pi.CanWrite && (!onlySimpleProperties || pi.PropertyType == typeof(string) || pi.PropertyType.IsValueType))
                    {
                        pi.SetValue(copyTo, pi.GetValue(copyFrom, null), null);
                    }
                }
            }
        }

        /// <summary>
        /// Clones the given object with all its subentities if their type is contained in propertiesToClone too
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="copyFrom"></param>
        /// <param name="propertiesToCloneToo"></param>
        /// <returns></returns>
        public static T DeepCloneLinqObject<T>(T copyFrom, params Type[] propertiesToCloneToo)
        {
            return (T)DeepCloneLinqObject(copyFrom, propertiesToCloneToo, new Dictionary<object, object>());
        }

        private static object DeepCloneLinqObject(object copyFrom, Type[] propertiesToCloneToo, Dictionary<object, object> clonedEntities)
        {
            object copyTo = Activator.CreateInstance(copyFrom.GetType());
            // Get all the fields of the type, also the privates.
            PropertyInfo[] propertyInfos = copyFrom.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public);
            // Loop through all the fields and copy the information from the parameter class
            // to the newPerson class.

            foreach (PropertyInfo propertyInfo in propertyInfos)
            {
                // Do not clone the Id field
                if (propertyInfo.Name != "Id")
                {
                    if (propertiesToCloneToo.Contains(propertyInfo.PropertyType))
                    {
                        var valueOfProperty = propertyInfo.GetValue(copyFrom, null);
                        object clone;
                        if (!clonedEntities.TryGetValue(valueOfProperty, out clone))
                        {
                            clonedEntities[valueOfProperty] = null;
                            clone = DeepCloneLinqObject(valueOfProperty, propertiesToCloneToo, clonedEntities);
                            clonedEntities[valueOfProperty] = clone;
                        }

                        if (clone != null)
                        {
                            propertyInfo.SetValue(copyTo, clone, null);
                        }
                    }
                    else if (propertyInfo.PropertyType.IsGenericType && !propertyInfo.PropertyType.IsGenericTypeDefinition
                             && propertyInfo.PropertyType.GetGenericTypeDefinition() == typeof(IDbEntitySet<>)
                             && propertiesToCloneToo.Contains(propertyInfo.PropertyType.GetGenericArguments()[0]))
                    {
                        var copyToSet = propertyInfo.GetValue(copyTo, null) as IEnumerable;
                        var copyFromSet = propertyInfo.GetValue(copyFrom, null) as IEnumerable;
                        foreach (object valueOfPropertyInSet in copyFromSet)
                        {
                            object clone;
                            if (!clonedEntities.TryGetValue(valueOfPropertyInSet, out clone))
                            {
                                clonedEntities[valueOfPropertyInSet] = null;
                                clone = DeepCloneLinqObject(valueOfPropertyInSet, propertiesToCloneToo, clonedEntities);
                                clonedEntities[valueOfPropertyInSet] = clone;
                            }

                            if (clone != null)
                            {
                                if (!(bool)copyToSet.GetType().GetMethod("Contains").Invoke(copyToSet, new[] { clone }))
                                {
                                    copyToSet.GetType().GetMethod("Add").Invoke(copyToSet, new[] { clone });
                                }
                            }
                        }
                    }
                    else if (propertyInfo.CanWrite &&
                             (propertyInfo.PropertyType == typeof(string) || propertyInfo.PropertyType.IsValueType ||
                              propertyInfo.PropertyType == typeof(byte[])))
                    {
                        propertyInfo.SetValue(copyTo, propertyInfo.GetValue(copyFrom, null), null);
                    }
                }
            }

            return copyTo;
        }

        /// <summary>
        /// Clone one object and its DbContents with reflection
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="copyFrom"></param>
        /// <param name="cloneIdField"></param>
        /// <param name="propertyFilter">Allows to specify which properties should be cloned. Return true, if the value should be copied; false otherwise.</param>
        /// <returns></returns>
        public static T CloneLinqObject<T>(T copyFrom, bool cloneIdField = true, Predicate<PropertyInfo> propertyFilter = null)
        {
            return (T)CloneLinqObject((object)copyFrom, cloneIdField, propertyFilter);
        }

        /// <summary>
        /// Clone one object and its DbContents with reflection
        /// </summary>
        private static object CloneLinqObject(object copyFrom, bool cloneIdField, Predicate<PropertyInfo> propertyFilter)
        {
            if (typeof(ICloneSpecially).IsAssignableFrom(copyFrom.GetType()))
            {
                var cloneAble = copyFrom as ICloneSpecially;
                return cloneAble?.GetClone(cloneIdField);
            }

            object copyTo = Activator.CreateInstance(copyFrom.GetType());
            // Get all the fields of the type, also the privates.
            var propertyInfos = copyFrom.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public);
            // Loop through all the fields and copy the information from the parameter class
            // to the newPerson class.

            foreach (var propertyInfo in propertyInfos)
            {
                if (!cloneIdField
                    && (propertyInfo.Name == nameof(IId.Id) || propertyInfo.Name == nameof(ILongId.Id)))
                {
                    continue;
                }

                if (propertyFilter == null || propertyFilter(propertyInfo))
                {
                    if (propertyInfo.PropertyType.GetInterfaces().Contains(typeof(ICloneSpecially))
                        && (!(propertyInfo.GetValue(copyFrom) as ICloneSpecially)?.GetExcludedTypes().Contains(copyFrom.GetType()) ?? false))
                    {
                        // Clone the ICloneSpeciallies too; they are considered as being part of the object
                        propertyInfo.SetValue(copyTo, CloneLinqObject(propertyInfo.GetValue(copyFrom, null), cloneIdField, null), null);
                    } // Do not clone EntitySets or IEnumerable or pointers to other Linq objects
                    else if (propertyInfo.CanWrite &&
                             (propertyInfo.PropertyType == typeof(string)
                              || propertyInfo.PropertyType.IsValueType
                              || propertyInfo.PropertyType == typeof(byte[])))
                    {
                        propertyInfo.SetValue(copyTo, propertyInfo.GetValue(copyFrom, null), null);
                    }
                }
            }

            return copyTo;
        }

        public static PropertyInfo[] GetPublicProperties(object obj)
        {
            return obj.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public);
        }

        public static PropertyInfo[] GetPublicSimpleProperties(object dbEntity)
        {
            return GetPublicSimpleProperties(dbEntity.GetType());
        }

        public static PropertyInfo[] GetPublicSimpleProperties(Type dbEntityType)
        {
            List<PropertyInfo> properties = new List<PropertyInfo>();

            PropertyInfo[] pis = dbEntityType.GetProperties(BindingFlags.Instance | BindingFlags.Public);
            // Loop through all the fields and copy the information from the parameter class
            // to the newPerson class.
            foreach (PropertyInfo pi in pis)
            {
                // Do not clone EntitySets or IEnumerable or such...only primitive types (int, int?, double, etc.)
                if (pi.CanWrite
                    && (pi.PropertyType == typeof(string)
                        || pi.PropertyType.IsValueType))
                {
                    properties.Add(pi);
                }
            }

            return properties.ToArray();
        }

        public static string PropertiesToString(object entity, Func<PropertyInfo, bool> predicate = null)
        {
            StringBuilder sb = new StringBuilder();
            foreach (var property in GetPublicSimpleProperties(entity)
                .Where(predicate ?? ((PropertyInfo e) => true))
                .OrderBy(p => p.Name == "Id" ? 0 : 1))
            {
                if (sb.Length > 0)
                {
                    sb.Append(", ");
                }

                sb.Append(property.Name + " = " + GetPropertyValue(entity, property));
            }

            return sb.ToString();
        }

        private static object GetPropertyValue(object entity, PropertyInfo property)
        {
            try
            {
                return property.GetValue(entity, null) ?? "null";
            }
            catch
            {
                return "EXCEPTION";
            }
        }

        public static IList<Tuple<PropertyInfo, DbEntity>> GetAllNeighborsWithNullablePointers(DbEntity entity, Predicate<PropertyInfo> predicate)
        {
            List<DbEntity> neighbors = GetEntitiesReferencingGivenEntity(entity, predicate);
            return GetAllNeighbors(entity, neighbors, AddNullableNeighbor);
        }

        private static IList<Tuple<PropertyInfo, DbEntity>> GetAllNeighbors(DbEntity entity, List<DbEntity> neighbors, AddNeighborDelegate deleg)
        {
            Type type = entity.GetType();

            var propertyInfos = new List<Tuple<PropertyInfo, DbEntity>>();
            // for all neighbors, check if they have a nullable reference to the current entity
            foreach (DbEntity neighbor in neighbors)
            {
                Type neighborType = neighbor.GetType();
                foreach (var PropertyInfp in neighborType
                    .GetProperties(BindingFlags.Instance | BindingFlags.Public).Where(i => i.CanWrite && i.PropertyType == type))
                {
                    PropertyInfo pointerToEntity = neighborType.GetProperty(PropertyInfp.Name + "Id");
                    if (pointerToEntity == null)
                    {
                        AssociationAttribute associationAttribute
                            = (AssociationAttribute)Attribute.GetCustomAttribute(PropertyInfp, typeof(AssociationAttribute));
                        if (associationAttribute != null)
                        {
                            pointerToEntity = neighborType.GetProperty(associationAttribute.OtherKey);
                        }
                    }

                    if (pointerToEntity != null)
                    {
                        deleg(entity, neighbor, PropertyInfp, pointerToEntity, propertyInfos);
                    }
                }
            }

            return propertyInfos;
        }

        private static void AddNullableNeighbor(
            DbEntity entity,
            DbEntity neighbor,
            PropertyInfo pi,
            PropertyInfo pointerToEntity,
            IList<Tuple<PropertyInfo, DbEntity>> propertyInfos)
        {
            if (pointerToEntity.PropertyType == typeof(int?))
            {
                int? pointerValueToEntity = (int?)pointerToEntity.GetValue(neighbor, null);
                if (pointerValueToEntity.HasValue && pointerValueToEntity.Value == ((IId)entity).Id)
                {
                    propertyInfos.Add(new Tuple<PropertyInfo, DbEntity>(pi, neighbor));
                }
            }
            else if (pointerToEntity.PropertyType == typeof(long?))
            {
                int? pointerValueToEntity = (int?)pointerToEntity.GetValue(neighbor, null);
                if (pointerValueToEntity.HasValue && pointerValueToEntity.Value == ((ILongId)entity).Id)
                {
                    propertyInfos.Add(new Tuple<PropertyInfo, DbEntity>(pi, neighbor));
                }
            }
            else if (IdTypeExtension.IsNullableIdType(pointerToEntity.PropertyType))
            {
                var rowId = (int?)pointerToEntity.GetValue(neighbor, null).Convert(typeof(int?));

                if (rowId.HasValue && rowId.Value == ((IId)entity).Id)
                {
                    propertyInfos.Add(new Tuple<PropertyInfo, DbEntity>(pi, neighbor));
                }
            }
            else if (LongIdTypeExtension.IsNullableLongIdType(pointerToEntity.PropertyType))
            {
                var rowId = (long?)pointerToEntity.GetValue(neighbor, null).Convert(typeof(long?));

                if (rowId.HasValue && rowId.Value == ((ILongId)entity).Id)
                {
                    propertyInfos.Add(new Tuple<PropertyInfo, DbEntity>(pi, neighbor));
                }
            }
        }

        public static IList<Tuple<PropertyInfo, DbEntity>> GetAllNeighborsWithNonNullablePointers(DbEntity entity, Predicate<PropertyInfo> predicate)
        {
            List<DbEntity> neighbors = GetEntitiesReferencingGivenEntity(entity, predicate);
            return GetAllNeighbors(entity, neighbors, AddNonNullableNeighbor);
        }

        private static void AddNonNullableNeighbor(
            DbEntity entity,
            DbEntity neighbor,
            PropertyInfo pi,
            PropertyInfo pointerToEntity,
            IList<Tuple<PropertyInfo, DbEntity>> propertyInfos)
        {
            if (pointerToEntity.PropertyType == typeof(int))
            {
                int pointerValueToEntity = (int)pointerToEntity.GetValue(neighbor, null);
                if (pointerValueToEntity == ((IId)entity).Id)
                {
                    propertyInfos.Add(new Tuple<PropertyInfo, DbEntity>(pi, neighbor));
                }
            }
            else if (pointerToEntity.PropertyType == typeof(long))
            {
                long pointerValueToEntity = (long)pointerToEntity.GetValue(neighbor, null);
                if (pointerValueToEntity == ((ILongId)entity).Id)
                {
                    propertyInfos.Add(new Tuple<PropertyInfo, DbEntity>(pi, neighbor));
                }
            }
            else if (IdTypeExtension.IsIdType(pointerToEntity.PropertyType))
            {
                var rowId = (int)pointerToEntity.GetValue(neighbor, null).Convert(typeof(int));

                if (rowId == ((IId)entity).Id)
                {
                    propertyInfos.Add(new Tuple<PropertyInfo, DbEntity>(pi, neighbor));
                }
            }
            else if (LongIdTypeExtension.IsLongIdType(pointerToEntity.PropertyType))
            {
                var rowId = (long)pointerToEntity.GetValue(neighbor, null).Convert(typeof(long));

                if (rowId == ((ILongId)entity).Id)
                {
                    propertyInfos.Add(new Tuple<PropertyInfo, DbEntity>(pi, neighbor));
                }
            }
        }

        private static List<DbEntity> GetEntitiesReferencingGivenEntity(DbEntity entity, Predicate<PropertyInfo> predicate)
        {
            List<DbEntity> neighbors = new List<DbEntity>();

            // Check all nullable references of this instance to other objects
            Type type = entity.GetType();
            PropertyInfo[] pis = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);
            neighbors = new List<DbEntity>();
            foreach (PropertyInfo pi in pis.Where(p => predicate(p)))
            {
                // Check if the property returns a linq object
                DbEntity neighbor = GetNeighborEntityFromPropertyInfo(entity, propertyInfo: pi, propertyInfos: pis);
                if (neighbor != null)
                {
                    neighbors.Add(neighbor);
                }
                else if (pi.PropertyType.IsGenericType)
                {
                    // All collection properties are validated too
                    if (typeof(IEnumerable).IsAssignableFrom(pi.PropertyType.GetGenericTypeDefinition()))
                    {
                        if (pi.PropertyType.GetGenericArguments()[0].Namespace == type.Namespace)
                        {
                            foreach (object obj in (IEnumerable)pi.GetValue(entity, null))
                            {
                                neighbor = obj as DbEntity;
                                if (neighbor != null)
                                {
                                    neighbors.Add(neighbor);
                                }
                            }
                        }
                    }
                }
            }

            return neighbors;
        }

        [CanBeNull]
        private static DbEntity GetNeighborEntityFromPropertyInfo(DbEntity entity, PropertyInfo propertyInfo, PropertyInfo[] propertyInfos)
        {
            if (!propertyInfo.Name.EndsWith("Id")
                && !propertyInfos.Any(p => p.Name == propertyInfo.Name + "Id"))
            {
                // If "entity" has a property called ProductId, it is pointing to Product, not Product pointing to entity. But we want the entities pointing to "entity".
                var neighbor = propertyInfo.GetValue(entity, null) as DbEntity;
                if (neighbor != null)
                {
                    return neighbor;
                }
            }

            return null;
        }

        private delegate void AddNeighborDelegate(
            DbEntity entity,
            DbEntity neighbor,
            PropertyInfo pi,
            PropertyInfo pointerToEntity,
            IList<Tuple<PropertyInfo, DbEntity>> propertyInfos);
    }
}