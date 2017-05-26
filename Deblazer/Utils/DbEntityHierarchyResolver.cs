using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Reflection;

namespace Dg.Deblazer.Utils
{
    internal class DbEntityHierarchyResolver
    {
        public IReadOnlyList<IImmutableSet<PropertyInfo>> ResolveHierarchy(
            Type dbEntityType)
        {
            var hierarchy = new List<ImmutableHashSet<PropertyInfo>>();
            HashSet<PropertyInfo> dependencies = null;
            var entityTypes = new[] { dbEntityType };
            do
            {
                dependencies = new HashSet<PropertyInfo>();
                foreach (var entityType in entityTypes)
                {
                    foreach (var entity in GetEntityTypesReferencingGivenEntity(entityType))
                    {
                        dependencies.Add(entity);
                    }
                }
                if (dependencies.Any())
                {
                    hierarchy.Add(dependencies.ToImmutableHashSet());
                }
                // prevent cyclular dependencies from leading to the endless loop
                entityTypes = dependencies.Select(pi => pi.DeclaringType)
                    .Where(t => !IsAlreadyAnalized(hierarchy, t))
                    .ToArray();
            } while (entityTypes.Any());

            return hierarchy;
        }

        private static bool IsAlreadyAnalized(List<ImmutableHashSet<PropertyInfo>> hierarchy, Type t)
        {
            foreach (var dependencies in hierarchy)
            {
                if (dependencies.Any(pi => pi.PropertyType == t))
                {
                    return true;
                }
            }
            return false;
        }

        private static IReadOnlyList<PropertyInfo> GetEntityTypesReferencingGivenEntity(Type type)
        {
            // Check all nullable references of this type to other objects
            PropertyInfo[] propertyInfos = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);
            var neighbourPropertyInfos = new List<PropertyInfo>();
            foreach (PropertyInfo pi in propertyInfos)
            {
                // Check if the property returns a linq object
                neighbourPropertyInfos.AddRange(
                    GetNeighborEntityPropertyInfo(
                        propertyInfo: pi,
                        propertyInfos: propertyInfos));

            }

            return neighbourPropertyInfos;
        }

        private static IImmutableSet<PropertyInfo> GetNeighborEntityPropertyInfo(PropertyInfo propertyInfo, PropertyInfo[] propertyInfos)
        {
            if (!propertyInfo.Name.EndsWith("Id")
                && HasNullableIdProperty(propertyInfo, propertyInfos))
            {
                // If "entity" has a property called ProductId, it is pointing to Product, 
                // not Product pointing to entity. But we want the entities pointing to "entity".
                var neighborType = propertyInfo.PropertyType;
                return GetNeighborEntityPropertyInfos(propertyInfo, neighborType);
            }
            else if (IsGenericCollection(propertyInfo))
            {
                var genericTypeArgument = propertyInfo.PropertyType.GetGenericArguments()[0];
                return GetNeighborEntityPropertyInfos(propertyInfo, genericTypeArgument);
            }

            return NoPropertyInfos();
        }

        private static IImmutableSet<PropertyInfo> GetNeighborEntityPropertyInfos(PropertyInfo propertyInfo, Type neighborType)
        {
            if (IsDbEntity(neighborType))
            {
                return neighborType.GetProperties(BindingFlags.Instance | BindingFlags.Public)
                    .Where(pi => pi.PropertyType == propertyInfo.DeclaringType)
                    .ToImmutableHashSet();
            }
            return NoPropertyInfos();
        }

        private static IImmutableSet<PropertyInfo> NoPropertyInfos()
        {
            return new PropertyInfo[0].ToImmutableHashSet();
        }

        private static bool IsGenericCollection(PropertyInfo propertyInfo)
        {
            return propertyInfo.PropertyType.IsGenericType
                && typeof(IEnumerable).IsAssignableFrom(propertyInfo.PropertyType.GetGenericTypeDefinition());
        }

        private static bool HasNullableIdProperty(PropertyInfo propertyInfo, PropertyInfo[] propertyInfos)
        {
            var idProperty = propertyInfos.SingleOrDefault(p => p.Name == propertyInfo.Name + "Id");
            return idProperty != null
                && idProperty.PropertyType.IsGenericType
                && idProperty.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>);
        }

        private static bool IsDbEntity(
            Type type)
        {
            return typeof(DbEntity).IsAssignableFrom(type);
        }
    }
}
