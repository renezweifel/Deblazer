using Dg.Deblazer.Extensions;
using Dg.Deblazer.Internal;
using Dg.Deblazer.Read;
using Dg.Deblazer.SqlUtils;
using Dg.Deblazer.Visitors;
using JetBrains.Annotations;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace Dg.Deblazer.SqlGeneration
{
    public static class QueryHelpers
    {
        private static readonly ConcurrentDictionary<Type, IHelper> helpersByEntityType = new ConcurrentDictionary<Type, IHelper>(concurrencyLevel: 1, capacity: 0);

        public static void AddSqlParameters(DbSqlCommand sqlCommand, IReadOnlyList<object> parameters)
        {
            AddSqlParameters(sqlCommand, "", parameters);
        }

        public static void AddSqlParameters(DbSqlCommand sqlCommand, string parameterPrefix, IReadOnlyList<object> parameters)
        {
            if (parameters != null)
            {
                for (int i = 0; i < parameters.Count; i++)
                {
                    AddSqlParameter(sqlCommand.Parameters, "@" + parameterPrefix + i, parameters[i]);
                }
            }
        }

        public static void AddSqlParameter(SqlParameterCollection sqlParameterCollection, string paramName, object parameter)
        {
            var sqlParameter = SqlParameterUtils.Create(paramName, parameter);
            sqlParameterCollection.Add(sqlParameter);
        }

        public static object Fill(Type type, object entity, FillVisitor fillVisitor)
        {
            var helper = GetHelperOrNull(type);
            if (helper != null)
            {
                return helper.Fill(entity, fillVisitor);
            }

            entity = fillVisitor.Fill(type);

            return entity;
        }

        public static T Fill<T>(T entity, FillVisitor fillVisitor)
        {
            var helper = GetHelperOrNull<T>();
            if (helper != null)
            {
                return helper.Fill(entity, fillVisitor);
            }

            entity = fillVisitor.Fill<T>();
            var dbEntity = entity as IDbEntityInternal;
            if (dbEntity != null)
            {
                dbEntity.ModifyInternalState(new InsertSetVisitor(doReset: true));

                dbEntity.SetDb(fillVisitor.DbInternal);
            }

            return entity;
        }

        public static IEnumerable<string> GetColumnsInSelectStatement<T>(int joinCount = -1) where T : ILongId
        {
            string tableAlias = GetTableAlias<T>(joinCount);
            return GetHelper<T>().ColumnsInSelectStatement.Select(c => string.Format(c, tableAlias));
        }

        internal static IEnumerable<string> GetColumnsInSelectStatement(Type type, int joinCount)
        {
            string tableAlias = GetTableAlias(type, joinCount);
            var helper = GetHelperOrNull(type);
            if (helper == null)
            {
                return Enumerable.Empty<string>();
            }

            return helper.ColumnsInSelectStatement.Select(c => string.Format(c, tableAlias));
        }

        public static string[] GetColumnsInInsertStatement<T>() where T : ILongId
        {
            return GetHelperOrNull<T>()?.ColumnsInInsertStatement;
        }

        public static string[] GetColumnsInInsertStatement(Type type)
        {
            return GetHelperOrNull(type)?.ColumnsInInsertStatement;
        }

        public static string GetCreateTempTableCommand(Type type)
        {
            return GetHelperOrNull(type)?.CreateTempTableCommand;
        }

        public static string GetCreateTempTableCommand<T>() where T : ILongId
        {
            return GetHelperOrNull<T>()?.CreateTempTableCommand;
        }

        internal static string GetTableAlias<T>(int joinCount)
        {
            string tableAlias = joinCount >= 0 ? "[t" + joinCount + "]" : GetFullTableName<T>();
            return tableAlias;
        }

        internal static string GetTableAlias(Type type, int joinCount)
        {
            string tableAlias = joinCount >= 0 ? "[t" + joinCount + "]" : GetFullTableName(type);
            return tableAlias;
        }

        public static string ConcatColumnsInSelectStatement<T>(string tableAlias) where T : ILongId
        {
            return string.Format(GetHelper<T>().ColumnsString, tableAlias);
        }

        public static string ConcatColumnsInSelectStatement<T>(int joinCount = -1) where T : ILongId
        {
            string tableAlias = GetTableAlias<T>(joinCount);
            return string.Format(GetHelper<T>().ColumnsString, tableAlias);
        }

        public static string ConcatColumnsInSelectStatement(Type type, int joinCount)
        {
            string tableAlias = GetTableAlias(type, joinCount);
            return string.Format(GetHelper(type).ColumnsString, tableAlias);
        }

        public static string GetFullTableName<T>(int joinCount) where T : ILongId
        {
            string tableAlias = GetTableAlias<T>(joinCount);
            return GetHelper<T>().FullTableName + " AS " + tableAlias;
        }

        public static string GetFullTableName<T>()
        {
            return GetHelper<T>().FullTableName;
        }

        public static string GetFullTableName(Type type)
        {
            return GetHelper(type).FullTableName;
        }

        public static string GetFullTableName(Type type, int joinCount)
        {
            var tableAlias = GetTableAlias(type, joinCount);
            return GetHelper(type).FullTableName + " AS " + tableAlias;
        }

        public static IHelper<TEntity> GetHelper<TEntity>()
        {
            return (IHelper<TEntity>)GetHelper(typeof(TEntity));
        }

        public static IHelper GetHelper(Type entityType)
        {
            IHelper helper;
            if (!helpersByEntityType.TryGetValue(entityType, out helper))
            {
                Initialize(entityType.Assembly);
                return helpersByEntityType[entityType];
            }

            return helper;
        }

        // Unfortunately there is no concurrent hashset, so we abuse the ConcurrentDictionary to efficiently get and add initialized assemblies. The boolean value is of no interest at all.
        private static ConcurrentDictionary<Assembly, bool> initializedAssemblies = new ConcurrentDictionary<Assembly, bool>();
        private static readonly object initializationLock = new object();

        public static void Initialize(Assembly assembly)
        {
            lock (initializationLock)
            {
                if (initializedAssemblies.ContainsKey(assembly))
                {
                    // Another thread already initialized this assembly
                    return;
                }

                initializedAssemblies.AddOrUpdate(assembly, true, (a, b) => b);
                var helperTypes = GetAllTypes(t => t.IsClass && !t.IsAbstract && typeof(IHelper).IsAssignableFrom(t), assembly);

                Parallel.ForEach(helperTypes, helperType =>
                {
                    var createdHelper = (IHelper)Activator.CreateInstance(helperType);
                    helpersByEntityType[createdHelper.DbType] = createdHelper;
                });
            }
        }

        /// <summary>
        /// Returns the Helper for the DbEntity type or NULL if the specified type is not directly deriving from DbEntity
        /// </summary>
        [CanBeNull]
        public static IHelper<T> GetHelperOrNull<T>()
        {
            // We explicitly check for the base type here because there are some legacy classes which derive from a DbEntity. This is not supported
            // in the DbLayer and would fail later anyways if we would not forbid it here.
            if (typeof(T).BaseType != typeof(DbEntity))
            {
                return null;
            }

            return GetHelper<T>();
        }

        /// <summary>
        /// Returns the Helper for the DbEntity type or NULL if the specified type is not directly deriving from DbEntity
        /// </summary>
        [CanBeNull]
        public static IHelper GetHelperOrNull(Type type)
        {
            // We explicitly check for the base type here because there are some legacy classes which derive from a DbEntity. This is not supported
            // in the DbLayer and would fail later anyways if we would not forbid it here.
            if (type.BaseType != typeof(DbEntity))
            {
                return null;
            }

            return GetHelper(type);
        }

        public static Type GetDbEntityType(string typeName)
        {
            var entityType = helpersByEntityType.Keys.SingleOrDefault(t => t.Name == typeName);
            if (entityType == null)
            {
                // This could also happen if this method is called before the first call to GetHelper. This is very unlikely, the only usage
                // is in UserSelectionEntry.
                throw new KeyNotFoundException($"Could not find Entity by type name '{typeName}'. Check your spelling");
            }
            return entityType;
        }

        private static IReadOnlyList<Type> GetAllTypes(Predicate<Type> predicate, Assembly assembly)
        {
            var types = new List<Type>();

            types.AddRange(GetTypesFromAssembly(predicate, assembly));

            return types;
        }

        private static IReadOnlyList<Type> GetAllTypes(Predicate<Type> predicate, IEnumerable<Assembly> assemblies)
        {
            var relevantAssemblies = assemblies.Where(a => a != null).ToArray();
            // We generally seem to fare better not using parallelism if the number of assemblies is no more than 3
            var useParallelism = relevantAssemblies.Length > 3;
            if (useParallelism)
            {
                var types = new ConcurrentBag<Type>();
                Parallel.ForEach(relevantAssemblies, assembly =>
                {
                    GetTypesFromAssembly(predicate, assembly).ForEach(t => types.Add(t));
                });

                return types.ToList();
            }
            else
            {
                var types = new List<Type>();
                foreach (var assembly in relevantAssemblies)
                {
                    GetTypesFromAssembly(predicate, assembly).ForEach(t => types.Add(t));
                }

                return types;
            }

        }

        private static IReadOnlyList<Type> GetTypesFromAssembly(Predicate<Type> predicate, Assembly assembly)
        {
            Type[] typesInAsm;
            try
            {
                typesInAsm = assembly.GetTypes();
            }
            catch (ReflectionTypeLoadException ex)
            {
                typesInAsm = ex.Types.Where(t => t != null).ToArray();
            }

            if (typesInAsm == null)
            {
                return ImmutableList<Type>.Empty;
            }

            return typesInAsm.Where(t => t != null && predicate(t)).ToList();
        }
    }
}