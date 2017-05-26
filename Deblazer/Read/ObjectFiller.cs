using Dg.Deblazer.Extensions;
using JetBrains.Annotations;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Linq;
using System.Data.SqlTypes;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Xml.Linq;

namespace Dg.Deblazer.Read
{
    public class ObjectFiller
    {
        private static readonly Dictionary<DeserializerKey, Delegate> objectFillerDelegates = new Dictionary<DeserializerKey, Delegate>();
        private static readonly ReaderWriterLockSlim objectFillerDelegatesLock = new ReaderWriterLockSlim();

        private static readonly MethodInfo getValueNotDBNullMethod = new Func<object, object>(GetValueNotDbNull).Method;

        public object Build(Type inputType, IDataRecord dataRecord)
        {
            Delegate d = GetFiller(inputType, dataRecord);

            return d.DynamicInvoke(dataRecord);
        }
        

        [CanBeNull]
        private static object GetNullableId(object obj, Func<object, object> idContructor)
        {
            if (obj == null || obj == DBNull.Value)
            {
                return null;
            }

            return idContructor(obj);
        }

        private Delegate GetObjectFillerDelegate(Type type, IDataRecord reader)
        {
            var fieldCount = reader.FieldCount;
            var fieldNames = new string[fieldCount];
            var fieldTypes = new Type[fieldCount];
            var fieldDataTypeNames = new string[fieldCount];
            for (var i = 0; i < fieldCount; i++)
            {
                fieldNames[i] = reader.GetName(i);
                fieldTypes[i] = reader.GetFieldType(i);
                fieldDataTypeNames[i] = reader.GetDataTypeName(i);
            }

            var typeConstructor = FindConstructor(type, fieldNames, fieldTypes, fieldDataTypeNames);

            if (typeConstructor == null)
            {
                throw new NullReferenceException($"Could not find a type constructor for {type.Name}");
            }

            var getValueMethod = typeof(IDataRecord).GetMethod("GetValue");
            var constructorParameters = typeConstructor.GetParameters();
            if (constructorParameters.Length == 0)
            {
                return GetParameterlessObjectFillerDelegate(type, reader);
            }

            var argument = Expression.Parameter(typeof(IDataRecord), "source");
            var argumentsExpression = new Expression[constructorParameters.Length];
            for (var i = 0; i < constructorParameters.Length; i++)
            {
                var indexExpression = Expression.Constant(i);
                Expression loadExpression;
                var parameterType = constructorParameters[i].ParameterType;
                if (parameterType == typeof(SqlXml))
                {
                    // this may crash as it is SQL specific if IDataRecord is not a SqlDataRecord, we'll leave it here for the time being though to be able to unit test this method against IDataRecord...
                    loadExpression = Expression.Call(argument, DynamicLoadHelpers.GetSqlXmlMethod, indexExpression);
                }
                else if (parameterType == typeof(decimal))
                {
                    loadExpression = Expression.Call(DynamicLoadHelpers.GetDecimalMethodInfo, argument, indexExpression);
                }
                else if (parameterType == typeof(decimal?))
                {
                    loadExpression = Expression.Call(DynamicLoadHelpers.GetNullableDecimalMethodInfo, argument, indexExpression);
                }
                else if (parameterType == typeof(XElement))
                {
                    loadExpression = Expression.Call(argument, getValueMethod, indexExpression);
                    loadExpression = Expression.Call(DynamicLoadHelpers.LoadXElement, loadExpression);
                }
                else if (parameterType == typeof(Binary))
                {
                    loadExpression = Expression.Call(DynamicLoadHelpers.GetBinaryMethodInfo, argument, indexExpression);
                }
                else if (parameterType == typeof(char))
                {
                    loadExpression = Expression.Call(DynamicLoadHelpers.GetCharMathodInfo, argument, indexExpression);
                }
                else if (parameterType == typeof(char?))
                {
                    loadExpression = Expression.Call(DynamicLoadHelpers.GetNullableCharMethodInfo, argument, indexExpression);
                }
                else if (parameterType == typeof(Date))
                {
                    loadExpression = Expression.Call(DynamicLoadHelpers.GetDateMethodInfo, argument, indexExpression);
                }
                else if (parameterType == typeof(Date?))
                {
                    loadExpression = Expression.Call(DynamicLoadHelpers.GetNullableDateMethodInfo, argument, indexExpression);
                }
                else if (IdTypeExtension.IsIdType(parameterType))
                {
                    if (IdTypeExtension.IsNullableIdType(type)) // parameter comes from unboxing of nullable Id struct
                    {
                        var methodInfo = type.GetGenericArguments()[0].GetMethod("Nullable", new[] { typeof(int) });
                        loadExpression = Expression.Call(null, methodInfo,
                            Expression.Call(null, DynamicLoadHelpers.GetNullableIntMethodInfo, argument, Expression.Constant(i)));
                        var nullableIdLambda = Expression.Lambda(loadExpression, argument);
                        return nullableIdLambda.Compile();
                    }
                    else
                    {
                        loadExpression = Expression.Call(DynamicLoadHelpers.GetIdParameterMethodInfo, argument, indexExpression);
                        var constructor = parameterType.GetConstructor(new[] { typeof(int) });
                        loadExpression = Expression.New(constructor, loadExpression);
                    }

                }
                else if (IdTypeExtension.IsNullableIdType(parameterType))
                {
                    var methodInfo = parameterType.GetGenericArguments()[0].GetMethod("Nullable", new[] { typeof(int) });
                    loadExpression = Expression.Call(null, methodInfo,
                        Expression.Call(null, DynamicLoadHelpers.GetNullableIntMethodInfo, argument, Expression.Constant(i)));
                }
                else if (LongIdTypeExtension.IsLongIdType(parameterType))
                {
                    if (LongIdTypeExtension.IsNullableLongIdType(type)) // parameter comes from unboxing of nullable Id struct
                    {
                        var methodInfo = type.GetGenericArguments()[0].GetMethod("Nullable", new[] { typeof(long) });
                        loadExpression = Expression.Call(null, methodInfo,
                            Expression.Call(null, DynamicLoadHelpers.GetNullableLongMethodInfo, argument, Expression.Constant(i)));
                        var nullableIdLambda = Expression.Lambda(loadExpression, argument);
                        return nullableIdLambda.Compile();
                    }
                    else
                    {
                        loadExpression = Expression.Call(DynamicLoadHelpers.GetLongIdParameterMethodInfo, argument, indexExpression);
                        var constructor = parameterType.GetConstructor(new[] { typeof(long) });
                        loadExpression = Expression.New(constructor, loadExpression);
                    }

                }
                else if (LongIdTypeExtension.IsNullableLongIdType(parameterType))
                {
                    var methodInfo = parameterType.GetGenericArguments()[0].GetMethod("Nullable", new[] { typeof(long) });
                    loadExpression = Expression.Call(null, methodInfo, Expression.Call(null, DynamicLoadHelpers.GetNullableLongMethodInfo, argument, Expression.Constant(i)));
                }
                else
                {
                    var valueExpression = Expression.Call(argument, getValueMethod, indexExpression);
                    var valueNotNullExpression = Expression.Call(getValueNotDBNullMethod, valueExpression);
                    loadExpression = Expression.Convert(valueNotNullExpression, parameterType);
                }

                argumentsExpression[i] = loadExpression;
            }

            var newExpression = Expression.New(typeConstructor, argumentsExpression);

            var lambda =
                Expression.Lambda(newExpression, argument);

            return lambda.Compile();
        }

        private Func<IDataRecord, object> GetParameterlessObjectFillerDelegate(Type type, IDataRecord reader)
        {
            var dest = Expression.Variable(type, "dest");
            var body = new List<Expression>();
            body.Add(Expression.Assign(dest, Expression.New(type)));

            var getValueMethod = typeof(IDataRecord).GetMethod("GetValue");
            var propertyCount = 0;
            HashSet<string> processedFieldNames = new HashSet<string>();
            // If we fill SectorWithPortalAndCountry and join Name_DbContent, we have 2 times Id in the fields, but the second Id is the Id of the DbContent, not the one of the Sector
            var source = Expression.Variable(typeof(IDataRecord), "source");
            for (int i = 0; i < reader.FieldCount; i++)
            {
                string fieldName = reader.GetName(i).Replace(" ", ""); // Handle columns whose name contains a whitespace
                if (!processedFieldNames.Contains(fieldName))
                {
                    processedFieldNames.Add(fieldName);
                    PropertyInfo propertyInfo = type.GetProperty(
                        fieldName,
                        BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase | BindingFlags.SetField);

                    if (propertyInfo?.GetSetMethod() != null)
                    {
                        Expression getValue;
                        if (propertyInfo.PropertyType == typeof(SqlXml))
                        {
                            getValue = Expression.Call(source, DynamicLoadHelpers.GetSqlXmlMethod, Expression.Constant(i));
                        }
                        else if (propertyInfo.PropertyType == typeof(decimal))
                        {
                            getValue = Expression.Call(null, DynamicLoadHelpers.GetDecimalMethodInfo, source, Expression.Constant(i));
                        }
                        else if (propertyInfo.PropertyType == typeof(decimal?))
                        {
                            getValue = Expression.Call(null, DynamicLoadHelpers.GetNullableDecimalMethodInfo, source, Expression.Constant(i));
                        }
                        else if (IdTypeExtension.IsIdType(propertyInfo.PropertyType))
                        {
                            getValue = Expression.Call(DynamicLoadHelpers.GetIdParameterMethodInfo, source, Expression.Constant(i));
                            var constructor = propertyInfo.PropertyType.GetConstructor(new[] { typeof(int) });
                            getValue = Expression.New(constructor, getValue);
                        }
                        else if (IdTypeExtension.IsNullableIdType(propertyInfo.PropertyType))
                        {
                            var methodInfo = propertyInfo.PropertyType.GetGenericArguments()[0].GetMethod("Nullable", new[] { typeof(int?) });
                            getValue = Expression.Call(null, methodInfo, Expression.Call(null, DynamicLoadHelpers.GetNullableIntMethodInfo, source, Expression.Constant(i)));
                        }
                        else if (LongIdTypeExtension.IsLongIdType(propertyInfo.PropertyType))
                        {
                            getValue = Expression.Call(DynamicLoadHelpers.GetLongIdParameterMethodInfo, source, Expression.Constant(i));

                            var constructor = propertyInfo.PropertyType.GetConstructor(new[] { typeof(long) });
                            getValue = Expression.New(constructor, getValue);
                        }
                        else if (LongIdTypeExtension.IsNullableLongIdType(propertyInfo.PropertyType))
                        {
                            var methodInfo = propertyInfo.PropertyType.GetGenericArguments()[0].GetMethod("Nullable", new[] { typeof(long?) });
                            getValue = Expression.Call(null, methodInfo, Expression.Call(null, DynamicLoadHelpers.GetNullableLongMethodInfo, source, Expression.Constant(i)));
                        }
                        else
                        {
                            getValue = Expression.Call(source, getValueMethod, Expression.Constant(i));
                            if (propertyInfo.PropertyType == typeof(XElement))
                            {
                                getValue = Expression.Call(null, DynamicLoadHelpers.LoadXElement, getValue);
                            }
                            else if (propertyInfo.PropertyType == typeof(Binary))
                            {
                                // Need this to convert byte[] to Binary
                                getValue = Expression.Call(null, DynamicLoadHelpers.GetBinaryMethodInfo, source, Expression.Constant(i));
                            }
                            else if (!propertyInfo.PropertyType.IsValueType
                                     && propertyInfo.PropertyType != typeof(string))
                            {
                                // generator.Emit(OpCodes.Unbox_Any, dataRecord.GetFieldType(i));
                                // NL 05.01.2009: Use this line below instead of the one above; otherwise, when there comes an int from the dataRecord,
                                // but the target object expects an int?, this int? is then 0 ... don't know what this Unbox_Any does exactly... :-/
                                getValue = Expression.Unbox(getValue, propertyInfo.PropertyType);
                            }
                        }

                        if (propertyInfo.PropertyType == typeof(Date)
                            || propertyInfo.PropertyType == typeof(Date?))
                        {
                            getValue = Expression.Convert(getValue, typeof(DateTime?));
                        }

                        getValue = Expression.Convert(getValue, propertyInfo.PropertyType);
                        var assignment = Expression.Assign(Expression.Property(dest, propertyInfo), getValue);
                        var expression = Expression.IfThen(
                            Expression.IsFalse(
                                Expression.Call(source, DynamicLoadHelpers.IsDBNullMethodInfo, Expression.Constant(i))),
                            assignment);

                        body.Add(expression);

                        propertyCount++;
                    }
                }
            }

            body.Add(dest);

            var expr = Expression.Lambda<Func<IDataRecord, object>>(Expression.Block(new[] { dest }, body.ToArray()), source);

            return expr.Compile();
        }

        [CanBeNull]
        private ConstructorInfo FindConstructor(Type type, string[] fieldNames, Type[] fieldTypes, string[] fieldDataTypeNames)
        {
            var checkFieldNames = true;
            if (type.Name.StartsWith("Tuple`")
                || IdTypeExtension.IsIdType(type)
                || IdTypeExtension.IsNullableIdType(type)
                || LongIdTypeExtension.IsLongIdType(type)
                || LongIdTypeExtension.IsNullableLongIdType(type)
                || fieldNames[0].StartsWith("Item")) // do not check field names when they are all in auto generated as "Item1" "Item2" etc.
            {
                checkFieldNames = false;
            }

            var constructors = type.GetConstructors(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            foreach (var constructor in constructors.OrderBy(c => c.IsPublic ? 0 : (c.IsPrivate ? 2 : 1)).ThenBy(c => c.GetParameters().Length))
            {
                var constructorParameters = constructor.GetParameters();
                if (constructorParameters.Length == 0)
                {
                    return constructor;
                }

                if (constructorParameters.Length > fieldTypes.Length)
                {
                    continue;
                }

                int i = 0;
                for (; i < constructorParameters.Length; i++)
                {
                    // do not check argument names in case object is a Tuple, else break on name mismatch.
                    if (checkFieldNames
                        && !String.Equals(constructorParameters[i].Name, fieldNames[i], StringComparison.OrdinalIgnoreCase))
                    {
                        break;
                    }

                    if (fieldTypes[i] == typeof(byte[]) && constructorParameters[i].ParameterType.FullName == "System.Data.Linq.Binary")
                    {
                        continue;
                    }

                    var unboxedType = Nullable.GetUnderlyingType(constructorParameters[i].ParameterType) ?? constructorParameters[i].ParameterType;
                    // SqlDataReader converts the SQL Date type into DateTime by default, that means if we expect a System.Date it would not match 
                    if (fieldTypes[i] == typeof(DateTime)
                        && fieldDataTypeNames[i] == "date"
                        && unboxedType == typeof(Date))
                    {
                        continue;
                    }

                    if (IsTypeMismatch(fieldTypes[i], unboxedType))
                    {
                        break;
                    }
                }

                if (i == constructorParameters.Length)
                {
                    return constructor;
                }
            }

            return null;
        }

        private static bool IsTypeMismatch(Type fieldType, Type unboxedType)
        {
            return (unboxedType != fieldType)
                && !(unboxedType.IsEnum && Enum.GetUnderlyingType(unboxedType) == fieldType)
                && !(unboxedType == typeof(char) && fieldType == typeof(string))
                && !(unboxedType.IsEnum && fieldType == typeof(string))
                && !(IdTypeExtension.IsIdType(unboxedType) && (fieldType == typeof(int)))
                && !(IdTypeExtension.IsNullableIdType(unboxedType) && (fieldType == typeof(int?)))
                && !(LongIdTypeExtension.IsLongIdType(unboxedType) && fieldType == typeof(long))
                && !(LongIdTypeExtension.IsNullableLongIdType(unboxedType) && fieldType == typeof(long?));
        }

        private Delegate GetFiller(Type type, IDataRecord reader)
        {
            var length = reader.FieldCount;
            var columnNames = new string[length];
            var columnTypes = new Type[length];
            for (int i = 0; i < length; i++)
            {
                columnNames[i] = reader.GetName(i);
                columnTypes[i] = reader.GetFieldType(i);
            }

            var hash = GetColumnHash(columnNames, columnTypes, type);
            var key = new DeserializerKey(hash, length, columnNames, columnTypes);
            Delegate objectFillerDelegate;

            objectFillerDelegatesLock.EnterReadLock();
            try
            {
                if (objectFillerDelegates.TryGetValue(key, out objectFillerDelegate))
                {
                    return objectFillerDelegate;
                }
            }
            finally
            {
                objectFillerDelegatesLock.ExitReadLock();
            }

            objectFillerDelegate = GetObjectFillerDelegate(type, reader);
            objectFillerDelegatesLock.EnterWriteLock();
            try
            {
                objectFillerDelegates[key] = objectFillerDelegate;
            }
            finally
            {
                objectFillerDelegatesLock.ExitWriteLock();
            }

            return objectFillerDelegate;
        }

        private static int GetColumnHash(string[] names, Type[] types, Type targetType)
        {
            unchecked // Overflow is fine, just wrap
            {
                int max = names.Length;
                int hash = max + targetType.GetHashCode();
                for (int i = 0; i < max; i++)
                {
                    hash = -79 * ((hash * 31) + (names[i]?.GetHashCode() ?? 0)) + (types[i]?.GetHashCode() ?? 0);
                }

                return hash;
            }
        }

        private static object GetValueNotDbNull(object value) => value == DBNull.Value ? null : value;
    }
}
