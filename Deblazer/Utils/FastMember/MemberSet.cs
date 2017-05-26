//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Reflection;
//// This class is copied from https://github.com/rasmus/fast-member/blob/master/FastMember/ObjectReader.cs
//// Some changes where necessary to support Id<T> otherwise BulkCopy does not work with typed Ids
//
//namespace Dg.Deblazer.Utils.FastMember
//{
//    /// <summary>
//    /// Represents an abstracted view of the members defined for a type
//    /// </summary>
//    public sealed class MemberSet : IEnumerable<Member>, IReadOnlyList<Member>
//    {
//        Member[] members;
//        internal MemberSet(Type type)
//        {
//            members = type.GetProperties()
//                .Cast<MemberInfo>()
//                .Concat(
//                    type.GetFields()
//                        .Cast<MemberInfo>())
//                .OrderBy(x => x.Name, StringComparer.InvariantCulture)
//                .Select(member => new Member(member))
//                .ToArray();
//        }
//
//        /// <summary>
//        /// Return a sequence of all defined members
//        /// </summary>
//        public IEnumerator<Member> GetEnumerator()
//        {
//            foreach (var member in members) yield return member;
//        }
//
//        /// <summary>
//        /// Get a member by index
//        /// </summary>
//        public Member this[int index]
//        {
//            get { return members[index]; }
//        }
//
//        /// <summary>
//        /// The number of members defined for this type
//        /// </summary>
//        public int Count { get { return members.Length; } }
//
//        Member IReadOnlyList<Member>.this[int index]
//        {
//            get { return members[index]; }
//        }
//
//        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() { return GetEnumerator(); }
//
//    }
//
//    /// <summary>
//    /// Represents an abstracted view of an individual member defined for a type
//    /// </summary>
//    public sealed class Member
//    {
//        private readonly MemberInfo member;
//
//        internal Member(MemberInfo member)
//        {
//            this.member = member;
//        }
//
//        /// <summary>
//        /// The name of this member
//        /// </summary>
//        public string Name { get { return member.Name; } }
//
//        /// <summary>
//        /// The type of value stored in this member
//        /// </summary>
//        public Type Type
//        {
//            get
//            {
//                var type = GetDefinedType();
//                // Special case Id<T>. We need to convert to int because SqlBulkCopy does not support typed ids.
//                if (type.IsGenericType
//                    && type.GetGenericTypeDefinition() == typeof(Nullable<>)
//                    && typeof(IConvertibleToInt32).IsAssignableFrom(type.GenericTypeArguments[0]))
//                {
//                    return typeof(int?);
//                }
//                else if (typeof(IConvertibleToInt32).IsAssignableFrom(type))
//                {
//                    return typeof(int);
//                }
//                return type;
//            }
//        }
//
//        private Type GetDefinedType()
//        {
//            switch (member.MemberType)
//            {
//                case MemberTypes.Field: return ((FieldInfo)member).FieldType;
//                case MemberTypes.Property: return ((PropertyInfo)member).PropertyType;
//                default: throw new NotSupportedException(member.MemberType.ToString());
//            }
//        }
//
//        /// <summary>
//        /// Is the attribute specified defined on this type
//        /// </summary>
//        public bool IsDefined(Type attributeType)
//        {
//            if (attributeType == null) throw new ArgumentNullException(nameof(attributeType));
//            return Attribute.IsDefined(member, attributeType);
//        }
//    }
//}