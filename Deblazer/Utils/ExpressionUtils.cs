using Dg.Deblazer.CodeAnnotation;
using JetBrains.Annotations;
using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Dg.Deblazer.Extensions;

namespace Dg.Deblazer.Utils
{
    public static class ExpressionUtils
    {
        public static int GetDbStringMaxLength<K>(this Expression<Func<K, string>> expression) where K : DbEntity
        {
            var memberExpression = expression.Body as MemberExpression;
            if (memberExpression == null)
            {
                throw new ArgumentException();
            }

            var memberName = memberExpression.GetMemberNames(false).Single();

            PropertyInfo[] pis = typeof(K).GetProperties(BindingFlags.Instance | BindingFlags.Public);
            var pi = pis.Single(p => p.Name == memberName);
            return GetDbStringMaxLength(pi) ?? int.MaxValue;
        }

        public static int? GetDbStringMaxLength(this Expression<Func<string>> expression)
        {
            var memberExpression = expression.Body as MemberExpression;
            if (memberExpression == null)
            {
                throw new ArgumentException();
            }

            var memberName = memberExpression.GetMemberNames(false).Last();

            PropertyInfo[] pis = null;
            pis = memberExpression.Member.DeclaringType.GetProperties(BindingFlags.Instance | BindingFlags.Public);
            var pi = pis.Single(p => p.Name == memberName);
            return GetDbStringMaxLength(pi);
        }

        [CanBeNull]
        private static int? GetDbStringMaxLength(PropertyInfo pi)
        {
            if (pi.IsDefined(typeof(StringColumnAttribute), false))
            {
                StringColumnAttribute attr = (StringColumnAttribute)pi.GetCustomAttributes(typeof(StringColumnAttribute), false)[0];
                return attr.MaxLength;
            }

            return null;
        }

      
    }
}