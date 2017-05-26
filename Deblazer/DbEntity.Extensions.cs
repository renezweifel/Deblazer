using Dg.Deblazer.Utils;
using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Dg.Deblazer.Extensions;

namespace Dg.Deblazer
{
    public static class DbEntityExtensions
    {
        public static bool ValueWasAssigned<TEntity, TValue>(this TEntity entity, Expression<Func<TEntity, TValue>> memberExpression)
            where TEntity : DbEntity
            where TValue : struct
        {
            return entity.ValueWasAssignedPrivate(memberExpression);
        }

        public static bool ValueWasAssigned<TEntity, TValue>(this TEntity entity, Expression<Func<TEntity, TValue?>> memberExpression)
            where TEntity : DbEntity
            where TValue : struct
        {
            return entity.ValueWasAssignedPrivate(memberExpression);
        }

        public static bool ValueWasAssigned<TEntity>(this TEntity entity, Expression<Func<TEntity, string>> memberExpression)
            where TEntity : DbEntity
        {
            return entity.ValueWasAssignedPrivate(memberExpression);
        }

        private static bool ValueWasAssignedPrivate<TEntity, TValue>(this TEntity entity, Expression<Func<TEntity, TValue>> memberExpression)
            where TEntity : DbEntity
        {
            var memberName = memberExpression.GetMemberNames(useTypeNameForLastMember: false).Last();

            var dbValue = typeof(TEntity).GetField("_" + memberName, BindingFlags.NonPublic | BindingFlags.Instance).GetValue(entity) as IHaveAssignedValue;

            if (dbValue != null)
            {
                return dbValue.ValueWasAssigned();
            }

            return false;
        }
    }
}
