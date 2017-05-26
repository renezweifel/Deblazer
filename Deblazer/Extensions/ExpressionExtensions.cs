using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Dg.Deblazer.Extensions
{
    internal static class ExpressionExtensions
    {
        public static MethodInfo GetMethodInfo<K, T, V>(this Expression<Func<K, Func<T, V>>> action)
        {
            return GetMethodInfo(action.Body);
        }

        public static MethodInfo GetMethodInfo<K, V>(this Expression<Func<K, Func<V>>> action)
        {
            return GetMethodInfo(action.Body);
        }

        private static MethodInfo GetMethodInfo(Expression actionBody)
        {
            return (MethodInfo)((ConstantExpression)((MethodCallExpression)((UnaryExpression)actionBody)?.Operand)?.Object)?.Value;
        }

        public static string GetLastMemberName<T>(this Expression<Func<T>> expression)
        {
            return expression.GetMemberNames().Last();
        }

        public static List<string> GetMemberNames<T>(this Expression<Func<T>> expression)
        {
            return GetMemberNames(expression, true);
        }

        public static List<string> GetMemberNames<T>(this Expression<Func<T>> expression, bool useTypeNameForLastMember)
        {
            Expression nonUnaryExpression = expression.Body;
            while (nonUnaryExpression is UnaryExpression)
            {
                nonUnaryExpression = ((UnaryExpression)nonUnaryExpression).Operand;
            }

            var memberExpression = nonUnaryExpression as MemberExpression;

            return GetMemberNames(memberExpression, useTypeNameForLastMember);
        }

        public static List<string> GetMemberNames<T>(this Expression<Action<T>> expression)
        {
            return GetMemberNames(expression, true);
        }

        public static List<string> GetMemberNames<T>(this Expression<Action<T>> expression, bool useTypeNameForLastMember)
        {
            MethodCallExpression methodCallExpression = ((UnaryExpression)expression.Body).Operand as MethodCallExpression;
            while (methodCallExpression != null && methodCallExpression.Object as MethodCallExpression != null)
            {
                methodCallExpression = methodCallExpression.Object as MethodCallExpression;
            }

            MemberExpression memberExpression = (methodCallExpression != null ? methodCallExpression.Object : expression.Body) as MemberExpression;

            return GetMemberNames(memberExpression, useTypeNameForLastMember);
        }

        public static List<string> GetMemberNames<K, T>(this Expression<Func<K, T>> expression)
        {
            return GetMemberNames(expression, true);
        }

        public static List<string> GetMemberNames<K, T>(this Expression<Func<K, T>> expression, bool useTypeNameForLastMember)
        {
            if (expression.Body is ParameterExpression)
            {
                var memberNames = new List<string>();
                memberNames.Add(expression.Body.Type.Name);
                return memberNames;
            }

            var unaryExpression = expression.Body as UnaryExpression;
            if (unaryExpression != null)
            {
                var methodCallEx = unaryExpression.Operand as MethodCallExpression;
                if (methodCallEx != null)
                {
                    var memberNames = new List<string>();
                    string actionName = ((MethodInfo)((ConstantExpression)methodCallEx.Object).Value).Name;
                    memberNames.Add(typeof(K).Name);
                    memberNames.Add(actionName);
                    return memberNames;
                }

                var memberEx = unaryExpression.Operand as MemberExpression;
                if (memberEx != null)
                {
                    // This is the case if you have an expression like 
                    // foo => (Id<Foo>)foo.MyInteger 
                    // or 
                    // foo => -foo.MyInteger
                    return GetMemberNamesFromMemberExpression<K>(useTypeNameForLastMember, memberEx);
                }

                throw new NotImplementedException("This kind of expression is not handled in the framework.");
            }
            else
            {
                MethodCallExpression methodCallExpression = expression.Body as MethodCallExpression;
                while (methodCallExpression != null && methodCallExpression.Object as MethodCallExpression != null)
                {
                    methodCallExpression = methodCallExpression.Object as MethodCallExpression;
                }

                MemberExpression memberExpression = expression.Body as MemberExpression;
                if (memberExpression == null)
                {
                    memberExpression = methodCallExpression.Object as MemberExpression;
                }

                return GetMemberNamesFromMemberExpression<K>(useTypeNameForLastMember, memberExpression);
            }
        }

        private static List<string> GetMemberNamesFromMemberExpression<K>(bool useTypeNameForLastMember, MemberExpression memberExpression)
        {
            var memberNames = GetMemberNames(memberExpression, useTypeNameForLastMember);

            memberNames.Insert(0, memberNames.Count > 0 ? typeof(K).Name : memberExpression.Member.Name);
            return memberNames;
        }

        public static List<string> GetMemberNames(this MemberExpression memberExpression, bool useTypeNameForLastMember)
        {
            var originalMemberExpression = memberExpression;
            var memberNames = new List<string>();
            string postfix = "";
            while (memberExpression != null)
            {
                var subMemberExpression = memberExpression.Expression as MemberExpression;

                if ((!useTypeNameForLastMember
                // Ignore compiler generated types that are generated in cases like this one to capture the htmlAttributes in a closure
                // object htmlAttributes = new
                // {
                //     OnChange = "$(this).ToggleNewsletterTopicsCheckBoxes();"
                // };
                //Html.Write(Html.BoxCell(BoxCellWidth.Third, () => Html.CheckBox(() => ViewModel.UserWantsNewsletters, () => ViewModel.UserWantsNewsletters, htmlAttributes: htmlAttributes)));
                     || (subMemberExpression != null && !subMemberExpression.Member.Name.StartsWith("<>"))
                     || typeof(IEnumerable).IsAssignableFrom(memberExpression.Type)))
                {
                    memberNames.Insert(0, memberExpression.Member.Name + postfix);
                }
                else
                {
                    memberNames.Insert(0, memberExpression.Type.Name + postfix);
                    break;
                }

                postfix = "";

                if (memberExpression.Expression is MethodCallExpression)
                {
                    MethodCallExpression methodCallExpression = memberExpression.Expression as MethodCallExpression;

                    if (methodCallExpression.Method.Name == "get_Item")
                    {
                        postfix = /*"[" + */ methodCallExpression.Arguments[0] /* + "]"*/+ "";
                    }

                    memberExpression = methodCallExpression.Object as MemberExpression;
                }
                else
                {
                    memberExpression = subMemberExpression;
                }
            }

            // We do not want compiler generated stuff like "<>c__DisplayClass10" in the memberNames
            // This happens in one case with the preview of the new compliler and might only be a compiler bug
            // and can be removed in the future (see the Test GetInputName_InThisObscureCase_RoslynGeneratesSomethingElse)
            if (memberNames.Any(s => s.StartsWith("<")))
            {
                return GetMemberNamesWithoutCompilerGeneratedCode(originalMemberExpression, useTypeNameForLastMember);
            }

            return memberNames;
        }

        /// <summary>
        /// Temporary method (probably) to work around a bug in the preview of the new Roslyn compiler. Can hopefully be removed in the future
        /// </summary>
        private static List<string> GetMemberNamesWithoutCompilerGeneratedCode(MemberExpression memberExpression, bool useTypeNameForLastMember)
        {
            var memberNames = new List<string>();
            string postfix = "";
            while (memberExpression != null)
            {
                if ((useTypeNameForLastMember == false
                     || memberExpression.Expression is MemberExpression
                     || typeof(IEnumerable).IsAssignableFrom(memberExpression.Type))
                    && !memberExpression.Expression.Type.IsCompilerGenerated())
                {
                    memberNames.Insert(0, memberExpression.Member.Name + postfix);
                }
                else
                {
                    memberNames.Insert(0, memberExpression.Type.Name + postfix);
                    break;
                }

                postfix = "";

                if (memberExpression.Expression is MethodCallExpression)
                {
                    MethodCallExpression methodCallExpression = memberExpression.Expression as MethodCallExpression;

                    if (methodCallExpression.Method.Name == "get_Item")
                    {
                        postfix = /*"[" + */ methodCallExpression.Arguments[0] /* + "]"*/+ "";
                    }

                    memberExpression = methodCallExpression.Object as MemberExpression;
                }
                else
                {
                    memberExpression = memberExpression.Expression as MemberExpression;
                }
            }

            return memberNames;
        }
        private static bool IsCompilerGenerated(this Type type)
        {
            if (type == null)
            {
                throw new ArgumentNullException("type");
            }

            // Compiler generated classes have names like "<>c__DisplayClass0"
            return type.FullName.Contains('<');
        }
    }
}
