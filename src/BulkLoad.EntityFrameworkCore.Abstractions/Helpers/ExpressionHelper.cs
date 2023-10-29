using System.Linq.Expressions;
using System.Reflection;

namespace BulkLoad.EntityFrameworkCore.Abstractions.Helpers;

internal static class ExpressionHelper
{
    public static PropertyInfo GetPropertyInfo<T, TReturn>(Expression<Func<T, TReturn>> property)
    {
        LambdaExpression lambda = property;
        var memberExpression = lambda.Body is UnaryExpression expression
            ? (MemberExpression)expression.Operand
            : (MemberExpression)lambda.Body;

        return (PropertyInfo)memberExpression.Member;
    }

    public static string GetPropertyName<T, TReturn>(Expression<Func<T, TReturn>> property)
        => GetPropertyInfo(property).Name;
}