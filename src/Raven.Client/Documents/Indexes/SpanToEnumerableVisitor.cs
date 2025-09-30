using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Raven.Client.Documents.Indexes
{
    /// <summary>
    /// Rewrites Expression Trees to convert MemoryExtensions calls to Enumerable equivalents for RavenDB index compatibility.
    /// </summary>
    internal sealed class SpanToEnumerableVisitor : ExpressionVisitor
    {
        private static readonly SpanToEnumerableVisitor Instance = new();

        private static readonly MethodInfo ContainsSpan;
        private static readonly MethodInfo ContainsEnumerable;
#if NET8_0_OR_GREATER
        private static readonly MethodInfo ContainsAnySpan;
        private static readonly MethodInfo ContainsAnySpanVariant;
#endif

        private const string ImplicitOperatorMethodName = "op_Implicit";

        static SpanToEnumerableVisitor()
        {
            // Find MemoryExtensions.Contains<T>(ReadOnlySpan<T>, T)
            ContainsSpan = typeof(MemoryExtensions).GetMethods(BindingFlags.Public | BindingFlags.Static)
                .FirstOrDefault(m =>
                    m.Name == nameof(MemoryExtensions.Contains) &&
                    m.IsGenericMethodDefinition &&
                    m.GetParameters().Length == 2 &&
                    m.GetParameters()[0].ParameterType.IsGenericType &&
                    m.GetParameters()[0].ParameterType.GetGenericTypeDefinition() == typeof(ReadOnlySpan<>));

            // Find Enumerable.Contains<T>(IEnumerable<T>, T)
            ContainsEnumerable = typeof(Enumerable).GetMethods(BindingFlags.Public | BindingFlags.Static)
                .FirstOrDefault(m =>
                    m.Name == nameof(Enumerable.Contains) &&
                    m.IsGenericMethodDefinition &&
                    m.GetParameters().Length == 2 &&
                    m.GetParameters()[0].ParameterType.IsGenericType &&
                    m.GetParameters()[0].ParameterType.GetGenericTypeDefinition() == typeof(IEnumerable<>));

#if NET8_0_OR_GREATER
            // Find MemoryExtensions.ContainsAny(ReadOnlySpan, ReadOnlySpan)
            ContainsAnySpan = typeof(MemoryExtensions).GetMethods(BindingFlags.Public | BindingFlags.Static)
                .FirstOrDefault(m =>
                    m.Name == nameof(MemoryExtensions.ContainsAny) &&
                    m.IsGenericMethodDefinition &&
                    m.GetParameters().Length == 2 &&
                    m.GetParameters()[0].ParameterType.IsGenericType &&
                    m.GetParameters()[0].ParameterType.GetGenericTypeDefinition() == typeof(ReadOnlySpan<>) &&
                    m.GetParameters()[1].ParameterType.IsGenericType &&
                    m.GetParameters()[1].ParameterType.GetGenericTypeDefinition() == typeof(ReadOnlySpan<>));

            // Find MemoryExtensions.ContainsAny(Span, ReadOnlySpan)
            ContainsAnySpanVariant = typeof(MemoryExtensions).GetMethods(BindingFlags.Public | BindingFlags.Static)
                .FirstOrDefault(m =>
                    m.Name == nameof(MemoryExtensions.ContainsAny) &&
                    m.IsGenericMethodDefinition &&
                    m.GetParameters().Length == 2 &&
                    m.GetParameters()[0].ParameterType.IsGenericType &&
                    m.GetParameters()[0].ParameterType.GetGenericTypeDefinition() == typeof(Span<>) &&
                    m.GetParameters()[1].ParameterType.IsGenericType &&
                    m.GetParameters()[1].ParameterType.GetGenericTypeDefinition() == typeof(ReadOnlySpan<>));
#endif
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (TryRewriteContains(node, out var rewrittenContains))
                return rewrittenContains;

#if NET8_0_OR_GREATER
            if (TryRewriteContainsAny(node, out var rewrittenContainsAny))
                return rewrittenContainsAny;
#endif

            return base.VisitMethodCall(node);
        }

        /// <summary>
        /// Rewrites MemoryExtensions.Contains{T}(ReadOnlySpan{T}, T) to Enumerable.Contains{T}(IEnumerable{T}, T).
        /// </summary>
        private static bool TryRewriteContains(MethodCallExpression node, out Expression result)
        {
            result = null;

            if (ContainsEnumerable == null ||
                ContainsSpan == null ||
                node.Method is { IsGenericMethod: false } ||
                node.Method.GetGenericMethodDefinition() != ContainsSpan ||
                node.Arguments.Count != 2)
                return false;

            if (TryExtractArrayExpression(node.Arguments[0], out var arrayExpression) == false)
                return false;

            var genericType = node.Method.GetGenericArguments()[0];
            var containsMethod = ContainsEnumerable.MakeGenericMethod(genericType);
            result = Expression.Call(containsMethod, arrayExpression, node.Arguments[1]);
            return true;
        }

#if NET8_0_OR_GREATER
        /// <summary>
        /// Rewrites MemoryExtensions.ContainsAny{T}(ReadOnlySpan{T}, ReadOnlySpan{T}) to firstArray.Intersect(secondArray).Any().
        /// </summary>
        private static bool TryRewriteContainsAny(MethodCallExpression node, out Expression result)
        {
            result = null;

            if (node.Method is { IsGenericMethod: false } || node.Arguments.Count != 2)
                return false;

            var methodDef = node.Method.GetGenericMethodDefinition();

            if (ContainsAnySpan == null && ContainsAnySpanVariant == null)
                return false;

            if (methodDef != ContainsAnySpan && methodDef != ContainsAnySpanVariant)
                return false;

            if (TryExtractArrayExpression(node.Arguments[0], out var firstArray) == false ||
                TryExtractArrayExpression(node.Arguments[1], out var secondArray) == false)
                return false;

            var genericType = node.Method.GetGenericArguments()[0];

            // Build: firstArray.Intersect(secondArray).Any()
            var intersectMethod = typeof(Enumerable).GetMethod(
                nameof(Enumerable.Intersect),
                bindingAttr: BindingFlags.Public | BindingFlags.Static,
                binder: null,
                types: [
                    typeof(IEnumerable<>).MakeGenericType(Type.MakeGenericMethodParameter(0)),
                    typeof(IEnumerable<>).MakeGenericType(Type.MakeGenericMethodParameter(0))
                ],
                modifiers: null)?
                .MakeGenericMethod(genericType);

            if (intersectMethod == null)
                return false;

            var anyMethod = typeof(Enumerable).GetMethod(
                nameof(Enumerable.Any),
                bindingAttr: BindingFlags.Public | BindingFlags.Static,
                binder: null,
                types: [typeof(IEnumerable<>).MakeGenericType(Type.MakeGenericMethodParameter(0))],
                modifiers: null)?
                .MakeGenericMethod(genericType);

            if (anyMethod == null)
                return false;

            var intersectCall = Expression.Call(intersectMethod, firstArray, secondArray);
            result = Expression.Call(anyMethod, intersectCall);
            return true;
        }
#endif

        /// <summary>
        /// Extracts array expression, handling both legacy op_Implicit wrappers, UnaryExpression Convert nodes, and direct array expressions.
        /// </summary>
        private static bool TryExtractArrayExpression(Expression expression, out Expression arrayExpression)
        {
            // Unwrap UnaryExpression Convert nodes (C# 14 behavior for Span/ReadOnlySpan conversions)
            while (expression is UnaryExpression { NodeType: ExpressionType.Convert } convertExpr)
            {
                // Check if converting to Span<T> or ReadOnlySpan<T>
                if (convertExpr.Type.IsGenericType)
                {
                    var genericDef = convertExpr.Type.GetGenericTypeDefinition();
                    if (genericDef == typeof(Span<>) || genericDef == typeof(ReadOnlySpan<>))
                    {
                        expression = convertExpr.Operand;
                        continue;
                    }
                }
                break;
            }

            // Unwrap any implicit ReadOnlySpan conversions (legacy compiler behavior via op_Implicit)
            while (expression is MethodCallExpression implicitCall &&
                   IsImplicitConversionToReadOnlySpan(implicitCall.Method) &&
                   implicitCall.Arguments.Count == 1)
            {
                expression = implicitCall.Arguments[0];
            }

            // Check if the unwrapped expression is array-typed
            if (expression.Type.IsArray)
            {
                arrayExpression = expression;
                return true;
            }

            arrayExpression = null;
            return false;
        }


        private static bool IsImplicitConversionToReadOnlySpan(MethodInfo method)
        {
            if (method.IsSpecialName == false ||
                method.Name.Equals(ImplicitOperatorMethodName, StringComparison.Ordinal) == false)
                return false;

            var returnType = method.ReturnType;
            return returnType.IsGenericType &&
                   returnType.GetGenericTypeDefinition() == typeof(ReadOnlySpan<>);
        }

        /// <summary>
        /// Converts MemoryExtensions.Contains and ContainsAny calls to their Enumerable equivalents
        /// for compatibility with RavenDB index compilation.
        /// </summary>
        /// <typeparam name="T">The expression type</typeparam>
        /// <param name="expression">The expression to transform</param>
        /// <returns>Transformed expression, or the original if no transformation was needed or possible</returns>
        public static T Convert<T>(T expression) where T : Expression
        {
            if (expression == null)
                return null;

            if (ContainsSpan == null || ContainsEnumerable == null)
                return expression;

            return (T)Instance.Visit(expression);
        }
    }
}
