using System;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    /// <summary>
    /// Rewrites MemoryExtensions.Contains and MemoryExtensions.ContainsAny calls to DynamicArray equivalents
    /// to avoid Span-related compilation issues in dynamically compiled indexes.
    /// </summary>
    public sealed class MemoryExtensionsRewriter : CSharpSyntaxRewriter
    {
        public static readonly MemoryExtensionsRewriter Instance = new();

        private const string ContainsMethodName = nameof(MemoryExtensions.Contains);
        private const string ContainsAnyMethodName = nameof(MemoryExtensions.ContainsAny);
        private const string SpanPrefix = "Span<";
        private const string ReadOnlySpanPrefix = "ReadOnlySpan<";

        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            if (node.Expression is MemberAccessExpressionSyntax memberAccess)
            {
                var methodName = memberAccess.Name.ToString();

                if (methodName.StartsWith(ContainsAnyMethodName))
                    return RewriteContainsAny(node);

                if (methodName.StartsWith(ContainsMethodName))
                    return RewriteContains(node);
            }

            return base.VisitInvocationExpression(node);
        }

        private InvocationExpressionSyntax RewriteContains(InvocationExpressionSyntax node)
        {
            var arguments = node.ArgumentList.Arguments;
            if (arguments.Count != 2)
                return node;

            var arrayArg = UnwrapConversions(arguments[0].Expression);
            var valueArg = arguments[1].Expression;

            // Build: (new DynamicArray(arrayArg)).Contains(valueArg)
            var dynamicArrayCreation = CreateDynamicArrayInstantiation(arrayArg);

            var containsMemberAccess = SyntaxFactory.MemberAccessExpression(
                SyntaxKind.SimpleMemberAccessExpression,
                SyntaxFactory.ParenthesizedExpression(dynamicArrayCreation),
                SyntaxFactory.IdentifierName(ContainsMethodName));

            var argumentList = SyntaxFactory.ArgumentList(
                SyntaxFactory.SingletonSeparatedList(
                    SyntaxFactory.Argument(valueArg)));

            return SyntaxFactory.InvocationExpression(containsMemberAccess, argumentList);
        }

        private InvocationExpressionSyntax RewriteContainsAny(InvocationExpressionSyntax node)
        {
            var arguments = node.ArgumentList.Arguments;
            if (arguments.Count != 2)
                return node;

            var firstArrayArg = UnwrapConversions(arguments[0].Expression);
            var secondArrayArg = UnwrapConversions(arguments[1].Expression);

            // Build: (new DynamicArray(firstArrayArg)).ContainsAny(secondArrayArg)
            var dynamicArrayCreation = CreateDynamicArrayInstantiation(firstArrayArg);

            var containsAnyMemberAccess = SyntaxFactory.MemberAccessExpression(
                SyntaxKind.SimpleMemberAccessExpression,
                SyntaxFactory.ParenthesizedExpression(dynamicArrayCreation),
                SyntaxFactory.IdentifierName(ContainsAnyMethodName));

            var argumentList = SyntaxFactory.ArgumentList(
                SyntaxFactory.SingletonSeparatedList(
                    SyntaxFactory.Argument(secondArrayArg)));

            return SyntaxFactory.InvocationExpression(containsAnyMemberAccess, argumentList);
        }

        private ObjectCreationExpressionSyntax CreateDynamicArrayInstantiation(ExpressionSyntax arrayExpression)
        {
            var dynamicArrayType = SyntaxFactory.IdentifierName(nameof(DynamicArray));

            var argumentList = SyntaxFactory.ArgumentList(
                SyntaxFactory.SingletonSeparatedList(
                    SyntaxFactory.Argument(arrayExpression)));

            return SyntaxFactory.ObjectCreationExpression(dynamicArrayType)
                .WithArgumentList(argumentList);
        }

        private static ExpressionSyntax UnwrapConversions(ExpressionSyntax expression)
        {
            while (expression is CastExpressionSyntax castExpr)
            {
                var castType = castExpr.Type.ToString();

                if (castType.Contains(SpanPrefix) || castType.Contains(ReadOnlySpanPrefix))
                    expression = castExpr.Expression;
                else
                    break;
            }

            return expression;
        }
    }
}
