using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using System.Linq;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public sealed class DynamicInvocationExpressionsRewriter : CSharpSyntaxRewriter
    {
        public static readonly DynamicInvocationExpressionsRewriter Instance = new DynamicInvocationExpressionsRewriter();

        private DynamicInvocationExpressionsRewriter()
        {
        }

        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            var expression = node.Expression.ToString();
            switch (expression)
            {
                case "Enumerable.Range":
                    return HandleEnumerableRange(node);
                case "Enumerable.Distinct":
                    return HandleEnumerableDistinct(node);
                case "Enumerable.Count":
                    return HandleEnumerableCount(node);
                case "Enumerable.Contains":
                    return HandleEnumerableContains(node);
                case "Enumerable.Any":
                case "Enumerable.All":
                    return HandleEnumerableAnyOrAll(node);
            }

            return base.VisitInvocationExpression(node);
        }

        private SyntaxNode HandleEnumerableContains(InvocationExpressionSyntax node)
        {
            if (node.ArgumentList.Arguments.Count != 2)
                return base.VisitInvocationExpression(node);

            var collectionArgument = node.ArgumentList.Arguments[0].Expression;
            var typeToCast = FindExplicitTypeInChain(collectionArgument);

            if (typeToCast == null)
                return base.VisitInvocationExpression(node);

            var valueArgument = node.ArgumentList.Arguments[1];
            var castedExpression = SyntaxFactory.ParseExpression($"({typeToCast})({valueArgument.Expression})");

            var newArguments = node.ArgumentList.Arguments.Replace(valueArgument, valueArgument.WithExpression(castedExpression));

            return node.WithArgumentList(node.ArgumentList.WithArguments(newArguments));
        }

        private static TypeSyntax FindExplicitTypeInChain(ExpressionSyntax expression)
        {
            var typeFromCreation = GetTypeFromCreation(expression);
            if (typeFromCreation != null)
                return typeFromCreation;

            switch (expression)
            {
                case InvocationExpressionSyntax invocation:
                    var methodName = GetMethodName(invocation);

                    if (methodName is "Enumerable.Cast" or "Enumerable.OfType")
                    {
                        if (invocation.Expression is MemberAccessExpressionSyntax memberAccess &&
                            memberAccess.Name is GenericNameSyntax genericName &&
                            genericName.TypeArgumentList.Arguments.Count == 1)
                        {
                            return genericName.TypeArgumentList.Arguments[0];
                        }
                    }

                    if (IsSetOperation(methodName))
                    {
                        if (invocation.ArgumentList.Arguments.Count == 2)
                        {
                            var rightSideType = FindExplicitTypeInChain(invocation.ArgumentList.Arguments[1].Expression);
                            if (rightSideType != null)
                                return rightSideType;

                            var leftSideType = FindExplicitTypeInChain(invocation.ArgumentList.Arguments[0].Expression);
                            if (leftSideType != null)
                                return leftSideType;
                        }
                    }

                    break;

                case ParenthesizedExpressionSyntax parenthesized:
                    return FindExplicitTypeInChain(parenthesized.Expression);

                case CastExpressionSyntax castExpression when castExpression.Type is ArrayTypeSyntax arrayType:
                    return arrayType.ElementType;
            }

            return null;
        }

        private static string GetMethodName(InvocationExpressionSyntax invocation)
        {
            return invocation.Expression.ToString();
        }

        private static bool IsSetOperation(string methodName)
        {
            switch (methodName)
            {
                case "Enumerable.Except":
                case "Enumerable.Intersect":
                case "Enumerable.Union":
                case "Enumerable.Concat":
                    return true;
                default:
                    return false;
            }
        }

        private static TypeSyntax GetTypeFromCreation(ExpressionSyntax expression)
        {
            if (expression is ArrayCreationExpressionSyntax array)
            {
                return array.Type.ElementType;
            }

            if (expression is ObjectCreationExpressionSyntax obj)
            {
                var name = obj.Type as GenericNameSyntax;
                if (name == null && obj.Type is QualifiedNameSyntax qualifiedName)
                    name = qualifiedName.Right as GenericNameSyntax;

                if (name != null)
                {
                    var arguments = name.TypeArgumentList.Arguments;
                    if (arguments.Count == 1)
                        return arguments[0];
                }
            }

            return null;
        }

        private SyntaxNode HandleEnumerableCount(InvocationExpressionSyntax node)
        {
            if (node.ArgumentList.Arguments.Count != 1)
                return node;
            var n = node.WithArgumentList(SyntaxFactory.ParseArgumentList($"((IEnumerable<dynamic>){node.ArgumentList})"));
            return n;
        }

        private SyntaxNode HandleEnumerableAnyOrAll(InvocationExpressionSyntax node)
        {
            if (node.ArgumentList.Arguments.Count != 1)
                return node;

            var n = node.WithArgumentList(SyntaxFactory.ParseArgumentList($"((IEnumerable<dynamic>){node.ArgumentList})"));
            return n;
        }

        private SyntaxNode HandleEnumerableDistinct(InvocationExpressionSyntax node)
        {
            return SyntaxFactory.ParseExpression($"((IEnumerable<dynamic>){node})");
        }

        private SyntaxNode HandleEnumerableRange(InvocationExpressionSyntax node)
        {
            var parentMethod = GetParentMethod(node);
            switch (parentMethod)
            {
                case "Select":
                case "SelectMany":
                case "Enumerable.ToDictionary":
                    return SyntaxFactory.ParseExpression($"{node}.Cast<dynamic>()");
            }

            return base.VisitInvocationExpression(node);
        }

        private static string GetParentMethod(InvocationExpressionSyntax currentInvocation)
        {
            var member = currentInvocation.Parent as MemberAccessExpressionSyntax;
            if (member != null)
                return member.Name.Identifier.Text;

            var argument = GetArgument(currentInvocation);
            if (argument == null)
                return null;

            var argumentList = argument.Parent as ArgumentListSyntax;
            if (argumentList == null)
                return null;

            var invocation = argumentList.Parent as InvocationExpressionSyntax;
            if (invocation == null)
                return null;

            member = invocation.Expression as MemberAccessExpressionSyntax;
            if (member == null)
                return null;

            return member.Name.Identifier.Text;

            static ArgumentSyntax GetArgument(InvocationExpressionSyntax node)
            {
                var parent = node.Parent;

                if (parent is ArgumentSyntax a)
                    return a;

                if (parent is CastExpressionSyntax ces)
                    parent = ces.Parent; // unwrapping

                var e = parent as SimpleLambdaExpressionSyntax;
                return e?.Parent as ArgumentSyntax;
            }
        }
    }
}
