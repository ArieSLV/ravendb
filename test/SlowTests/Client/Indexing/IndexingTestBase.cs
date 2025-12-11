using System;
using System.Linq;
using System.Runtime.CompilerServices;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Indexing;

public class IndexingTestBase : RavenTestBase
{
    public IndexingTestBase(ITestOutputHelper output) : base(output)
    {
    }

    #region Document Types

    private protected class DocWithStrings
    {
        public string Id { get; set; }
        public string[] Tags { get; set; }
        public string[] Categories { get; set; }
    }

    private protected class DocWithInts
    {
        public string Id { get; set; }
        public int[] Values { get; set; }
    }

    private protected class TagCount
    {
        public string Tag { get; set; }
        public int Count { get; set; }
    }

    private protected class CategoryCount
    {
        public string Category { get; set; }
        public int Count { get; set; }
    }

    private protected class DocWithDates
    {
        public string Id { get; set; }
        public DateTime[] ImportantDates { get; set; }
    }

    private protected class DocWithDoubles
    {
        public string Id { get; set; }
        public double[] Values { get; set; }
    }

    private protected class DocWithLongs
    {
        public string Id { get; set; }
        public long[] Values { get; set; }
        public int[] IntValues { get; set; }
        public ulong[] ULongValues { get; set; }
    }

    private protected class ItemWithTags
    {
        public string Id { get; set; }
        public string[] Tags { get; set; }
    }

    private protected class DocWithNestedArray
    {
        public string Id { get; set; }
        public ItemWithTags[] Items { get; set; }
    }

    private protected class DocWithFloats
    {
        public string Id { get; set; }
        public float[] Values { get; set; }
    }

    private protected class DocWithDecimals
    {
        public string Id { get; set; }
        public decimal[] Values { get; set; }
    }

    private protected class DocWithChars
    {
        public string Id { get; set; }
        public char[] Values { get; set; }
    }

    private protected class DocWithBools
    {
        public string Id { get; set; }
        public bool[] Values { get; set; }
    }

    #endregion

    #region Test Helpers

    private protected const string MemoryExtensionsMethodName = "MemoryExtensions";
    private protected const string ReadOnlySpanMethodName = "ReadOnlySpan";

    protected void AssertIndexBuilderRewritesAndRunsCorrectly<TDoc, TReduce>(
        Options options,
        IndexDefinitionBuilder<TDoc, TReduce> indexBuilder,
        object[] docs,
        [CallerMemberName] string indexName = null,
        Action<string> additionalMapAsserts = null,
        Action<string> additionalReduceAsserts = null,
        Action<IDocumentStore, string> additionalRunAsserts = null)
    {
        string map = null;
        string reduce = null;

        using (var store = GetDocumentStore(options))
        {
            var indexDefinition = indexBuilder.ToIndexDefinition(store.Conventions);
            indexDefinition.Name = indexName;

            if (indexDefinition.Maps.Count != 0)
            {
                map = indexDefinition.Maps.First();

                Assert.False(map.Contains(MemoryExtensionsMethodName), $"Map should not contain '{MemoryExtensionsMethodName}', but it is mapped to {map}");
                Assert.False(map.Contains(ReadOnlySpanMethodName), $"Map should not contain '{ReadOnlySpanMethodName}', but it is mapped to {map}");

                additionalMapAsserts?.Invoke(map);
            }

            if (string.IsNullOrEmpty(indexDefinition.Reduce) == false)
            {
                reduce = indexDefinition.Reduce;

                Assert.DoesNotContain(MemoryExtensionsMethodName, reduce);
                Assert.DoesNotContain(ReadOnlySpanMethodName, reduce);

                additionalReduceAsserts?.Invoke(reduce);
            }
        }

        AssertStringBasedIndexCompilesAndRuns(
            options,
            map,
            docs,
            reduce,
            indexName,
            additionalRunAsserts);
    }

    private void AssertStringBasedIndexCompilesAndRuns(
        Options options,
        string map,
        object[] docs = null,
        string reduce = null,
        [CallerMemberName] string indexName = null,
        Action<IDocumentStore, string> additionalAsserts = null)
    {
        using (var store = GetDocumentStore(options))
        {
            var indexDefinition = new IndexDefinition
            {
                Name = indexName,
                Maps = { map },
                Reduce = reduce
            };

            store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

            if (docs != null && docs.Length > 0)
            {
                using (var session = store.OpenSession())
                {
                    foreach (var doc in docs)
                    {
                        session.Store(doc);
                    }

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
            }

            WaitForUserToContinueTheTest(store);

            var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(indexName));
            Assert.Equal(0, indexStats.ErrorsCount);

            additionalAsserts?.Invoke(store, indexName);
        }
    }

    protected void AssertMapContains(string map, params string[] methods)
    {
        foreach (var method in methods)
        {
            Assert.True(map.Contains(method), $"Map expression does not contain '{method}', but should.{Environment.NewLine}Map expression:{Environment.NewLine}{map}");
        }
    }

    #endregion
}
