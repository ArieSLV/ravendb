using System;
using System.Linq;
using System.Runtime.CompilerServices;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using FastTests;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Indexing
{
    public class IndexDefinitionTests : RavenTestBase
    {
        public IndexDefinitionTests(ITestOutputHelper output) : base(output)
        {
        }

        #region Map Index Tests - Contains

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContains_StringArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, DocWithArray>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        HasTag = MemoryExtensions.Contains(doc.Tags, "csharp")
                    }
            };

            AssertIndexBuilderRewritesCorrectly(options, indexBuilder, additionalMapAsserts: map =>
                    Assert.True(map.Contains(nameof(Enumerable.Contains)), $"The map should have been rewritten to use '{nameof(Enumerable.Contains)}', but it did not. Map: '{map}'"));
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContains_IntArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, DocWithArray>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        HasNumber = MemoryExtensions.Contains(doc.Numbers, 42)
                    }
            };

            AssertIndexBuilderRewritesCorrectly(options, indexBuilder, additionalMapAsserts: map =>
                    Assert.True(map.Contains(nameof(Enumerable.Contains)), $"The map should have been rewritten to use '{nameof(Enumerable.Contains)}', but it did not. Map: '{map}'"));
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContains_MultipleFields_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, DocWithArray>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        HasTag = MemoryExtensions.Contains(doc.Tags, "csharp"),
                        HasCategory = MemoryExtensions.Contains(doc.Categories, "backend")
                    }
            };

            AssertIndexBuilderRewritesCorrectly(options, indexBuilder, additionalMapAsserts: map =>
                Assert.True(map.Contains(nameof(Enumerable.Contains)), $"The map should have been rewritten to use '{nameof(Enumerable.Contains)}', but it did not. Map: '{map}'"));
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContains_WithNegation_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, DocWithArray>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        DoesNotHaveTag = MemoryExtensions.Contains(doc.Tags, "deprecated") == false
                    }
            };

            AssertIndexBuilderRewritesCorrectly(options, indexBuilder, additionalMapAsserts: map =>
                Assert.True(map.Contains(nameof(Enumerable.Contains)), $"The map should have been rewritten to use '{nameof(Enumerable.Contains)}', but it did not. Map: '{map}'"));
        }

        #endregion

        #region Map Index Tests - ContainsAny

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContainsAny_IntArrays_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, DocWithArray>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        HasAnyNumber = MemoryExtensions.ContainsAny<int>(doc.Numbers, new int[] { 1, 2, 3, 42 })
                    }
            };

            AssertIndexBuilderRewritesCorrectly(options, indexBuilder, additionalMapAsserts: map =>
                {
                    var hasIntersectAny = map.Contains(nameof(Enumerable.Intersect)) && map.Contains(nameof(Enumerable.Any));
                    var hasContainsAny = map.Contains(nameof(MemoryExtensions.ContainsAny));
                    Assert.True(hasIntersectAny || hasContainsAny, $"Map should contain either '{nameof(Enumerable.Intersect)}'/'{nameof(Enumerable.Any)}' or '{nameof(MemoryExtensions.ContainsAny)}'. Map: '{map}'");
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContainsAny_StringArrays_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, DocWithArray>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        HasAnyTag = MemoryExtensions.ContainsAny<string>(doc.Tags, new string[] { "csharp", "dotnet", "ravendb" })
                    }
            };

            AssertIndexBuilderRewritesCorrectly(options, indexBuilder, additionalMapAsserts: map =>
                {
                    var hasIntersectAny = map.Contains(nameof(Enumerable.Intersect)) && map.Contains(nameof(Enumerable.Any));
                    var hasContainsAny = map.Contains(nameof(MemoryExtensions.ContainsAny));
                    Assert.True(hasIntersectAny || hasContainsAny, $"Map should contain either '{nameof(Enumerable.Intersect)}'/'{nameof(Enumerable.Any)}' or '{nameof(MemoryExtensions.ContainsAny)}'. Map: '{map}'");
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContainsAny_WithNegation_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, DocWithArray>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        IsValid = MemoryExtensions.ContainsAny<string>(doc.Tags, new string[] { "deprecated", "obsolete" }) == false
                    }
            };

            AssertIndexBuilderRewritesCorrectly(options, indexBuilder, additionalMapAsserts: map =>
                {
                    var hasIntersectAny = map.Contains(nameof(Enumerable.Intersect)) && map.Contains(nameof(Enumerable.Any));
                    var hasContainsAny = map.Contains(nameof(MemoryExtensions.ContainsAny));
                    Assert.True(hasIntersectAny || hasContainsAny, $"Map should contain either '{nameof(Enumerable.Intersect)}'/'{nameof(Enumerable.Any)}' or '{nameof(MemoryExtensions.ContainsAny)}'. Map: '{map}'");
                });
        }

        #endregion

        #region MapReduce Index Tests - Contains

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapReduceIndex_MemoryExtensionsContains_InMap_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, TagCount>
            {
                Map = docs => from doc in docs
                    where MemoryExtensions.Contains(doc.Tags, "csharp")
                    select new TagCount
                    {
                        Tag = "csharp",
                        Count = 1
                    },

                Reduce = results => from result in results
                    group result by result.Tag
                    into g
                    select new TagCount
                    {
                        Tag = g.Key,
                        Count = g.Sum(x => x.Count)
                    }
            };

            AssertIndexBuilderRewritesCorrectly(options, indexBuilder, additionalMapAsserts: map =>
                Assert.True(map.Contains(nameof(Enumerable.Contains)), $"The map should have been rewritten to use '{nameof(Enumerable.Contains)}', but it did not. Map: '{map}'"));
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapReduceIndex_MemoryExtensionsContains_InReduce_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, TagCount>
            {
                Map = docs => from doc in docs
                    from tag in doc.Tags
                    select new TagCount
                    {
                        Tag = tag,
                        Count = 1
                    },

                Reduce = results => from result in results
                    group result by result.Tag
                    into g
                    where MemoryExtensions.Contains(new string[] { "deprecated", "obsolete" }, (string)g.Key) == false
                    select new TagCount
                    {
                        Tag = g.Key,
                        Count = g.Sum(x => x.Count)
                    }
            };

            AssertIndexBuilderRewritesCorrectly(options, indexBuilder, additionalReduceAsserts: reduce =>
                Assert.True(reduce.Contains(nameof(Enumerable.Contains)), $"The reduce should have been rewritten to use '{nameof(Enumerable.Contains)}', but it did not. Reduce: '{reduce}'"));
        }

        #endregion

        #region MapReduce Index Tests - ContainsAny

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapReduceIndex_MemoryExtensionsContainsAny_InMap_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, TagCount>
            {
                Map = docs => from doc in docs
                    where MemoryExtensions.ContainsAny<string>(doc.Tags, new string[] { "csharp", "dotnet", "ravendb" })
                    select new TagCount
                    {
                        Tag = "important",
                        Count = 1
                    },

                Reduce = results => from result in results
                    group result by result.Tag
                    into g
                    select new TagCount
                    {
                        Tag = g.Key,
                        Count = g.Sum(x => x.Count)
                    }
            };

            AssertIndexBuilderRewritesCorrectly(options, indexBuilder, additionalMapAsserts: map =>
                {
                    var hasIntersectAny = map.Contains(nameof(Enumerable.Intersect)) && map.Contains(nameof(Enumerable.Any));
                    var hasContainsAny = map.Contains(nameof(MemoryExtensions.ContainsAny)); // For server-side compilation
                    Assert.True(hasIntersectAny || hasContainsAny, $"Map should contain either '{nameof(Enumerable.Intersect)}'/'{nameof(Enumerable.Any)}' or '{nameof(MemoryExtensions.ContainsAny)}'. Map: '{map}'");
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapReduceIndex_MemoryExtensionsContainsAny_InReduce_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, CategoryCount>
            {
                Map = docs => from doc in docs
                    from category in doc.Categories
                    select new CategoryCount
                    {
                        Category = category,
                        Count = 1
                    },

                Reduce = results => from result in results
                    group result by result.Category
                    into g
                    where MemoryExtensions.ContainsAny<string>(new string[] { "backend", "frontend", "database" }, new string[] { g.Key })
                    select new CategoryCount
                    {
                        Category = g.Key,
                        Count = g.Sum(x => x.Count)
                    }
            };

            AssertIndexBuilderRewritesCorrectly(options, indexBuilder,
                additionalReduceAsserts: reduce =>
                {
                    var hasIntersectAny = reduce.Contains(nameof(Enumerable.Intersect)) && reduce.Contains(nameof(Enumerable.Any));
                    var hasContainsAny = reduce.Contains(nameof(MemoryExtensions.ContainsAny)); // For server-side compilation
                    Assert.True(hasIntersectAny || hasContainsAny, $"Reduce should contain either '{nameof(Enumerable.Intersect)}'/'{nameof(Enumerable.Any)}' or '{nameof(MemoryExtensions.ContainsAny)}'. Reduce: '{reduce}'");
                });
        }

        #endregion

        #region Combined Scenarios

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MixedMemoryExtensionsCalls_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        HasTag = MemoryExtensions.Contains(doc.Tags, "csharp"),
                        HasAnyTag = MemoryExtensions.ContainsAny<string>(doc.Tags, new string[] { "csharp", "dotnet" }),
                        HasCategory = MemoryExtensions.Contains(doc.Categories, "backend")
                    }
            };

            AssertIndexBuilderRewritesCorrectly(options, indexBuilder);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapReduceIndex_MixedMemoryExtensionsCalls_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, TagCount>
            {
                Map = docs => from doc in docs
                    where MemoryExtensions.ContainsAny<string>(doc.Tags, new string[] { "important", "critical" })
                    from tag in doc.Tags
                    select new TagCount
                    {
                        Tag = tag,
                        Count = 1
                    },


                Reduce = results => from result in results
                    group result by result.Tag
                    into g
                    where MemoryExtensions.Contains(new string[] { "deprecated", "obsolete" }, (string)g.Key) == false
                    select new TagCount
                    {
                        Tag = g.Key,
                        Count = g.Sum(x => x.Count)
                    }
            };

            AssertIndexBuilderRewritesCorrectly(options, indexBuilder);
        }

        #endregion

        #region Type Coverage Tests - DateTime

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContains_DateTimeArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDates>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        HasDate = MemoryExtensions.Contains(doc.ImportantDates, new DateTime(2024, 1, 1))
                    }
            };

            AssertIndexBuilderRewritesCorrectly(options, indexBuilder);
        }

        #endregion

        #region Type Coverage Tests - Double

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContains_DoubleArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDoubles>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        HasValue = MemoryExtensions.Contains(doc.Values, 3.14)
                    }
            };

            AssertIndexBuilderRewritesCorrectly(options, indexBuilder);
        }

        #endregion

        #region Type Coverage Tests - Long

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContains_LongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        HasValue = MemoryExtensions.Contains(doc.Values, 9223372036854775807L)
                    }
            };

            AssertIndexBuilderRewritesCorrectly(options, indexBuilder);
        }

        #endregion

        #region Edge Cases Tests - Empty Array

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContains_EmptyArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        HasTag = MemoryExtensions.Contains(Array.Empty<string>(), "test")
                    }
            };

            AssertIndexBuilderRewritesCorrectly(options, indexBuilder);
        }

        #endregion

        #region Logical Operators Tests - OR

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensions_WithOrOperator_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        Match = MemoryExtensions.Contains(doc.Tags, "csharp") ||
                                MemoryExtensions.Contains(doc.Categories, "backend")
                    }
            };

            AssertIndexBuilderRewritesCorrectly(options, indexBuilder);
        }

        #endregion

        #region Logical Operators Tests - AND

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensions_WithAndOperator_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        Match = MemoryExtensions.Contains(doc.Tags, "csharp") &&
                                MemoryExtensions.ContainsAny<string>(doc.Categories, new string[] { "backend", "frontend" })
                    }
            };

            AssertIndexBuilderRewritesCorrectly(options, indexBuilder);
        }

        #endregion

        #region Let Clause Tests

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_WithLetClause_MemoryExtensionsContains_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray>
            {
                Map = docs => from doc in docs
                    let hasTag = MemoryExtensions.Contains(doc.Tags, "csharp")
                    where hasTag
                    select new
                    {
                        HasTag = hasTag
                    }
            };

            AssertIndexBuilderRewritesCorrectly(options, indexBuilder);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapReduceIndex_WithLetClauseInMap_MemoryExtensionsContains_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, TagCount>
            {
                Map = docs => from doc in docs
                    let hasTag = MemoryExtensions.Contains(doc.Tags, "csharp")
                    where hasTag
                    select new TagCount
                    {
                        Tag = "csharp",
                        Count = 1
                    },

                Reduce = results => from result in results
                    group result by result.Tag
                    into g
                    select new TagCount
                    {
                        Tag = g.Key,
                        Count = g.Sum(x => x.Count)
                    }
            };

            AssertIndexBuilderRewritesCorrectly(options, indexBuilder);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapReduceIndex_WithLetClauseInReduce_MemoryExtensionsContains_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithArray, TagCount>
            {
                Map = docs => from doc in docs
                    from tag in doc.Tags
                    select new TagCount
                    {
                        Tag = tag,
                        Count = 1
                    },

                Reduce = results => from result in results
                    group result by result.Tag
                    into g
                    let isDeprecated = MemoryExtensions.Contains(new string[] { "deprecated", "obsolete" }, (string)g.Key)
                    where isDeprecated == false
                    select new TagCount
                    {
                        Tag = g.Key,
                        Count = g.Sum(x => x.Count)
                    }
            };

            AssertIndexBuilderRewritesCorrectly(options, indexBuilder,
                additionalReduceAsserts: reduce => Assert.True(reduce.Contains(nameof(Enumerable.Contains)), $"The reduce should have been rewritten to use '{nameof(Enumerable.Contains)}', but it did not. Reduce: '{reduce}'"));
        }

        #endregion

        #region Nested Collections Tests

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensions_NestedCollections_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithNestedArray>
            {
                Map = docs => from doc in docs
                    from item in doc.Items
                    where MemoryExtensions.Contains(item.Tags, "important")
                    select new
                    {
                        ItemId = item.Id,
                        IsImportant = true
                    }
            };

            AssertIndexBuilderRewritesCorrectly(options, indexBuilder);
        }

        #endregion

        #region Document Types

        private class DocWithArray
        {
            public string[] Tags { get; set; }
            public string[] Categories { get; set; }
            public int[] Numbers { get; set; }
        }

        private class TagCount
        {
            public string Tag { get; set; }
            public int Count { get; set; }
        }

        private class CategoryCount
        {
            public string Category { get; set; }
            public int Count { get; set; }
        }

        private class DocWithDates
        {
            public string Id { get; set; }
            public DateTime[] ImportantDates { get; set; }
        }

        private class DocWithDoubles
        {
            public string Id { get; set; }
            public double[] Values { get; set; }
        }

        private class DocWithLongs
        {
            public string Id { get; set; }
            public long[] Values { get; set; }
        }

        private class ItemWithTags
        {
            public string Id { get; set; }
            public string[] Tags { get; set; }
        }

        private class DocWithNestedArray
        {
            public string Id { get; set; }
            public ItemWithTags[] Items { get; set; }
        }

        #endregion

        #region Test Helpers

        private const string MemoryExtensionsMethodName = "MemoryExtensions";
        private const string ReadOnlySpanMethodName = "ReadOnlySpan";

        private void AssertIndexBuilderRewritesCorrectly<TDoc, TReduce>(
            Options options,
            IndexDefinitionBuilder<TDoc, TReduce> indexBuilder,
            [CallerMemberName] string indexName = null,
            Action<string> additionalMapAsserts = null,
            Action<string> additionalReduceAsserts = null)
        {
            using (var store = GetDocumentStore(options))
            {
                var indexDefinition = indexBuilder.ToIndexDefinition(store.Conventions);
                indexDefinition.Name = indexName;

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                if (indexDefinition.Maps.Count != 0)
                {
                    var map = indexDefinition.Maps.First();
                    Assert.True(map.Contains(MemoryExtensionsMethodName) == false, $"Map should not contain '{MemoryExtensionsMethodName}'. Map: '{map}'");
                    Assert.True(map.Contains(ReadOnlySpanMethodName) == false, $"Map should not contain '{ReadOnlySpanMethodName}'. Map: '{map}'");

                    additionalMapAsserts?.Invoke(map);
                }

                if (string.IsNullOrEmpty(indexDefinition.Reduce) == false)
                {
                    var reduce = indexDefinition.Reduce;
                    Assert.True(reduce.Contains(MemoryExtensionsMethodName) == false, $"Reduce should not contain '{MemoryExtensionsMethodName}'. Reduce: '{reduce}'");
                    Assert.True(reduce.Contains(ReadOnlySpanMethodName) == false, $"Reduce should not contain '{ReadOnlySpanMethodName}'. Reduce: '{reduce}'");

                    additionalReduceAsserts?.Invoke(reduce);
                }
            }
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

                if (docs is { Length: > 0 })
                {
                    using (var session = store.OpenSession())
                    {
                        foreach (var doc in docs)
                            session.Store(doc);

                        session.SaveChanges();
                    }

                    Indexes.WaitForIndexing(store);

                    using (var session = store.OpenSession())
                    {
                        var results = session.Query<object>(indexName)
                            .Customize(x => x.WaitForNonStaleResults())
                            .ToList();

                        Assert.NotEmpty(results);
                    }
                }

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(indexName));
                Assert.Equal(0, indexStats.ErrorsCount);

                additionalAsserts?.Invoke(store, indexName);
            }
        }

        #endregion
    }
}
