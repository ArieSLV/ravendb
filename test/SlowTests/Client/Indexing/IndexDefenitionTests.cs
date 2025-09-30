using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
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

        // ReSharper disable once ClassNeverInstantiated.Local
        private class DocWithArray
        {
            public string Id { get; set; }
            public string[] Tags { get; set; }
            public string[] Categories { get; set; }
            public int[] Numbers { get; set; }
        }

        #region Map Index Tests - Contains

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContains_StringArray_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                // Arrange
                var indexBuilder = new IndexDefinitionBuilder<DocWithArray>
                {
                    Map = docs => from doc in docs
                        select new
                        {
                            // ReSharper disable once InvokeAsExtensionMethod
                            HasTag = MemoryExtensions.Contains(doc.Tags, "csharp")
                        }
                };

                var indexDefinition = indexBuilder.ToIndexDefinition(store.Conventions);
                indexDefinition.Name = "DocsByTag";

                // Act & Assert
                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                var map = indexDefinition.Maps.First();
                Assert.DoesNotContain("MemoryExtensions", map);
                Assert.DoesNotContain("ReadOnlySpan", map);
                Assert.Contains("Contains", map);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContains_IntArray_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                // Arrange
                var indexBuilder = new IndexDefinitionBuilder<DocWithArray>
                {
                    Map = docs => from doc in docs
                        select new
                        {
                            // ReSharper disable once InvokeAsExtensionMethod
                            HasNumber = MemoryExtensions.Contains(doc.Numbers, 42)
                        }
                };

                var indexDefinition = indexBuilder.ToIndexDefinition(store.Conventions);
                indexDefinition.Name = "DocsByNumber";

                // Act & Assert
                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                var map = indexDefinition.Maps.First();
                Assert.DoesNotContain("MemoryExtensions", map);
                Assert.DoesNotContain("ReadOnlySpan", map);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContains_MultipleFields_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                // Arrange
                var indexBuilder = new IndexDefinitionBuilder<DocWithArray>
                {
                    Map = docs => from doc in docs
                        select new
                        {
                            // ReSharper disable once InvokeAsExtensionMethod
                            HasTag = MemoryExtensions.Contains(doc.Tags, "csharp"),
                            // ReSharper disable once InvokeAsExtensionMethod
                            HasCategory = MemoryExtensions.Contains(doc.Categories, "backend")
                        }
                };

                var indexDefinition = indexBuilder.ToIndexDefinition(store.Conventions);
                indexDefinition.Name = "DocsByTagAndCategory";

                // Act & Assert
                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                var map = indexDefinition.Maps.First();
                Assert.DoesNotContain("MemoryExtensions", map);
                Assert.DoesNotContain("ReadOnlySpan", map);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContains_WithNegation_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                // Arrange
                var indexBuilder = new IndexDefinitionBuilder<DocWithArray>
                {
                    Map = docs => from doc in docs
                        select new
                        {
                            // ReSharper disable once InvokeAsExtensionMethod
                            DoesNotHaveTag = MemoryExtensions.Contains(doc.Tags, "deprecated") == false
                        }
                };

                var indexDefinition = indexBuilder.ToIndexDefinition(store.Conventions);
                indexDefinition.Name = "DocsWithoutDeprecatedTag";

                // Act & Assert
                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                var map = indexDefinition.Maps.First();
                Assert.DoesNotContain("MemoryExtensions", map);
                Assert.DoesNotContain("ReadOnlySpan", map);
            }
        }

        #endregion

        #region Map Index Tests - ContainsAny

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContainsAny_IntArrays_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                // Arrange
                var indexBuilder = new IndexDefinitionBuilder<DocWithArray>
                {
                    Map = docs => from doc in docs
                        select new
                        {
                            // ReSharper disable once InvokeAsExtensionMethod
                            HasAnyNumber = MemoryExtensions.ContainsAny<int>(doc.Numbers, new int[] { 1, 2, 3, 42 })
                        }
                };

                var indexDefinition = indexBuilder.ToIndexDefinition(store.Conventions);
                indexDefinition.Name = "DocsByAnyNumber";

                // Act & Assert
                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                var map = indexDefinition.Maps.First();
                Assert.False(map.Contains("MemoryExtensions"), $"Map: {map} should not contain 'MemoryExtensions', but it does");
                Assert.False(map.Contains("ReadOnlySpan"), $"Map: {map} should not contain 'ReadOnlySpan', but it does");

                bool hasIntersectAny = map.Contains("Intersect") && map.Contains("Any");
                bool hasContainsAny = map.Contains("ContainsAny");
                Assert.True(hasIntersectAny || hasContainsAny, $"Map: {map} should contain either 'Intersect + Any' or 'ContainsAny' pattern");
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContainsAny_StringArrays_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                // Arrange
                var indexBuilder = new IndexDefinitionBuilder<DocWithArray>
                {
                    Map = docs => from doc in docs
                        select new
                        {
                            // ReSharper disable once InvokeAsExtensionMethod
                            HasAnyTag = MemoryExtensions.ContainsAny<string>(doc.Tags, new string[] { "csharp", "dotnet", "ravendb" })
                        }
                };

                var indexDefinition = indexBuilder.ToIndexDefinition(store.Conventions);
                indexDefinition.Name = "DocsByAnyTag";

                // Act & Assert
                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                var map = indexDefinition.Maps.First();
                Assert.False(map.Contains("MemoryExtensions"), $"Map: {map} should not contain 'MemoryExtensions', but it does");
                Assert.False(map.Contains("ReadOnlySpan"), $"Map: {map} should not contain 'ReadOnlySpan', but it does");

                bool hasIntersectAny = map.Contains("Intersect") && map.Contains("Any");
                bool hasContainsAny = map.Contains("ContainsAny");
                Assert.True(hasIntersectAny || hasContainsAny, $"Map: {map} should contain either 'Intersect + Any' or 'ContainsAny' pattern");
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContainsAny_WithNegation_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                // Arrange
                var indexBuilder = new IndexDefinitionBuilder<DocWithArray>
                {
                    Map = docs => from doc in docs
                        select new
                        {
                            // ReSharper disable once InvokeAsExtensionMethod
                            IsValid = MemoryExtensions.ContainsAny<string>(doc.Tags, new string[] { "deprecated", "obsolete" }) == false
                        }
                };

                var indexDefinition = indexBuilder.ToIndexDefinition(store.Conventions);
                indexDefinition.Name = "ValidDocs";

                // Act & Assert
                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                var map = indexDefinition.Maps.First();
                Assert.DoesNotContain("MemoryExtensions", map);
                Assert.DoesNotContain("ReadOnlySpan", map);
            }
        }

        #endregion

        #region MapReduce Index Tests - Contains

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapReduceIndex_MemoryExtensionsContains_InMap_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                // Arrange
                var indexBuilder = new IndexDefinitionBuilder<DocWithArray, TagCount>
                {
                    Map = docs => from doc in docs
                        // ReSharper disable once InvokeAsExtensionMethod
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

                var indexDefinition = indexBuilder.ToIndexDefinition(store.Conventions);
                indexDefinition.Name = "TagCounts";

                // Act & Assert
                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                var map = indexDefinition.Maps.First();
                Assert.DoesNotContain("MemoryExtensions", map);
                Assert.DoesNotContain("ReadOnlySpan", map);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapReduceIndex_MemoryExtensionsContains_InReduce_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
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
                        // ReSharper disable once InvokeAsExtensionMethod
                        where MemoryExtensions.Contains(new string[] { "deprecated", "obsolete" }, (string)g.Key) == false
                        select new TagCount
                        {
                            Tag = g.Key,
                            Count = g.Sum(x => x.Count)
                        }
                };

                var indexDefinition = indexBuilder.ToIndexDefinition(store.Conventions);
                indexDefinition.Name = "ActiveTagCounts";

                // Act & Assert
                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                var reduce = indexDefinition.Reduce;
                Assert.DoesNotContain("MemoryExtensions", reduce);
                Assert.DoesNotContain("ReadOnlySpan", reduce);
            }
        }

        #endregion

        #region MapReduce Index Tests - ContainsAny

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapReduceIndex_MemoryExtensionsContainsAny_InMap_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                // Arrange
                var indexBuilder = new IndexDefinitionBuilder<DocWithArray, TagCount>
                {
                    Map = docs => from doc in docs
                        // ReSharper disable once InvokeAsExtensionMethod
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

                var indexDefinition = indexBuilder.ToIndexDefinition(store.Conventions);
                indexDefinition.Name = "ImportantDocCounts";

                // Act & Assert
                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                var map = indexDefinition.Maps.First();
                Assert.DoesNotContain("MemoryExtensions", map);
                Assert.DoesNotContain("ReadOnlySpan", map);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapReduceIndex_MemoryExtensionsContainsAny_InReduce_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                // Arrange
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
                        // ReSharper disable once InvokeAsExtensionMethod
                        where MemoryExtensions.ContainsAny<string>(new string[] { "backend", "frontend", "database" }, new string[] { g.Key })
                        select new CategoryCount
                        {
                            Category = g.Key,
                            Count = g.Sum(x => x.Count)
                        }
                };

                var indexDefinition = indexBuilder.ToIndexDefinition(store.Conventions);
                indexDefinition.Name = "ValidCategoryCounts";

                // Act & Assert
                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                var reduce = indexDefinition.Reduce;
                Assert.DoesNotContain("MemoryExtensions", reduce);
                Assert.DoesNotContain("ReadOnlySpan", reduce);
            }
        }

        #endregion

        #region Combined Scenarios

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MixedMemoryExtensionsCalls_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                // Arrange
                var indexBuilder = new IndexDefinitionBuilder<DocWithArray>
                {
                    Map = docs => from doc in docs
                        select new
                        {
                            // ReSharper disable once InvokeAsExtensionMethod
                            HasTag = MemoryExtensions.Contains(doc.Tags, "csharp"),
                            // ReSharper disable once InvokeAsExtensionMethod
                            HasAnyTag = MemoryExtensions.ContainsAny<string>(doc.Tags, new string[] { "csharp", "dotnet" }),
                            // ReSharper disable once InvokeAsExtensionMethod
                            HasCategory = MemoryExtensions.Contains(doc.Categories, "backend")
                        }
                };

                var indexDefinition = indexBuilder.ToIndexDefinition(store.Conventions);
                indexDefinition.Name = "MixedMemoryExtensions";

                // Act & Assert
                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                var map = indexDefinition.Maps.First();
                Assert.DoesNotContain("MemoryExtensions", map);
                Assert.DoesNotContain("ReadOnlySpan", map);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapReduceIndex_MixedMemoryExtensionsCalls_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                // Arrange
                var indexBuilder = new IndexDefinitionBuilder<DocWithArray, TagCount>
                {
                    Map = docs => from doc in docs
                        // ReSharper disable once InvokeAsExtensionMethod
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
                        // ReSharper disable once InvokeAsExtensionMethod
                        where MemoryExtensions.Contains(new string[] { "deprecated", "obsolete" }, (string)g.Key) == false
                        select new TagCount
                        {
                            Tag = g.Key,
                            Count = g.Sum(x => x.Count)
                        }
                };

                var indexDefinition = indexBuilder.ToIndexDefinition(store.Conventions);
                indexDefinition.Name = "FilteredTagCounts";

                // Act & Assert
                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                var map = indexDefinition.Maps.First();
                var reduce = indexDefinition.Reduce;

                Assert.DoesNotContain("MemoryExtensions", map);
                Assert.DoesNotContain("ReadOnlySpan", map);
                Assert.DoesNotContain("MemoryExtensions", reduce);
                Assert.DoesNotContain("ReadOnlySpan", reduce);
            }
        }

        #endregion

        #region Helper Classes

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

        #endregion
    }
}
