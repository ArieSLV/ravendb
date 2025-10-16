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

        #region Map Index Tests - Contains (String-based)

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensionsContains_StringArray_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                // Arrange
                var indexDefinition = new IndexDefinition
                {
                    Name = "DocsByTag_StringBased",
                    Maps = ["""
                            from doc in docs 
                            select new { 
                                HasTag = MemoryExtensions.Contains(doc.Tags, "csharp") 
                            }
                            """]
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                // Add test documents
                using (var session = store.OpenSession())
                {
                    session.Store(new DocWithArray
                    {
                        Id = "docs/1",
                        Tags = ["csharp", "dotnet"],
                        Categories = ["backend"],
                        Numbers = [1, 2, 3]
                    });

                    session.Store(new DocWithArray
                    {
                        Id = "docs/2",
                        Tags = ["python", "django"],
                        Categories = ["backend"],
                        Numbers = [4, 5, 6]
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                // Act & Assert
                using (var session = store.OpenSession())
                {
                    var results = session.Query<DocWithArray>(indexDefinition.Name)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(results);
                }

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(indexDefinition.Name));
                Assert.Equal(0, indexStats.ErrorsCount);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensionsContains_IntArray_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                // Arrange
                var indexDefinition = new IndexDefinition
                {
                    Name = "DocsByNumber_StringBased",
                    Maps = ["""
                            from doc in docs 
                            select new { 
                                HasNumber = MemoryExtensions.Contains(doc.Numbers, 42) 
                            }
                            """]
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                // Add test documents
                using (var session = store.OpenSession())
                {
                    session.Store(new DocWithArray
                    {
                        Id = "docs/1",
                        Tags = ["csharp"],
                        Categories = ["backend"],
                        Numbers = [42, 100]
                    });

                    session.Store(new DocWithArray
                    {
                        Id = "docs/2",
                        Tags = ["python"],
                        Categories = ["backend"],
                        Numbers = [1, 2, 3]
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                // Act & Assert
                using (var session = store.OpenSession())
                {
                    var results = session.Query<DocWithArray>(indexDefinition.Name)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(results);
                }

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(indexDefinition.Name));
                Assert.Equal(0, indexStats.ErrorsCount);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensionsContains_MultipleFields_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                // Arrange
                var indexDefinition = new IndexDefinition
                {
                    Name = "DocsByTagAndCategory_StringBased",
                    Maps = ["""
                            from doc in docs 
                            select new { 
                                HasTag = MemoryExtensions.Contains(doc.Tags, "csharp"), 
                                HasCategory = MemoryExtensions.Contains(doc.Categories, "backend") 
                            }
                            """]
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                // Add test documents
                using (var session = store.OpenSession())
                {
                    session.Store(new DocWithArray
                    {
                        Id = "docs/1",
                        Tags = ["csharp", "dotnet"],
                        Categories = ["backend", "database"],
                        Numbers = [1]
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                // Act & Assert
                using (var session = store.OpenSession())
                {
                    var results = session.Query<DocWithArray>(indexDefinition.Name)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(results);
                }

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(indexDefinition.Name));
                Assert.Equal(0, indexStats.ErrorsCount);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensionsContains_WithNegation_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                // Arrange
                var indexDefinition = new IndexDefinition
                {
                    Name = "DocsWithoutDeprecatedTag_StringBased",
                    Maps = ["""
                            from doc in docs 
                            select new { 
                                DoesNotHaveTag = MemoryExtensions.Contains(doc.Tags, "deprecated") == false 
                            }
                            """]
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                // Add test documents
                using (var session = store.OpenSession())
                {
                    session.Store(new DocWithArray
                    {
                        Id = "docs/1",
                        Tags = ["csharp"],
                        Categories = ["backend"],
                        Numbers = [1]
                    });

                    session.Store(new DocWithArray
                    {
                        Id = "docs/2",
                        Tags = ["deprecated"],
                        Categories = ["backend"],
                        Numbers = [2]
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                // Act & Assert
                using (var session = store.OpenSession())
                {
                    var results = session.Query<DocWithArray>(indexDefinition.Name)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(results);
                }

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(indexDefinition.Name));
                Assert.Equal(0, indexStats.ErrorsCount);
            }
        }

        #endregion

        #region Map Index Tests - ContainsAny (String-based)

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensionsContainsAny_IntArrays_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                // Arrange
                var indexDefinition = new IndexDefinition
                {
                    Name = "DocsByAnyNumber_StringBased",
                    Maps = ["""
                            from doc in docs 
                            select new { 
                                HasAnyNumber = MemoryExtensions.ContainsAny(doc.Numbers, new int[] { 1, 2, 3, 42 }) 
                            }
                            """]
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                // Add test documents
                using (var session = store.OpenSession())
                {
                    session.Store(new DocWithArray
                    {
                        Id = "docs/1",
                        Tags = ["csharp"],
                        Categories = ["backend"],
                        Numbers = [42, 100]
                    });

                    session.Store(new DocWithArray
                    {
                        Id = "docs/2",
                        Tags = ["python"],
                        Categories = ["backend"],
                        Numbers = [7, 8, 9]
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                // Act & Assert
                using (var session = store.OpenSession())
                {
                    var results = session.Query<DocWithArray>(indexDefinition.Name)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(results);
                }

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(indexDefinition.Name));
                Assert.Equal(0, indexStats.ErrorsCount);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensionsContainsAny_StringArrays_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                // Arrange
                var indexDefinition = new IndexDefinition
                {
                    Name = "DocsByAnyTag_StringBased",
                    Maps = ["""
                            from doc in docs 
                            select new { 
                                HasAnyTag = MemoryExtensions.ContainsAny(doc.Tags, new string[] { "csharp", "dotnet", "ravendb" }) 
                            }
                            """]
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                // Add test documents
                using (var session = store.OpenSession())
                {
                    session.Store(new DocWithArray
                    {
                        Id = "docs/1",
                        Tags = ["csharp", "aspnet"],
                        Categories = ["backend"],
                        Numbers = [1]
                    });

                    session.Store(new DocWithArray
                    {
                        Id = "docs/2",
                        Tags = ["python", "django"],
                        Categories = ["backend"],
                        Numbers = [2]
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                // Act & Assert
                using (var session = store.OpenSession())
                {
                    var results = session.Query<DocWithArray>(indexDefinition.Name)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(results);
                }

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(indexDefinition.Name));
                Assert.Equal(0, indexStats.ErrorsCount);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensionsContainsAny_WithNegation_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                // Arrange
                var indexDefinition = new IndexDefinition
                {
                    Name = "ValidDocs_StringBased",
                    Maps = ["""
                            from doc in docs 
                            select new { 
                                IsValid = MemoryExtensions.ContainsAny(doc.Tags, new string[] { "deprecated", "obsolete" }) == false 
                            }
                            """]
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                // Add test documents
                using (var session = store.OpenSession())
                {
                    session.Store(new DocWithArray
                    {
                        Id = "docs/1",
                        Tags = ["csharp", "active"],
                        Categories = ["backend"],
                        Numbers = [1]
                    });

                    session.Store(new DocWithArray
                    {
                        Id = "docs/2",
                        Tags = ["deprecated"],
                        Categories = ["backend"],
                        Numbers = [2]
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                // Act & Assert
                using (var session = store.OpenSession())
                {
                    var results = session.Query<DocWithArray>(indexDefinition.Name)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(results);
                }

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(indexDefinition.Name));
                Assert.Equal(0, indexStats.ErrorsCount);
            }
        }

        #endregion

        #region MapReduce Index Tests - Contains (String-based)

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapReduceIndex_StringBased_MemoryExtensionsContains_InMap_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                // Arrange
                var indexDefinition = new IndexDefinition
                {
                    Name = "TagCounts_StringBased",
                    Maps = ["""
                            from doc in docs 
                            where MemoryExtensions.Contains(doc.Tags, "csharp") 
                            select new { 
                                Tag = "csharp", Count = 1 
                            }
                            """],
                    Reduce = """
                             from result in results 
                             group result by result.Tag into g 
                             select new { 
                                Tag = g.Key, 
                                Count = g.Sum(x => x.Count) 
                             }
                             """
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                // Add test documents
                using (var session = store.OpenSession())
                {
                    session.Store(new DocWithArray
                    {
                        Id = "docs/1",
                        Tags = ["csharp", "dotnet"],
                        Categories = ["backend"],
                        Numbers = [1]
                    });

                    session.Store(new DocWithArray
                    {
                        Id = "docs/2",
                        Tags = ["csharp", "aspnet"],
                        Categories = ["web"],
                        Numbers = [2]
                    });

                    session.Store(new DocWithArray
                    {
                        Id = "docs/3",
                        Tags = ["python"],
                        Categories = ["backend"],
                        Numbers = [3]
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                // Act & Assert
                using (var session = store.OpenSession())
                {
                    var results = session.Query<TagCount>(indexDefinition.Name)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(results);
                    var csharpCount = results.FirstOrDefault(x => x.Tag == "csharp");
                    Assert.NotNull(csharpCount);
                    Assert.Equal(2, csharpCount.Count);
                }

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(indexDefinition.Name));
                Assert.Equal(0, indexStats.ErrorsCount);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapReduceIndex_StringBased_MemoryExtensionsContains_InReduce_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                // Arrange
                var indexDefinition = new IndexDefinition
                {
                    Name = "ActiveTagCounts_StringBased",
                    Maps = ["""
                            from doc in docs 
                            from tag in doc.Tags 
                            select new { 
                                Tag = tag, 
                                Count = 1 
                            }
                            """],
                    Reduce = """
                             from result in results 
                             group result by result.Tag into g 
                             where MemoryExtensions.Contains(new string[] { "deprecated", "obsolete" }, g.Key) == false 
                             select new { 
                                Tag = g.Key, 
                                Count = g.Sum(x => x.Count) 
                             }
                             """
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                // Add test documents
                using (var session = store.OpenSession())
                {
                    session.Store(new DocWithArray
                    {
                        Id = "docs/1",
                        Tags = ["csharp", "active"],
                        Categories = ["backend"],
                        Numbers = [1]
                    });

                    session.Store(new DocWithArray
                    {
                        Id = "docs/2",
                        Tags = ["deprecated"],
                        Categories = ["backend"],
                        Numbers = [2]
                    });

                    session.Store(new DocWithArray
                    {
                        Id = "docs/3",
                        Tags = ["csharp", "modern"],
                        Categories = ["web"],
                        Numbers = [3]
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                // Act & Assert
                using (var session = store.OpenSession())
                {
                    var results = session.Query<TagCount>(indexDefinition.Name)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(results);
                    Assert.DoesNotContain(results, x => x.Tag == "deprecated");
                    Assert.Contains(results, x => x.Tag == "csharp");
                }

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(indexDefinition.Name));
                Assert.Equal(0, indexStats.ErrorsCount);
            }
        }

        #endregion

        #region MapReduce Index Tests - ContainsAny (String-based)

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapReduceIndex_StringBased_MemoryExtensionsContainsAny_InMap_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                // Arrange
                var indexDefinition = new IndexDefinition
                {
                    Name = "ImportantDocCounts_StringBased",
                    Maps = ["""
                            from doc in docs 
                            where MemoryExtensions.ContainsAny(doc.Tags, new string[] { "csharp", "dotnet", "ravendb" }) 
                            select new { 
                                Tag = "important", 
                                Count = 1 
                            }
                            """],
                    Reduce = """
                             from result in results 
                             group result by result.Tag into g 
                             select new { 
                                 Tag = g.Key, 
                                 Count = g.Sum(x => x.Count) 
                             }
                             """
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                // Add test documents
                using (var session = store.OpenSession())
                {
                    session.Store(new DocWithArray
                    {
                        Id = "docs/1",
                        Tags = ["csharp", "backend"],
                        Categories = ["backend"],
                        Numbers = [1]
                    });

                    session.Store(new DocWithArray
                    {
                        Id = "docs/2",
                        Tags = ["python", "django"],
                        Categories = ["backend"],
                        Numbers = [2]
                    });

                    session.Store(new DocWithArray
                    {
                        Id = "docs/3",
                        Tags = ["ravendb", "database"],
                        Categories = ["database"],
                        Numbers = [3]
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                // Act & Assert
                using (var session = store.OpenSession())
                {
                    var results = session.Query<TagCount>(indexDefinition.Name)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(results);
                    var importantCount = results.FirstOrDefault(x => x.Tag == "important");
                    Assert.NotNull(importantCount);
                    Assert.Equal(2, importantCount.Count); // docs/1 + docs/3
                }

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(indexDefinition.Name));
                Assert.Equal(0, indexStats.ErrorsCount);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapReduceIndex_StringBased_MemoryExtensionsContainsAny_InReduce_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                // Arrange
                var indexDefinition = new IndexDefinition
                {
                    Name = "ValidCategoryCounts_StringBased",
                    Maps = ["""
                            from doc in docs 
                            from category in doc.Categories 
                            select new { 
                                Category = category, 
                                Count = 1 
                            }
                            """],
                    Reduce = """
                             from result in results 
                             group result by result.Category into g 
                             where MemoryExtensions.ContainsAny(new string[] { "backend", "frontend", "database" }, new string[] { g.Key }) 
                             select new { 
                                 Category = g.Key, 
                                 Count = g.Sum(x => x.Count) 
                             }
                             """
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                // Add test documents
                using (var session = store.OpenSession())
                {
                    session.Store(new DocWithArray
                    {
                        Id = "docs/1",
                        Tags = ["csharp"],
                        Categories = ["backend"],
                        Numbers = [1]
                    });

                    session.Store(new DocWithArray
                    {
                        Id = "docs/2",
                        Tags = ["react"],
                        Categories = ["frontend"],
                        Numbers = [2]
                    });

                    session.Store(new DocWithArray
                    {
                        Id = "docs/3",
                        Tags = ["mobile"],
                        Categories = ["mobile"],
                        Numbers = [3]
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                // Act & Assert
                using (var session = store.OpenSession())
                {
                    var results = session.Query<CategoryCount>(indexDefinition.Name)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(results);
                    Assert.Contains(results, x => x.Category == "backend");
                    Assert.Contains(results, x => x.Category == "frontend");
                    Assert.DoesNotContain(results, x => x.Category == "mobile");
                }

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(indexDefinition.Name));
                Assert.Equal(0, indexStats.ErrorsCount);
            }
        }

        #endregion

        #region Combined Scenarios (String-based)

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MixedMemoryExtensionsCalls_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                // Arrange
                var indexDefinition = new IndexDefinition
                {
                    Name = "MixedMemoryExtensions_StringBased",
                    Maps =
                    [
                        """
                        from doc in docs 
                        select new { 
                            HasTag = MemoryExtensions.Contains(doc.Tags, "csharp"), 
                            HasAnyTag = MemoryExtensions.ContainsAny(doc.Tags, new string[] { "csharp", "dotnet" }), 
                            HasCategory = MemoryExtensions.Contains(doc.Categories, "backend") 
                        }
                        """
                    ]
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                // Add test documents
                using (var session = store.OpenSession())
                {
                    session.Store(new DocWithArray
                    {
                        Id = "docs/1",
                        Tags = ["csharp", "dotnet"],
                        Categories = ["backend"],
                        Numbers = [1]
                    });

                    session.Store(new DocWithArray
                    {
                        Id = "docs/2",
                        Tags = ["python"],
                        Categories = ["frontend"],
                        Numbers = [2]
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                // Act & Assert
                using (var session = store.OpenSession())
                {
                    var results = session.Query<DocWithArray>(indexDefinition.Name)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(results);
                }

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(indexDefinition.Name));
                Assert.Equal(0, indexStats.ErrorsCount);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapReduceIndex_StringBased_MixedMemoryExtensionsCalls_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                // Arrange
                var indexDefinition = new IndexDefinition
                {
                    Name = "FilteredTagCounts_StringBased",
                    Maps = ["""
                            from doc in docs 
                            where MemoryExtensions.ContainsAny(doc.Tags, new string[] { "important", "critical" }) 
                            from tag in doc.Tags 
                            select new { 
                                Tag = tag, 
                                Count = 1 
                            }
                            """],
                    Reduce = """
                             from result in results 
                             group result by result.Tag into g 
                             where MemoryExtensions.Contains(new string[] { "deprecated", "obsolete" }, (string)g.Key) == false 
                             select new { 
                                 Tag = g.Key, 
                                 Count = g.Sum(x => x.Count) 
                             }
                             """
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                // Add test documents
                using (var session = store.OpenSession())
                {
                    session.Store(new DocWithArray
                    {
                        Id = "docs/1",
                        Tags = ["important", "csharp"],
                        Categories = ["backend"],
                        Numbers = [1]
                    });

                    session.Store(new DocWithArray
                    {
                        Id = "docs/2",
                        Tags = ["critical", "deprecated"],
                        Categories = ["backend"],
                        Numbers = [2]
                    });

                    session.Store(new DocWithArray
                    {
                        Id = "docs/3",
                        Tags = ["normal"],
                        Categories = ["backend"],
                        Numbers = [3]
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                // Act & Assert
                using (var session = store.OpenSession())
                {
                    var results = session.Query<TagCount>(indexDefinition.Name)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(results);
                    Assert.Contains(results, x => x.Tag == "important");
                    Assert.Contains(results, x => x.Tag == "csharp");
                    Assert.Contains(results, x => x.Tag == "critical");
                    Assert.DoesNotContain(results, x => x.Tag == "deprecated");
                }

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(indexDefinition.Name));
                Assert.Equal(0, indexStats.ErrorsCount);
            }
        }

        #endregion

        #region Type Coverage Tests - DateTime

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContains_DateTimeArray_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var indexBuilder = new IndexDefinitionBuilder<DocWithDates>
                {
                    Map = docs => from doc in docs
                        select new
                        {
                            HasDate = MemoryExtensions.Contains(doc.ImportantDates, new DateTime(2024, 1, 1))
                        }
                };

                var indexDefinition = indexBuilder.ToIndexDefinition(store.Conventions);
                indexDefinition.Name = "DocsByDate";

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                var map = indexDefinition.Maps.First();
                Assert.DoesNotContain("MemoryExtensions", map);
                Assert.DoesNotContain("ReadOnlySpan", map);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensionsContains_DateTimeArray_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var indexDefinition = new IndexDefinition
                {
                    Name = "DocsByDate_StringBased",
                    Maps = ["""
                            from doc in docs 
                            select new { 
                                HasDate = MemoryExtensions.Contains(doc.ImportantDates, DateTime.Parse("2024-01-01")) 
                            }
                            """]
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                using (var session = store.OpenSession())
                {
                    session.Store(new DocWithDates
                    {
                        Id = "docs/1",
                        ImportantDates = [
                            new DateTime(2024, 1, 1),
                            new DateTime(2024, 12, 31)]
                    });

                    session.Store(new DocWithDates
                    {
                        Id = "docs/2",
                        ImportantDates = [new DateTime(2023, 6, 15)]
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<DocWithDates>(indexDefinition.Name)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(results);
                }

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(indexDefinition.Name));
                Assert.Equal(0, indexStats.ErrorsCount);
            }
        }

        #endregion

        #region Type Coverage Tests - Double

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContains_DoubleArray_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var indexBuilder = new IndexDefinitionBuilder<DocWithDoubles>
                {
                    Map = docs => from doc in docs
                        select new
                        {
                            HasValue = MemoryExtensions.Contains(doc.Values, 3.14)
                        }
                };

                var indexDefinition = indexBuilder.ToIndexDefinition(store.Conventions);
                indexDefinition.Name = "DocsByDouble";

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                var map = indexDefinition.Maps.First();
                Assert.DoesNotContain("MemoryExtensions", map);
                Assert.DoesNotContain("ReadOnlySpan", map);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensionsContains_DoubleArray_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var indexDefinition = new IndexDefinition
                {
                    Name = "DocsByDouble_StringBased",
                    Maps = ["""
                            from doc in docs 
                            select new { 
                                HasValue = MemoryExtensions.Contains(doc.Values, 3.14) 
                            }
                            """]
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                using (var session = store.OpenSession())
                {
                    session.Store(new DocWithDoubles
                    {
                        Id = "docs/1",
                        Values = [
                            3.14,
                            2.71,
                            1.41
                        ]
                    });
                    session.Store(new DocWithDoubles
                    {
                        Id = "docs/2",
                        Values = [
                            1.0,
                            2.0,
                            3.0
                        ]
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<DocWithDoubles>(indexDefinition.Name)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(results);
                }

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(indexDefinition.Name));
                Assert.Equal(0, indexStats.ErrorsCount);
            }
        }

        #endregion

        #region Type Coverage Tests - Long

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContains_LongArray_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var indexBuilder = new IndexDefinitionBuilder<DocWithLongs>
                {
                    Map = docs => from doc in docs
                        select new
                        {
                            HasValue = MemoryExtensions.Contains(doc.Values, 9223372036854775807L)
                        }
                };

                var indexDefinition = indexBuilder.ToIndexDefinition(store.Conventions);
                indexDefinition.Name = "DocsByLong";

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                var map = indexDefinition.Maps.First();
                Assert.DoesNotContain("MemoryExtensions", map);
                Assert.DoesNotContain("ReadOnlySpan", map);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensionsContains_LongArray_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var indexDefinition = new IndexDefinition
                {
                    Name = "DocsByLong_StringBased",
                    Maps = ["""
                            from doc in docs 
                            select new { 
                                HasValue = MemoryExtensions.Contains(doc.Values, 9223372036854775807L) 
                            }
                            """]
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                using (var session = store.OpenSession())
                {
                    session.Store(new DocWithLongs
                    {
                        Id = "docs/1",
                        Values = [
                            100L,
                            200L,
                            9223372036854775807L
                        ]
                    });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<DocWithLongs>(indexDefinition.Name)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(results);
                }

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(indexDefinition.Name));
                Assert.Equal(0, indexStats.ErrorsCount);
            }
        }

        #endregion

        #region Edge Cases Tests - Empty Array

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensionsContains_EmptyArray_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var indexBuilder = new IndexDefinitionBuilder<DocWithArray>
                {
                    Map = docs => from doc in docs
                        select new
                        {
                            HasTag = MemoryExtensions.Contains(Array.Empty<string>(), "test")
                        }
                };

                var indexDefinition = indexBuilder.ToIndexDefinition(store.Conventions);
                indexDefinition.Name = "EmptyArrayTest";

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                var map = indexDefinition.Maps.First();
                Assert.DoesNotContain("MemoryExtensions", map);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensionsContains_EmptyArray_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var indexDefinition = new IndexDefinition
                {
                    Name = "EmptyArrayTest_StringBased",
                    Maps = ["""
                            from doc in docs 
                            select new { 
                                HasTag = MemoryExtensions.Contains(new string[] { }, "test") 
                            }
                            """]
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                using (var session = store.OpenSession())
                {
                    session.Store(new DocWithArray
                    {
                        Id = "docs/1",
                        Tags = ["test"]
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<DocWithArray>(indexDefinition.Name)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(results);
                }

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(indexDefinition.Name));
                Assert.Equal(0, indexStats.ErrorsCount);
            }
        }

        #endregion

        #region Logical Operators Tests - OR

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensions_WithOrOperator_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
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

                var indexDefinition = indexBuilder.ToIndexDefinition(store.Conventions);
                indexDefinition.Name = "OrOperatorTest";

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                var map = indexDefinition.Maps.First();
                Assert.DoesNotContain("MemoryExtensions", map);
                Assert.DoesNotContain("ReadOnlySpan", map);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensions_WithOrOperator_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var indexDefinition = new IndexDefinition
                {
                    Name = "OrOperatorTest_StringBased",
                    Maps =
                    [
                        """
                        from doc in docs 
                        select new { 
                            Match = MemoryExtensions.Contains(doc.Tags, "csharp") || 
                            MemoryExtensions.Contains(doc.Categories, "backend") 
                        }
                        """
                    ]
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                using (var session = store.OpenSession())
                {
                    session.Store(new DocWithArray
                    {
                        Id = "docs/1",
                        Tags = ["csharp"],
                        Categories = ["frontend"],
                        Numbers = [1]
                    });

                    session.Store(new DocWithArray
                    {
                        Id = "docs/2",
                        Tags = ["python"],
                        Categories = ["backend"],
                        Numbers = [2]
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<DocWithArray>(indexDefinition.Name)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(results);
                }

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(indexDefinition.Name));
                Assert.Equal(0, indexStats.ErrorsCount);
            }
        }

        #endregion

        #region Logical Operators Tests - AND

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensions_WithAndOperator_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
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

                var indexDefinition = indexBuilder.ToIndexDefinition(store.Conventions);
                indexDefinition.Name = "AndOperatorTest";

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                var map = indexDefinition.Maps.First();
                Assert.DoesNotContain("MemoryExtensions", map);
                Assert.DoesNotContain("ReadOnlySpan", map);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensions_WithAndOperator_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var indexDefinition = new IndexDefinition
                {
                    Name = "AndOperatorTest_StringBased",
                    Maps = ["""
                            from doc in docs 
                            select new { 
                                Match = MemoryExtensions.Contains(doc.Tags, "csharp") && 
                                MemoryExtensions.ContainsAny(doc.Categories, new string[] { "backend", "frontend" }) 
                            }
                            """]
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                using (var session = store.OpenSession())
                {
                    session.Store(new DocWithArray
                    {
                        Id = "docs/1",
                        Tags = ["csharp", "dotnet"],
                        Categories = ["backend"],
                        Numbers = [1]
                    });

                    session.Store(new DocWithArray
                    {
                        Id = "docs/2",
                        Tags = ["csharp"],
                        Categories = ["mobile"],
                        Numbers = [2]
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<DocWithArray>(indexDefinition.Name)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(results);
                }

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(indexDefinition.Name));
                Assert.Equal(0, indexStats.ErrorsCount);
            }
        }

        #endregion

        #region Nested Collections Tests

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MemoryExtensions_NestedCollections_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
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

                var indexDefinition = indexBuilder.ToIndexDefinition(store.Conventions);
                indexDefinition.Name = "NestedArrayTest";

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                var map = indexDefinition.Maps.First();
                Assert.DoesNotContain("MemoryExtensions", map);
                Assert.DoesNotContain("ReadOnlySpan", map);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensions_NestedCollections_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var indexDefinition = new IndexDefinition
                {
                    Name = "NestedArrayTest_StringBased",
                    Maps = ["""
                            from doc in docs 
                            from item in doc.Items 
                            where MemoryExtensions.Contains(item.Tags, "important") 
                            select new { 
                                ItemId = item.Id, 
                                IsImportant = true 
                            }
                            """]
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                using (var session = store.OpenSession())
                {
                    session.Store(new DocWithNestedArray
                    {
                        Id = "docs/1",
                        Items =
                        [
                            new ItemWithTags { Id = "item1", Tags = ["important", "urgent"] },
                            new ItemWithTags { Id = "item2", Tags = ["normal"] }
                        ]
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<dynamic>(indexDefinition.Name)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(results);
                }

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(indexDefinition.Name));
                Assert.Equal(0, indexStats.ErrorsCount);
            }
        }

        #endregion

        #region Correctness Tests

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensions_CorrectResults_ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var indexDefinition = new IndexDefinition
                {
                    Name = "CorrectnessTest",
                    Maps = ["""
                            from doc in docs 
                            select new { 
                                HasCSharp = MemoryExtensions.Contains(doc.Tags, "csharp"), 
                                HasAny = MemoryExtensions.ContainsAny(doc.Tags, new string[] { "java", "python" }) 
                            }
                            """]
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                using (var session = store.OpenSession())
                {
                    session.Store(new DocWithArray
                    {
                        Id = "docs/1",
                        Tags = ["csharp", "dotnet"],
                        Categories = ["backend"],
                        Numbers = [1]
                    });

                    session.Store(new DocWithArray
                    {
                        Id = "docs/2",
                        Tags = ["java", "spring"],
                        Categories = ["backend"],
                        Numbers = [2]
                    });

                    session.Store(new DocWithArray
                    {
                        Id = "docs/3",
                        Tags = ["rust", "go"],
                        Categories = ["systems"],
                        Numbers = [3]
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.DocumentQuery<dynamic>(indexDefinition.Name)
                        .WaitForNonStaleResults()
                        .ToList();

                    Assert.Equal(3, results.Count);
                }

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(indexDefinition.Name));
                Assert.Equal(0, indexStats.ErrorsCount);
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
    }
}
