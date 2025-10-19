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

        #region Map Index Tests - Contains (String-based)

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensionsContains_StringArray_ShouldWork(Options options)
        {
            const string map = """
                               from doc in docs 
                               select new { 
                                   HasTag = MemoryExtensions.Contains(doc.Tags, "csharp") 
                               }
                               """;

            var docs = new object[]
            {
                new DocWithArray { Tags = ["csharp", "dotnet"] },
                new DocWithArray { Tags = ["python", "django"] }
            };

            AssertStringBasedIndexCompilesAndRuns(options, map, docs);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensionsContains_IntArray_ShouldWork(Options options)
        {
            const string map = """
                               from doc in docs 
                               select new { 
                                   HasNumber = MemoryExtensions.Contains(doc.Numbers, 42) 
                               }
                               """;

            var docs = new object[]
            {
                new DocWithArray
                {
                    Tags = ["csharp"],
                    Categories = ["backend"],
                    Numbers = [42, 100]
                },
                new DocWithArray
                {
                    Tags = ["python"],
                    Categories = ["backend"],
                    Numbers = [1, 2, 3]
                }
            };

            AssertStringBasedIndexCompilesAndRuns(options, map, docs);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensionsContains_MultipleFields_ShouldWork(Options options)
        {
            const string map = """
                               from doc in docs 
                               select new { 
                                   HasTag = MemoryExtensions.Contains(doc.Tags, "csharp"), 
                                   HasCategory = MemoryExtensions.Contains(doc.Categories, "backend") 
                               }
                               """;

            var docs = new object[]
            {
                new DocWithArray
                {
                    Tags = ["csharp", "dotnet"],
                    Categories = ["backend", "database"],
                    Numbers = [1]
                }
            };

            AssertStringBasedIndexCompilesAndRuns(options, map, docs);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensionsContains_WithNegation_ShouldWork(Options options)
        {
            const string map = """
                               from doc in docs 
                               select new { 
                                   DoesNotHaveTag = MemoryExtensions.Contains(doc.Tags, "deprecated") == false 
                               }
                               """;

            var docs = new object[]
            {
                new DocWithArray
                {
                    Tags = ["csharp"],
                    Categories = ["backend"],
                    Numbers = [1]
                },
                new DocWithArray
                {
                    Tags = ["deprecated"],
                    Categories = ["backend"],
                    Numbers = [2]
                }
            };

            AssertStringBasedIndexCompilesAndRuns(options, map, docs);
        }

        #endregion

        #region Map Index Tests - ContainsAny (String-based)

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensionsContainsAny_IntArrays_ShouldWork(Options options)
        {
            const string map = """
                               from doc in docs 
                               select new { 
                                   HasAnyNumber = MemoryExtensions.ContainsAny(doc.Numbers, new int[] { 1, 2, 3, 42 }) 
                               }
                               """;

            var docs = new object[]
            {
                new DocWithArray
                {
                    Tags = ["csharp"],
                    Categories = ["backend"],
                    Numbers = [42, 100]
                },
                new DocWithArray
                {
                    Tags = ["python"],
                    Categories = ["backend"],
                    Numbers = [7, 8, 9]
                }
            };

            AssertStringBasedIndexCompilesAndRuns(options, map, docs);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensionsContainsAny_StringArrays_ShouldWork(Options options)
        {
            const string map = """
                               from doc in docs 
                               select new { 
                                   HasAnyTag = MemoryExtensions.ContainsAny(doc.Tags, new string[] { "csharp", "dotnet", "ravendb" }) 
                               }
                               """;

            var docs = new object[]
            {
                new DocWithArray
                {
                    Tags = ["csharp", "aspnet"],
                    Categories = ["backend"],
                    Numbers = [1]
                },
                new DocWithArray
                {
                    Tags = ["python", "django"],
                    Categories = ["backend"],
                    Numbers = [2]
                }
            };

            AssertStringBasedIndexCompilesAndRuns(options, map, docs);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensionsContainsAny_WithNegation_ShouldWork(Options options)
        {
            const string map = """
                               from doc in docs 
                               select new { 
                                   IsValid = MemoryExtensions.ContainsAny(doc.Tags, new string[] { "deprecated", "obsolete" }) == false 
                               }
                               """;

            var docs = new object[]
            {
                new DocWithArray
                {
                    Tags = ["csharp", "active"],
                    Categories = ["backend"],
                    Numbers = [1]
                },
                new DocWithArray
                {
                    Tags = ["deprecated"],
                    Categories = ["backend"],
                    Numbers = [2]
                }
            };

            AssertStringBasedIndexCompilesAndRuns(options, map, docs);
        }

        #endregion

        #region MapReduce Index Tests - Contains (String-based)

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapReduceIndex_StringBased_MemoryExtensionsContains_InMap_ShouldWork(Options options)
        {
            const string map = """
                               from doc in docs 
                               where MemoryExtensions.Contains(doc.Tags, "csharp") 
                               select new { 
                                   Tag = "csharp", 
                                   Count = 1 
                               }
                               """;
            const string reduce = """
                             from result in results 
                             group result by result.Tag into g 
                             select new { 
                                Tag = g.Key, 
                                Count = g.Sum(x => x.Count) 
                             }
                             """;

            var docs = new object[]
            {
                new DocWithArray
                {
                    Tags = ["csharp", "dotnet"],
                    Categories = ["backend"],
                    Numbers = [1]
                },
                new DocWithArray
                {
                    Tags = ["csharp", "aspnet"],
                    Categories = ["web"],
                    Numbers = [2]
                },
                new DocWithArray
                {
                    Tags = ["python"],
                    Categories = ["backend"],
                    Numbers = [3]
                }
            };

            AssertStringBasedIndexCompilesAndRuns(options, map, docs, reduce, additionalAsserts: (store, indexName) =>
            {
                using (var session = store.OpenSession())
                {
                    var results = session.Query<TagCount>(indexName)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(results);
                    var csharpCount = results.FirstOrDefault(x => x.Tag == "csharp");
                    Assert.NotNull(csharpCount);
                    Assert.Equal(2, csharpCount.Count);
                }
            });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapReduceIndex_StringBased_MemoryExtensionsContains_InReduce_ShouldWork(Options options)
        {
            const string map = """
                            from doc in docs 
                            from tag in doc.Tags 
                            select new { 
                                Tag = tag, 
                                Count = 1 
                            }
                            """;
            const string reduce = """
                             from result in results 
                             group result by result.Tag into g 
                             where MemoryExtensions.Contains(new string[] { "deprecated", "obsolete" }, g.Key) == false 
                             select new { 
                                Tag = g.Key, 
                                Count = g.Sum(x => x.Count) 
                             }
                             """;

            var docs = new object[]
            {
                new DocWithArray
                {
                    Tags = ["csharp", "active"],
                    Categories = ["backend"],
                    Numbers = [1]
                },
                new DocWithArray
                {
                    Tags = ["deprecated"],
                    Categories = ["backend"],
                    Numbers = [2]
                },
                new DocWithArray
                {
                    Tags = ["csharp", "modern"],
                    Categories = ["web"],
                    Numbers = [3]
                }
            };

            AssertStringBasedIndexCompilesAndRuns(options, map, docs: docs, reduce: reduce, additionalAsserts: (store, indexName) =>
            {
                using (var session = store.OpenSession())
                {
                    var results = session.Query<TagCount>(indexName)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(results);
                    Assert.DoesNotContain(results, x => x.Tag == "deprecated");
                    Assert.Contains(results, x => x.Tag == "csharp");
                }
            });
        }

        #endregion

        #region MapReduce Index Tests - ContainsAny (String-based)

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapReduceIndex_StringBased_MemoryExtensionsContainsAny_InMap_ShouldWork(Options options)
        {
            const string map = """
                            from doc in docs 
                            where MemoryExtensions.ContainsAny(doc.Tags, new string[] { "csharp", "dotnet", "ravendb" }) 
                            select new { 
                                Tag = "important", 
                                Count = 1 
                            }
                            """;
            const string reduce = """
                             from result in results 
                             group result by result.Tag into g 
                             select new { 
                                 Tag = g.Key, 
                                 Count = g.Sum(x => x.Count) 
                             }
                             """;

            var docs = new object[]
            {
                new DocWithArray
                {
                    Tags = ["csharp", "backend"],
                    Categories = ["backend"],
                    Numbers = [1]
                },
                new DocWithArray
                {
                    Tags = ["python", "django"],
                    Categories = ["backend"],
                    Numbers = [2]
                },
                new DocWithArray
                {
                    Tags = ["ravendb", "database"],
                    Categories = ["database"],
                    Numbers = [3]
                }
            };

            AssertStringBasedIndexCompilesAndRuns(options, map, docs: docs, reduce: reduce, additionalAsserts: (store, indexName) =>
            {
                using (var session = store.OpenSession())
                {
                    var results = session.Query<TagCount>(indexName)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(results);
                    var importantCount = results.FirstOrDefault(x => x.Tag == "important");
                    Assert.NotNull(importantCount);
                    Assert.Equal(2, importantCount.Count); // docs/1 + docs/3
                }
            });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapReduceIndex_StringBased_MemoryExtensionsContainsAny_InReduce_ShouldWork(Options options)
        {
            const string map = """
                            from doc in docs 
                            from category in doc.Categories 
                            select new { 
                                Category = category, 
                                Count = 1 
                            }
                            """;
            const string reduce = """
                             from result in results 
                             group result by result.Category into g 
                             where MemoryExtensions.ContainsAny(new string[] { "backend", "frontend", "database" }, new string[] { g.Key }) 
                             select new { 
                                 Category = g.Key, 
                                 Count = g.Sum(x => x.Count) 
                             }
                             """;

            var docs = new object[]
            {
                new DocWithArray
                {
                    Tags = ["csharp"],
                    Categories = ["backend"],
                    Numbers = [1]
                },
                new DocWithArray
                {
                    Tags = ["react"],
                    Categories = ["frontend"],
                    Numbers = [2]
                },
                new DocWithArray
                {
                    Tags = ["mobile"],
                    Categories = ["mobile"],
                    Numbers = [3]
                }
            };

            AssertStringBasedIndexCompilesAndRuns(options, map, docs: docs, reduce: reduce, additionalAsserts: (store, indexName) =>
            {
                using (var session = store.OpenSession())
                {
                    var results = session.Query<CategoryCount>(indexName)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(results);
                    Assert.Contains(results, x => x.Category == "backend");
                    Assert.Contains(results, x => x.Category == "frontend");
                    Assert.DoesNotContain(results, x => x.Category == "mobile");
                }
            });
        }

        #endregion

        #region Combined Scenarios (String-based)

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MixedMemoryExtensionsCalls_ShouldWork(Options options)
        {
            const string map = """
                        from doc in docs 
                        select new { 
                            HasTag = MemoryExtensions.Contains(doc.Tags, "csharp"), 
                            HasAnyTag = MemoryExtensions.ContainsAny(doc.Tags, new string[] { "csharp", "dotnet" }), 
                            HasCategory = MemoryExtensions.Contains(doc.Categories, "backend") 
                        }
                        """;

            var docs = new object[]
            {
                new DocWithArray
                {
                    Tags = ["csharp", "dotnet"],
                    Categories = ["backend"],
                    Numbers = [1]
                },
                new DocWithArray
                {
                    Tags = ["python"],
                    Categories = ["frontend"],
                    Numbers = [2]
                }
            };

            AssertStringBasedIndexCompilesAndRuns(options, map, docs);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapReduceIndex_StringBased_MixedMemoryExtensionsCalls_ShouldWork(Options options)
        {
            const string map = """
                            from doc in docs 
                            where MemoryExtensions.ContainsAny(doc.Tags, new string[] { "important", "critical" }) 
                            from tag in doc.Tags 
                            select new { 
                                Tag = tag, 
                                Count = 1 
                            }
                            """;
            const string reduce = """
                             from result in results 
                             group result by result.Tag into g 
                             where MemoryExtensions.Contains(new string[] { "deprecated", "obsolete" }, (string)g.Key) == false 
                             select new { 
                                 Tag = g.Key, 
                                 Count = g.Sum(x => x.Count) 
                             }
                             """;

            var docs = new object[]
            {
                new DocWithArray
                {
                    Tags = ["important", "csharp"],
                    Categories = ["backend"],
                    Numbers = [1]
                },
                new DocWithArray
                {
                    Tags = ["critical", "deprecated"],
                    Categories = ["backend"],
                    Numbers = [2]
                },
                new DocWithArray
                {
                    Tags = ["normal"],
                    Categories = ["backend"],
                    Numbers = [3]
                }
            };

            AssertStringBasedIndexCompilesAndRuns(options, map, docs, reduce: reduce, additionalAsserts: (store, indexName) =>
            {
                using (var session = store.OpenSession())
                {
                    var results = session.Query<TagCount>(indexName)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(results);
                    Assert.Contains(results, x => x.Tag == "important");
                    Assert.Contains(results, x => x.Tag == "csharp");
                    Assert.Contains(results, x => x.Tag == "critical");
                    Assert.DoesNotContain(results, x => x.Tag == "deprecated");
                }
            });
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

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensionsContains_DateTimeArray_ShouldWork(Options options)
        {
            const string map = """
                            from doc in docs 
                            select new { 
                                HasDate = MemoryExtensions.Contains(doc.ImportantDates, DateTime.Parse("2024-01-01")) 
                            }
                            """;

            var docs = new object[]
            {
                new DocWithDates
                {
                    ImportantDates = [
                        new DateTime(2024, 1, 1),
                        new DateTime(2024, 12, 31)]
                },
                new DocWithDates
                {
                    ImportantDates = [new DateTime(2023, 6, 15)]
                }
            };

            AssertStringBasedIndexCompilesAndRuns(options, map, docs);
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

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensionsContains_DoubleArray_ShouldWork(Options options)
        {
            const string map = """
                            from doc in docs 
                            select new { 
                                HasValue = MemoryExtensions.Contains(doc.Values, 3.14) 
                            }
                            """;

            var docs = new object[]
            {
                new DocWithDoubles
                {
                    Values = [3.14, 2.71, 1.41]
                },
                new DocWithDoubles
                {
                    Values = [1.0, 2.0, 3.0]
                }
            };

            AssertStringBasedIndexCompilesAndRuns(options, map, docs);
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

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensionsContains_LongArray_ShouldWork(Options options)
        {
            const string map = """
                            from doc in docs 
                            select new { 
                                HasValue = MemoryExtensions.Contains(doc.Values, 9223372036854775807L) 
                            }
                            """;

            var docs = new object[]
            {
                new DocWithLongs
                {
                    Values = [100L, 200L, 9223372036854775807L]
                }
            };

            AssertStringBasedIndexCompilesAndRuns(options, map, docs);
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

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensionsContains_EmptyArray_ShouldWork(Options options)
        {
            const string map = """
                            from doc in docs 
                            select new { 
                                HasTag = MemoryExtensions.Contains(new string[] { }, "test") 
                            }
                            """;

            var docs = new object[]
            {
                new DocWithArray
                {
                    Tags = ["test"]
                }
            };

            AssertStringBasedIndexCompilesAndRuns(options, map, docs);
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

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensions_WithOrOperator_ShouldWork(Options options)
        {
            const string map = """
                        from doc in docs 
                        select new { 
                            Match = MemoryExtensions.Contains(doc.Tags, "csharp") || 
                            MemoryExtensions.Contains(doc.Categories, "backend") 
                        }
                        """;

            var docs = new object[]
            {
                new DocWithArray
                {
                    Tags = ["csharp"],
                    Categories = ["frontend"],
                    Numbers = [1]
                },
                new DocWithArray
                {
                    Tags = ["python"],
                    Categories = ["backend"],
                    Numbers = [2]
                }
            };

            AssertStringBasedIndexCompilesAndRuns(options, map, docs);
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

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensions_WithAndOperator_ShouldWork(Options options)
        {
            const string map = """
                            from doc in docs 
                            select new { 
                                Match = MemoryExtensions.Contains(doc.Tags, "csharp") && 
                                MemoryExtensions.ContainsAny(doc.Categories, new string[] { "backend", "frontend" }) 
                            }
                            """;

            var docs = new object[]
            {
                new DocWithArray
                {
                    Tags = ["csharp", "dotnet"],
                    Categories = ["backend"],
                    Numbers = [1]
                },
                new DocWithArray
                {
                    Tags = ["csharp"],
                    Categories = ["mobile"],
                    Numbers = [2]
                }
            };

            AssertStringBasedIndexCompilesAndRuns(options, map, docs);
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

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensions_NestedCollections_ShouldWork(Options options)
        {
            const string map = """
                            from doc in docs 
                            from item in doc.Items 
                            where MemoryExtensions.Contains(item.Tags, "important") 
                            select new { 
                                ItemId = item.Id, 
                                IsImportant = true 
                            }
                            """;

            var docs = new object[]
            {
                new DocWithNestedArray
                {
                    Items =
                    [
                        new ItemWithTags { Tags = ["important", "urgent"] },
                        new ItemWithTags { Tags = ["normal"] }
                    ]
                }
            };

            AssertStringBasedIndexCompilesAndRuns(options, map, docs);
        }

        #endregion

        #region Correctness Tests

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_StringBased_MemoryExtensions_CorrectResults_ShouldWork(Options options)
        {
            const string map = """
                            from doc in docs 
                            select new { 
                                HasCSharp = MemoryExtensions.Contains(doc.Tags, "csharp"), 
                                HasAny = MemoryExtensions.ContainsAny(doc.Tags, new string[] { "java", "python" }) 
                            }
                            """;

            var docs = new object[]
            {
                new DocWithArray
                {
                    Tags = ["csharp", "dotnet"],
                    Categories = ["backend"],
                    Numbers = [1]
                },
                new DocWithArray
                {
                    Tags = ["java", "spring"],
                    Categories = ["backend"],
                    Numbers = [2]
                },
                new DocWithArray
                {
                    Tags = ["rust", "go"],
                    Categories = ["systems"],
                    Numbers = [3]
                }
            };

            AssertStringBasedIndexCompilesAndRuns(options, map, docs, additionalAsserts: (store, indexName) =>
            {
                using (var session = store.OpenSession())
                {
                    var results = session.Query<DocWithArray>(indexName)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.Equal(4, results.Count); // 3 docs + hilo
                }
            });
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
