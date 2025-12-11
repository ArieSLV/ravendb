using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
// ReSharper disable InvokeAsExtensionMethod
// ReSharper disable CSharp14OverloadResolutionWithSpanBreakingChange
// ReSharper disable ConvertClosureToMethodGroup

namespace SlowTests.Client.Indexing
{
    [SuppressMessage("Performance", "CA1861:Avoid constant arrays as arguments")]
    public class DynamicArrayEqualityAndSetOperationsTests : IndexingTestBase
    {
        public DynamicArrayEqualityAndSetOperationsTests(ITestOutputHelper output) : base(output)
        {
        }

        #region Longs

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Intersect_LongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasAny = doc.Values.Intersect(new[] { 42L, 100L }).Any()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { Values = [42L, 7L] },
                new DocWithLongs { Values = [100L] },
                new DocWithLongs { Values = [1L, 2L, 3L] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Intersect), nameof(Enumerable.Any));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where HasAny = true")
                            .ToList();

                        Assert.Equal(2, results.Count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Union_LongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        UnitedValues = doc.Values
                            .Union(new[] { 3L })
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { Values = [1L] },
                new DocWithLongs { Values = [3L] },
                new DocWithLongs { Values = [4L] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Union));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where UnitedValues in (3)")
                            .ToList();

                        Assert.Equal(3, results.Count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Distinct_LongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        DistinctCount = doc.Values.Distinct().Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { Values = [1L, 1L, 2L] }, // 2 distinct
                new DocWithLongs { Values = [3L, 4L, 5L] } // 3 distinct
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Distinct)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where DistinctCount = 2")
                            .ToList();

                        Assert.Single(results);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Concat_LongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        AllValues = doc.Values.Concat(new[] { 999L }).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { Values = [1L, 2L] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Concat)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // 1, 2 + 999 => Length 3
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where AllValues = 3").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_OrderBy_LongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Smallest = doc.Values.OrderBy(x => x).First(),
                        Largest = doc.Values.OrderByDescending(x => x).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { Values = [50L, 10L, 90L] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.OrderBy), nameof(Enumerable.OrderByDescending));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Smallest = 10 and Largest = 90").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Sum_Average_Min_Max_WithSelector_LongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        MinVal = doc.Values.Min(x => x * 2),
                        MaxVal = doc.Values.Max(x => x - 5),
                        AvgVal = doc.Values.Average(x => x + 10),
                        SumVal = doc.Values.Sum(x => x * 3)
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { Values = [10L, 20L] }
            };

            // Expected:
            // Min: (10*2)=20, (20*2)=40 -> Min 20
            // Max: (10-5)=5, (20-5)=15 -> Max 15
            // Avg: (10+10)=20, (20+10)=30 -> Avg 25
            // Sum: (10*3)=30, (20*3)=60 -> Sum 90

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Min), nameof(Enumerable.Max), nameof(Enumerable.Average), nameof(Enumerable.Sum));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where MinVal = 20 and MaxVal = 15 and AvgVal = 25 and SumVal = 90").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Reverse_LongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        LastAfterReverse = doc.Values.Reverse().Last()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { Values = [1L, 2L, 3L] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Reverse)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // [1, 2, 3] -> Reverse [3, 2, 1] -> Last is 1
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where LastAfterReverse = 1").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_TakeWhile_SkipWhile_LongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        CountTaken = doc.Values.TakeWhile(x => x < 10L).Count(),
                        CountSkipped = doc.Values.SkipWhile(x => x < 10L).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { Values = [1L, 5L, 20L, 2L] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.TakeWhile), nameof(Enumerable.SkipWhile));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // TakeWhile < 10: [1, 5] -> Count 2
                        // SkipWhile < 10: [20, 2] -> Count 2
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where CountTaken = 2 and CountSkipped = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Aggregate_LongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // 0 + 1 + 2 + 3 = 6
                        SumAgg = doc.Values.Aggregate(0L, (acc, val) => acc + val)
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { Values = [1L, 2L, 3L] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Aggregate)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where SumAgg = 6").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Zip_LongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Zip with self, sum pairs. [1,2] zip [1,2] -> [(1,1), (2,2)] -> sums [2, 4] -> First = 2
                        ZipFirst = doc.Values.Zip(doc.Values, (a, b) => a + b).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { Values = [1L, 2L] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Zip)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ZipFirst = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_DefaultIfEmpty_LongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Val = doc.Values.DefaultIfEmpty(99L).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { Values = [] },
                new DocWithLongs { Values = [1L] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.DefaultIfEmpty)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var countDefault = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Val = 99").Count();
                        Assert.Equal(1, countDefault);

                        var countNormal = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Val = 1").Count();
                        Assert.Equal(1, countNormal);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_GroupBy_LongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Group by Odd/Even (x % 2).
                        // [1, 2, 3, 4] -> Key 1 has [1,3] (count 2), Key 0 has [2,4] (count 2)
                        GroupsCount = doc.Values.GroupBy(x => x % 2).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { Values = [1L, 2L, 3L, 4L] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.GroupBy)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where GroupsCount = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_First_Last_Single_WithPredicate_LongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        FirstGt = doc.Values.First(x => x > 10),
                        LastGt = doc.Values.Last(x => x > 10),
                        SingleVal = doc.Values.Single(x => x == 20),
                        SingleOrDefaultVal = doc.Values.SingleOrDefault(x => x == 999) // Should be default
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { Values = [5L, 20L, 50L] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.First), nameof(Enumerable.Last), nameof(Enumerable.Single), nameof(Enumerable.SingleOrDefault));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // First > 10 is 20
                        // Last > 10 is 50
                        // Single == 20 is 20
                        // SingleOrDefault == 999 is 0 (long default)

                        var results = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where FirstGt = 20 and LastGt = 50 and SingleVal = 20 and SingleOrDefaultVal = 0").ToList();
                        Assert.Single(results);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Any_All_Count_WithPredicate_LongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        AllPositive = doc.Values.All(x => x > 0),
                        AnyGt100 = doc.Values.Any(x => x > 100),
                        CountEven = doc.Values.Count(x => x % 2 == 0)
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { Values = [2L, 4L, 102L] }
            };

            // All > 0 : True
            // Any > 100 : True (102)
            // Count Even : 3

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.All), nameof(Enumerable.Any), nameof(Enumerable.Count));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where AllPositive = true and AnyGt100 = true and CountEven = 3").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_ElementAt_Indexer_LongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        ValAt0 = doc.Values.ElementAt(0),
                        ValAtIndex1 = doc.Values[1],
                        ValAtDef = doc.Values.ElementAtOrDefault(5) // Should be default
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { Values = [10L, 20L] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.ElementAt), nameof(Enumerable.ElementAtOrDefault));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ValAt0 = 10 and ValAtIndex1 = 20 and ValAtDef = 0").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_ToDictionary_ToLookup_LongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Key is value, Value is value*10.
                        DictCount = doc.Values.ToDictionary(k => k, v => v * 10).Count(),
                        // Lookup by Modulo 2. [1, 2, 3, 4] -> Keys: 1 (vals 1,3), 0 (vals 2,4)
                        LookupCount = doc.Values.ToLookup(k => k % 2).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { Values = [1L, 2L, 3L, 4L] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.ToDictionary), nameof(Enumerable.ToLookup));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // Dictionary should have 4 entries.
                        // Lookup should have 2 groups (Odd and Even).
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where DictCount = 4 and LookupCount = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        #endregion

        #region Ints

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Intersect_IntArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasAny = doc.IntValues.Intersect(new[] { 42, 100 }).Any()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { IntValues = [42, 7] },
                new DocWithLongs { IntValues = [100] },
                new DocWithLongs { IntValues = [1, 2, 3] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Intersect), nameof(Enumerable.Any));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where HasAny = true")
                            .ToList();

                        Assert.Equal(2, results.Count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Distinct_IntArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        DistinctCount = doc.IntValues.Distinct().Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { IntValues = [1, 1, 2] }, // 2 distinct
                new DocWithLongs { IntValues = [3, 4, 5] } // 3 distinct
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Distinct)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where DistinctCount = 2")
                            .ToList();

                        Assert.Single(results);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Concat_IntArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        AllValues = doc.IntValues.Concat(new[] { 999 }).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { IntValues = [1, 2] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Concat)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // 1, 2 + 999 => Length 3
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where AllValues = 3").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_OrderBy_IntArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Smallest = doc.IntValues.OrderBy(x => x).First(),
                        Largest = doc.IntValues.OrderByDescending(x => x).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { IntValues = [50, 10, 90] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.OrderBy), nameof(Enumerable.OrderByDescending));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Smallest = 10 and Largest = 90").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Sum_Average_Min_Max_WithSelector_IntArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        MinVal = doc.IntValues.Min(x => x * 2),
                        MaxVal = doc.IntValues.Max(x => x - 5),
                        AvgVal = doc.IntValues.Average(x => x + 10),
                        SumVal = doc.IntValues.Sum(x => x * 3)
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { IntValues = [10, 20] }
            };

            // Expected:
            // Min: (10*2)=20, (20*2)=40 -> Min 20
            // Max: (10-5)=5, (20-5)=15 -> Max 15
            // Avg: (10+10)=20, (20+10)=30 -> Avg 25
            // Sum: (10*3)=30, (20*3)=60 -> Sum 90

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Min), nameof(Enumerable.Max), nameof(Enumerable.Average), nameof(Enumerable.Sum));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where MinVal = 20 and MaxVal = 15 and AvgVal = 25 and SumVal = 90").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Reverse_IntArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        LastAfterReverse = doc.IntValues.Reverse().Last()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { IntValues = [1, 2, 3] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Reverse)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // [1, 2, 3] -> Reverse [3, 2, 1] -> Last is 1
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where LastAfterReverse = 1").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_TakeWhile_SkipWhile_IntArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        CountTaken = doc.IntValues.TakeWhile(x => x < 10).Count(),
                        CountSkipped = doc.IntValues.SkipWhile(x => x < 10).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { IntValues = [1, 5, 20, 2] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.TakeWhile), nameof(Enumerable.SkipWhile));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // TakeWhile < 10: [1, 5] -> Count 2
                        // SkipWhile < 10: [20, 2] -> Count 2
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where CountTaken = 2 and CountSkipped = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Aggregate_IntArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // 0 + 1 + 2 + 3 = 6
                        SumAgg = doc.IntValues.Aggregate(0, (acc, val) => acc + val)
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { IntValues = [1, 2, 3] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Aggregate)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where SumAgg = 6").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Zip_IntArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Zip with self, sum pairs. [1,2] zip [1,2] -> [(1,1), (2,2)] -> sums [2, 4] -> First = 2
                        ZipFirst = doc.IntValues.Zip(doc.IntValues, (a, b) => a + b).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { IntValues = [1, 2] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Zip)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ZipFirst = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_DefaultIfEmpty_IntArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Val = doc.IntValues.DefaultIfEmpty(99).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { IntValues = [] },
                new DocWithLongs { IntValues = [1] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.DefaultIfEmpty)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var countDefault = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Val = 99").Count();
                        Assert.Equal(1, countDefault);

                        var countNormal = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Val = 1").Count();
                        Assert.Equal(1, countNormal);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_GroupBy_IntArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Group by Odd/Even (x % 2).
                        // [1, 2, 3, 4] -> Key 1 has [1,3] (count 2), Key 0 has [2,4] (count 2)
                        GroupsCount = doc.IntValues.GroupBy(x => x % 2).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { IntValues = [1, 2, 3, 4] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.GroupBy)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where GroupsCount = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_First_Last_Single_WithPredicate_IntArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        FirstGt = doc.IntValues.First(x => x > 10),
                        LastGt = doc.IntValues.Last(x => x > 10),
                        SingleVal = doc.IntValues.Single(x => x == 20),
                        SingleOrDefaultVal = doc.IntValues.SingleOrDefault(x => x == 999) // Should be default
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { IntValues = [5, 20, 50] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.First), nameof(Enumerable.Last), nameof(Enumerable.Single), nameof(Enumerable.SingleOrDefault));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // First > 10 is 20
                        // Last > 10 is 50
                        // Single == 20 is 20

                        var results = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where FirstGt = 20 and LastGt = 50 and SingleVal = 20").ToList();
                        Assert.Single(results);

                        // Checking SingleOrDefault returning null/0
                        var countNull = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where SingleOrDefaultVal = null").Count();
                        if (countNull == 0)
                        {
                            var countZero = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where SingleOrDefaultVal = 0").Count();
                            Assert.Equal(1, countZero);
                        }
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Any_All_Count_WithPredicate_IntArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        AllPositive = doc.IntValues.All(x => x > 0),
                        AnyGt100 = doc.IntValues.Any(x => x > 100),
                        CountEven = doc.IntValues.Count(x => x % 2 == 0)
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { IntValues = [2, 4, 102] }
            };

            // All > 0 : True
            // Any > 100 : True (102)
            // Count Even : 3

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.All), nameof(Enumerable.Any), nameof(Enumerable.Count));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where AllPositive = true and AnyGt100 = true and CountEven = 3").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_ElementAt_Indexer_IntArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        ValAt0 = doc.IntValues.ElementAt(0),
                        ValAtIndex1 = doc.IntValues[1],
                        ValAtDef = doc.IntValues.ElementAtOrDefault(5) // Should be default
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { IntValues = [10, 20] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.ElementAt), nameof(Enumerable.ElementAtOrDefault));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ValAt0 = 10 and ValAtIndex1 = 20 and ValAtDef = 0").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_ToDictionary_ToLookup_IntArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Key is value, Value is value*10.
                        DictCount = doc.IntValues.ToDictionary(k => k, v => v * 10).Count(),
                        // Lookup by Modulo 2. [1, 2, 3, 4] -> Keys: 1 (vals 1,3), 0 (vals 2,4)
                        LookupCount = doc.IntValues.ToLookup(k => k % 2).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { IntValues = [1, 2, 3, 4] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.ToDictionary), nameof(Enumerable.ToLookup));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // Dictionary should have 4 entries.
                        // Lookup should have 2 groups (Odd and Even).
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where DictCount = 4 and LookupCount = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        #endregion

        #region ULongs

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Intersect_ULongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasAny = doc.ULongValues.Intersect(new[] { 42UL, 100UL }).Any()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { ULongValues = [42UL, 7UL] },
                new DocWithLongs { ULongValues = [100UL] },
                new DocWithLongs { ULongValues = [1UL, 2UL, 3UL] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Intersect), nameof(Enumerable.Any));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where HasAny = true")
                            .ToList();

                        Assert.Equal(2, results.Count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Distinct_ULongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        DistinctCount = doc.ULongValues.Distinct().Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { ULongValues = [1UL, 1UL, 2UL] }, // 2 distinct
                new DocWithLongs { ULongValues = [3UL, 4UL, 5UL] } // 3 distinct
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Distinct)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where DistinctCount = 2")
                            .ToList();

                        Assert.Single(results);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_TakeSkip_ULongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Skip 1, Take 1. [10, 20, 30] -> [20] -> Cast to decimal to Sum
                        Value = doc.ULongValues.Skip(1).Take(1).Sum(x => (decimal)x)
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { ULongValues = [10UL, 20UL, 30UL] },
                new DocWithLongs { ULongValues = [5UL, 5UL] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Skip), nameof(Enumerable.Take), nameof(Enumerable.Sum));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Value = 20").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Concat_ULongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        AllValues = doc.ULongValues.Concat(new[] { 999UL }).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { ULongValues = [1UL, 2UL] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Concat)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where AllValues = 3").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_OrderBy_ULongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Smallest = doc.ULongValues.OrderBy(x => x).First(),
                        Largest = doc.ULongValues.OrderByDescending(x => x).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { ULongValues = [50UL, 10UL, 90UL] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.OrderBy), nameof(Enumerable.OrderByDescending));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Smallest = 10 and Largest = 90").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Sum_Average_Min_Max_WithSelector_ULongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        MinVal = doc.ULongValues.Min(x => x * 2), // 20, 40
                        MaxVal = doc.ULongValues.Max(x => x - 5), // 5, 15
                        // For Sum/Average on ulongs, it's safer to cast to decimal to avoid compilation issues or missing overloads
                        AvgVal = doc.ULongValues.Average(x => (decimal)x + 10), // 20, 30 -> 25
                        SumVal = doc.ULongValues.Sum(x => (decimal)x * 3) // 30, 60 -> 90
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { ULongValues = [10UL, 20UL] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Min), nameof(Enumerable.Max), nameof(Enumerable.Average), nameof(Enumerable.Sum));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where MinVal = 20 and MaxVal = 15 and AvgVal = 25 and SumVal = 90").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Reverse_ULongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        LastAfterReverse = doc.ULongValues.Reverse().Last()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { ULongValues = [1UL, 2UL, 3UL] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Reverse)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where LastAfterReverse = 1").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_TakeWhile_SkipWhile_ULongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        CountTaken = doc.ULongValues.TakeWhile(x => x < 10UL).Count(),
                        CountSkipped = doc.ULongValues.SkipWhile(x => x < 10UL).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { ULongValues = [1UL, 5UL, 20UL, 2UL] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.TakeWhile), nameof(Enumerable.SkipWhile));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // TakeWhile < 10: [1, 5] -> Count 2
                        // SkipWhile < 10: [20, 2] -> Count 2
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where CountTaken = 2 and CountSkipped = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Aggregate_ULongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // 0 + 1 + 2 + 3 = 6
                        SumAgg = doc.ULongValues.Aggregate(0UL, (acc, val) => acc + val)
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { ULongValues = [1UL, 2UL, 3UL] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Aggregate)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where SumAgg = 6").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Zip_ULongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Zip with self, sum pairs. [1,2] zip [1,2] -> [(1,1), (2,2)] -> sums [2, 4] -> First = 2
                        ZipFirst = doc.ULongValues.Zip(doc.ULongValues, (a, b) => a + b).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { ULongValues = [1UL, 2UL] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Zip)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ZipFirst = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_DefaultIfEmpty_ULongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Val = doc.ULongValues.DefaultIfEmpty(99UL).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { ULongValues = [] },
                new DocWithLongs { ULongValues = [1UL] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.DefaultIfEmpty)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var countDefault = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Val = 99").Count();
                        Assert.Equal(1, countDefault);

                        var countNormal = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Val = 1").Count();
                        Assert.Equal(1, countNormal);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_GroupBy_ULongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Group by Odd/Even (x % 2).
                        GroupsCount = doc.ULongValues.GroupBy(x => x % 2).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { ULongValues = [1UL, 2UL, 3UL, 4UL] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.GroupBy)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where GroupsCount = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_First_Last_Single_WithPredicate_ULongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        FirstGt = doc.ULongValues.First(x => x > 10),
                        LastGt = doc.ULongValues.Last(x => x > 10),
                        SingleVal = doc.ULongValues.Single(x => x == 20),
                        SingleOrDefaultVal = doc.ULongValues.SingleOrDefault(x => x == 999)
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { ULongValues = [5UL, 20UL, 50UL] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.First), nameof(Enumerable.Last), nameof(Enumerable.Single), nameof(Enumerable.SingleOrDefault));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where FirstGt = 20 and LastGt = 50 and SingleVal = 20").ToList();
                        Assert.Single(results);

                        var countNull = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where SingleOrDefaultVal = null").Count();
                        if (countNull == 0)
                        {
                            var countZero = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where SingleOrDefaultVal = 0").Count();
                            Assert.Equal(1, countZero);
                        }
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Any_All_Count_WithPredicate_ULongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        AllPositive = doc.ULongValues.All(x => x > 0),
                        AnyGt100 = doc.ULongValues.Any(x => x > 100),
                        CountEven = doc.ULongValues.Count(x => x % 2 == 0)
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { ULongValues = [2UL, 4UL, 102UL] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.All), nameof(Enumerable.Any), nameof(Enumerable.Count));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where AllPositive = true and AnyGt100 = true and CountEven = 3").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_ElementAt_Indexer_ULongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        ValAt0 = doc.ULongValues.ElementAt(0),
                        ValAtIndex1 = doc.ULongValues[1],
                        ValAtDef = doc.ULongValues.ElementAtOrDefault(5)
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { ULongValues = [10UL, 20UL] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.ElementAt), nameof(Enumerable.ElementAtOrDefault));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ValAt0 = 10 and ValAtIndex1 = 20 and ValAtDef = 0").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_ToDictionary_ToLookup_ULongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Key is value, Value is value*10.
                        DictCount = doc.ULongValues.ToDictionary(k => k, v => v * 10).Count(),
                        // Lookup by Modulo 2.
                        LookupCount = doc.ULongValues.ToLookup(k => k % 2).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { ULongValues = [1UL, 2UL, 3UL, 4UL] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.ToDictionary), nameof(Enumerable.ToLookup));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where DictCount = 4 and LookupCount = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        #endregion

        #region Floats

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Intersect_FloatArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithFloats, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasAny = doc.Values.Intersect(new[] { 1.5f, 9.9f }).Any()
                    }
            };

            var docs = new object[]
            {
                new DocWithFloats { Values = [1.5f, 2.5f] },
                new DocWithFloats { Values = [3.5f] },
                new DocWithFloats { Values = [9.9f] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Intersect), nameof(Enumerable.Any));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where HasAny = true")
                            .ToList();

                        Assert.Equal(2, results.Count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Contains_FloatArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithFloats, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasValue = doc.Values.Contains(1.5f)
                    }
            };

            var docs = new object[]
            {
                new DocWithFloats { Values = [1.5f, 2.5f] },
                new DocWithFloats { Values = [3.5f] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Contains)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where HasValue = true")
                            .Count();

                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Distinct_FloatArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithFloats, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        DistinctCount = doc.Values.Distinct().Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithFloats { Values = [1.5f, 1.5f, 2.5f] }, // 2 distinct
                new DocWithFloats { Values = [3.5f, 4.5f, 5.5f] } // 3 distinct
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Distinct)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where DistinctCount = 2")
                            .ToList();

                        Assert.Single(results);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Concat_FloatArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithFloats, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        TotalLen = doc.Values.Concat(new[] { 0.5f }).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithFloats { Values = [1.5f, 2.5f] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Concat)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where TotalLen = 3").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_OrderBy_FloatArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithFloats, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Smallest = doc.Values.OrderBy(x => x).First(),
                        Largest = doc.Values.OrderByDescending(x => x).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithFloats { Values = [3.0f, 1.0f, 2.0f] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.OrderBy), nameof(Enumerable.OrderByDescending));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Smallest = 1.0 and Largest = 3.0").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Sum_Average_Min_Max_WithSelector_FloatArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithFloats, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        MinVal = doc.Values.Min(x => x * 2),
                        MaxVal = doc.Values.Max(x => x - 0.5f),
                        AvgVal = doc.Values.Average(x => x + 10),
                        SumVal = doc.Values.Sum(x => x * 3)
                    }
            };

            var docs = new object[]
            {
                new DocWithFloats { Values = [10.0f, 20.0f] }
            };

            // Expected:
            // Min: (10*2)=20, (20*2)=40 -> Min 20
            // Max: (10-0.5)=9.5, (20-0.5)=19.5 -> Max 19.5
            // Avg: (10+10)=20, (20+10)=30 -> Avg 25
            // Sum: (10*3)=30, (20*3)=60 -> Sum 90

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Min), nameof(Enumerable.Max), nameof(Enumerable.Average), nameof(Enumerable.Sum));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where MinVal = 20 and MaxVal = 19.5 and AvgVal = 25 and SumVal = 90").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Reverse_FloatArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithFloats, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        LastAfterReverse = doc.Values.Reverse().Last()
                    }
            };

            var docs = new object[]
            {
                new DocWithFloats { Values = [1.1f, 2.2f, 3.3f] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Reverse)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where LastAfterReverse > 1.0 and LastAfterReverse < 1.2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_TakeWhile_SkipWhile_FloatArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithFloats, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        CountTaken = doc.Values.TakeWhile(x => x < 5.0f).Count(),
                        CountSkipped = doc.Values.SkipWhile(x => x < 5.0f).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithFloats { Values = [1.0f, 2.0f, 10.0f, 1.0f] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.TakeWhile), nameof(Enumerable.SkipWhile));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // TakeWhile < 5: [1.0, 2.0] -> Count 2
                        // SkipWhile < 5: [10.0, 1.0] -> Count 2
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where CountTaken = 2 and CountSkipped = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Aggregate_FloatArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithFloats, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        SumAgg = doc.Values.Aggregate(0.0f, (acc, val) => acc + val)
                    }
            };

            var docs = new object[]
            {
                new DocWithFloats { Values = [1.0f, 2.0f, 3.0f] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Aggregate)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where SumAgg = 6.0").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Zip_FloatArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithFloats, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        ZipSum = doc.Values.Zip(doc.Values, (a, b) => a + b).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithFloats { Values = [1.0f, 2.0f] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Zip)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // 1.0 + 1.0 = 2.0
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ZipSum = 2.0").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_DefaultIfEmpty_FloatArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithFloats, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Val = doc.Values.DefaultIfEmpty(9.9f).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithFloats { Values = [] },
                new DocWithFloats { Values = [1.1f] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.DefaultIfEmpty)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var countDefault = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Val > 9.8 and Val < 10.0").Count();
                        Assert.Equal(1, countDefault);

                        var countNormal = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Val > 1.0 and Val < 1.2").Count();
                        Assert.Equal(1, countNormal);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_GroupBy_FloatArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithFloats, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Group by whole number part.
                        // [1.1, 1.9, 2.5, 2.9] -> Group 1 (2 items), Group 2 (2 items).
                        GroupsCount = doc.Values.GroupBy(x => (int)x).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithFloats { Values = [1.1f, 1.9f, 2.5f, 2.9f] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.GroupBy)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where GroupsCount = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_First_Last_Single_WithPredicate_FloatArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithFloats, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        FirstGt = doc.Values.First(x => x > 2.0f),
                        LastGt = doc.Values.Last(x => x > 2.0f),
                        SingleVal = doc.Values.Single(x => x > 4.0f), // Only 5.0 matches
                        SingleOrDefaultVal = doc.Values.SingleOrDefault(x => x > 10.0f) // Should be null/default
                    }
            };

            var docs = new object[]
            {
                new DocWithFloats { Values = [1.0f, 3.0f, 5.0f] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.First), nameof(Enumerable.Last), nameof(Enumerable.Single), nameof(Enumerable.SingleOrDefault));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where FirstGt = 3.0 and LastGt = 5.0 and SingleVal = 5.0").ToList();
                        Assert.Single(results);

                        // Check default
                        var countNull = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where SingleOrDefaultVal = null or SingleOrDefaultVal = 0.0").Count();
                        Assert.Equal(1, countNull);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Any_All_Count_WithPredicate_FloatArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithFloats, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        AllPositive = doc.Values.All(x => x > 0f),
                        AnyGt5 = doc.Values.Any(x => x > 5.0f),
                        CountGt1 = doc.Values.Count(x => x > 1.0f)
                    }
            };

            var docs = new object[]
            {
                new DocWithFloats { Values = [1.0f, 6.0f] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.All), nameof(Enumerable.Any), nameof(Enumerable.Count));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where AllPositive = true and AnyGt5 = true and CountGt1 = 1").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_ElementAt_Indexer_FloatArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithFloats, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        ValAt0 = doc.Values.ElementAt(0),
                        ValAtIndex1 = doc.Values[1],
                        ValAtDef = doc.Values.ElementAtOrDefault(5)
                    }
            };

            var docs = new object[]
            {
                new DocWithFloats { Values = [10.5f, 20.5f] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.ElementAt), nameof(Enumerable.ElementAtOrDefault));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ValAt0 = 10.5 and ValAtIndex1 = 20.5 and ValAtDef = 0.0").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_ToDictionary_ToLookup_FloatArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithFloats, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Key x, Value x+1
                        DictCount = doc.Values.ToDictionary(k => k, v => v + 1.0f).Count(),
                        // Lookup by integer part
                        LookupCount = doc.Values.ToLookup(k => (int)k).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithFloats { Values = [1.1f, 1.9f, 2.5f] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.ToDictionary), nameof(Enumerable.ToLookup));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // Dictionary: 3 items
                        // Lookup: 1 (for 1.1, 1.9), 2 (for 2.5) -> 2 groups
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where DictCount = 3 and LookupCount = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        #endregion

        #region Doubles

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Intersect_DoubleArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDoubles, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasAny = doc.Values.Intersect(new[] { 1.1, 9.9 }).Any()
                    }
            };

            var docs = new object[]
            {
                new DocWithDoubles { Values = [1.1, 2.2] },
                new DocWithDoubles { Values = [9.9] },
                new DocWithDoubles { Values = [3.3] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Intersect), nameof(Enumerable.Any));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where HasAny = true")
                            .ToList();

                        Assert.Equal(2, results.Count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Union_DoubleArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDoubles, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasUnionValue = doc.Values
                            .Union(new[] { 3.3 })
                            .Contains(3.3)
                    }
            };

            var docs = new object[]
            {
                new DocWithDoubles { Values = [1.1] },
                new DocWithDoubles { Values = [3.3] },
                new DocWithDoubles { Values = [4.4] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Union), nameof(Enumerable.Contains));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where HasUnionValue = true")
                            .ToList();

                        Assert.Equal(3, results.Count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Contains_DoubleArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDoubles, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasValue = doc.Values.Contains(1.1)
                    }
            };

            var docs = new object[]
            {
                new DocWithDoubles { Values = [1.1, 2.2] },
                new DocWithDoubles { Values = [3.3] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Contains)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where HasValue = true")
                            .Count();

                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Distinct_DoubleArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDoubles, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        DistinctCount = doc.Values.Distinct().Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithDoubles { Values = [1.1, 1.1, 2.2] }, // 2 distinct
                new DocWithDoubles { Values = [3.3, 4.4, 5.5] } // 3 distinct
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Distinct)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where DistinctCount = 2")
                            .ToList();

                        Assert.Single(results);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Concat_DoubleArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDoubles, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        TotalLen = doc.Values.Concat(new[] { 0.1 }).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithDoubles { Values = [1.1, 2.2] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Concat)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where TotalLen = 3").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_OrderBy_DoubleArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDoubles, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Smallest = doc.Values.OrderBy(x => x).First(),
                        Largest = doc.Values.OrderByDescending(x => x).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithDoubles { Values = [3.3, 1.1, 2.2] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.OrderBy), nameof(Enumerable.OrderByDescending));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Smallest = 1.1 and Largest = 3.3").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Reverse_DoubleArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDoubles, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        LastAfterReverse = doc.Values.Reverse().Last()
                    }
            };

            var docs = new object[]
            {
                new DocWithDoubles { Values = [1.1, 2.2, 3.3] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Reverse)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // [1.1, 2.2, 3.3] -> Reverse [3.3, 2.2, 1.1] -> Last is 1.1
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where LastAfterReverse = 1.1").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_TakeWhile_SkipWhile_DoubleArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDoubles, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        CountTaken = doc.Values.TakeWhile(x => x < 3.0).Count(),
                        CountSkipped = doc.Values.SkipWhile(x => x < 3.0).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithDoubles { Values = [1.1, 2.2, 5.5, 1.1] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.TakeWhile), nameof(Enumerable.SkipWhile));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // TakeWhile < 3.0: [1.1, 2.2] -> Count 2
                        // SkipWhile < 3.0: [5.5, 1.1] -> Count 2
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where CountTaken = 2 and CountSkipped = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Aggregate_DoubleArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDoubles, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        SumAgg = doc.Values.Aggregate(0.0, (acc, val) => acc + val)
                    }
            };

            var docs = new object[]
            {
                new DocWithDoubles { Values = [1.0, 2.0, 3.0] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Aggregate)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where SumAgg = 6.0").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Zip_DoubleArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDoubles, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        ZipSum = doc.Values.Zip(doc.Values, (a, b) => a + b).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithDoubles { Values = [1.0, 2.0] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Zip)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ZipSum = 2.0").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_DefaultIfEmpty_DoubleArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDoubles, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Val = doc.Values.DefaultIfEmpty(9.9).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithDoubles { Values = [] },
                new DocWithDoubles { Values = [1.1] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.DefaultIfEmpty)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var countDefault = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Val = 9.9").Count();
                        Assert.Equal(1, countDefault);

                        var countNormal = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Val = 1.1").Count();
                        Assert.Equal(1, countNormal);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_First_Last_Single_WithPredicate_DoubleArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDoubles, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        FirstGt = doc.Values.First(x => x > 2.0),
                        LastGt = doc.Values.Last(x => x > 2.0),
                        // ReSharper disable once CompareOfFloatsByEqualityOperator
                        SingleVal = doc.Values.Single(x => x == 3.3),
                        // ReSharper disable once CompareOfFloatsByEqualityOperator
                        SingleOrDefaultVal = doc.Values.SingleOrDefault(x => x == 99.9) // Should be default
                    }
            };

            var docs = new object[]
            {
                new DocWithDoubles { Values = [1.1, 3.3, 5.5] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.First), nameof(Enumerable.Last), nameof(Enumerable.Single), nameof(Enumerable.SingleOrDefault));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // First > 2.0 is 3.3
                        // Last > 2.0 is 5.5
                        // Single == 3.3 is 3.3

                        var results = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where FirstGt = 3.3 and LastGt = 5.5 and SingleVal = 3.3").ToList();
                        Assert.Single(results);

                        var countNull = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where SingleOrDefaultVal = null").Count();
                        if (countNull == 0)
                        {
                            var countZero = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where SingleOrDefaultVal = 0").Count();
                            Assert.Equal(1, countZero);
                        }
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Any_All_Count_WithPredicate_DoubleArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDoubles, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        AllPositive = doc.Values.All(x => x > 0),
                        AnyGt5 = doc.Values.Any(x => x > 5.0),
                        CountGt2 = doc.Values.Count(x => x > 2.0)
                    }
            };

            var docs = new object[]
            {
                new DocWithDoubles { Values = [1.1, 3.3, 6.6] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.All), nameof(Enumerable.Any), nameof(Enumerable.Count));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where AllPositive = true and AnyGt5 = true and CountGt2 = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_ElementAt_Indexer_DoubleArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDoubles, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        ValAt0 = doc.Values.ElementAt(0),
                        ValAtIndex1 = doc.Values[1],
                        ValAtDef = doc.Values.ElementAtOrDefault(5) // Should be default
                    }
            };

            var docs = new object[]
            {
                new DocWithDoubles { Values = [1.1, 2.2] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.ElementAt), nameof(Enumerable.ElementAtOrDefault));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ValAt0 = 1.1 and ValAtIndex1 = 2.2 and ValAtDef = 0").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        #endregion

        #region Decimals

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Intersect_DecimalArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDecimals, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasAny = doc.Values.Intersect(new[] { 1.1m, 9.9m }).Any()
                    }
            };

            var docs = new object[]
            {
                new DocWithDecimals { Values = [1.1m, 2.2m] },
                new DocWithDecimals { Values = [9.9m] },
                new DocWithDecimals { Values = [3.3m] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Intersect), nameof(Enumerable.Any));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where HasAny = true").Count();
                        Assert.Equal(2, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Union_DecimalArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDecimals, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasUnionValue = doc.Values.Union(new[] { 3.3m }).Contains(3.3m)
                    }
            };

            var docs = new object[]
            {
                new DocWithDecimals { Values = [1.1m] },
                new DocWithDecimals { Values = [3.3m] },
                new DocWithDecimals { Values = [4.4m] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Union), nameof(Enumerable.Contains));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where HasUnionValue = true").Count();
                        Assert.Equal(3, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Concat_DecimalArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDecimals, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        TotalLen = doc.Values.Concat(new[] { 9.9m }).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithDecimals { Values = [1.1m] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Concat)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where TotalLen = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Contains_Distinct_DecimalArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDecimals, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasVal = doc.Values.Contains(2.2m),
                        UniqueCount = doc.Values.Distinct().Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithDecimals { Values = [1.1m, 2.2m, 1.1m] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Contains), nameof(Enumerable.Distinct));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where HasVal = true and UniqueCount = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Sum_Average_Min_Max_WithSelector_DecimalArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDecimals, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        MinVal = doc.Values.Min(x => x * 2m),
                        MaxVal = doc.Values.Max(x => x - 5m),
                        AvgVal = doc.Values.Average(x => x + 10m),
                        SumVal = doc.Values.Sum(x => x * 3m)
                    }
            };

            var docs = new object[]
            {
                new DocWithDecimals { Values = [10.0m, 20.0m] }
            };

            // Expected:
            // Min: (10*2)=20, (20*2)=40 -> Min 20.0
            // Max: (10-5)=5, (20-5)=15 -> Max 15.0
            // Avg: (10+10)=20, (20+10)=30 -> Avg 25.0
            // Sum: (10*3)=30, (20*3)=60 -> Sum 90.0

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Min), nameof(Enumerable.Max), nameof(Enumerable.Average), nameof(Enumerable.Sum));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where MinVal = 20.0 and MaxVal = 15.0 and AvgVal = 25.0 and SumVal = 90.0").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Where_Select_DecimalArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDecimals, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        CountBig = doc.Values.Where(x => x > 5.0m).Select(x => x * 2).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithDecimals { Values = [1.1m, 6.6m, 7.7m] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Where), nameof(Enumerable.Select));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where CountBig = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Reverse_DecimalArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDecimals, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        LastAfterReverse = doc.Values.Reverse().Last()
                    }
            };

            var docs = new object[]
            {
                new DocWithDecimals { Values = [1.1m, 2.2m, 3.3m] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Reverse)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where LastAfterReverse = 1.1").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Take_Skip_DecimalArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDecimals, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Val = doc.Values.Skip(1).Take(1).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithDecimals { Values = [1.1m, 2.2m, 3.3m] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Skip), nameof(Enumerable.Take));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Val = 2.2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_TakeWhile_SkipWhile_DecimalArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDecimals, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        CountTaken = doc.Values.TakeWhile(x => x < 5.0m).Count(),
                        CountSkipped = doc.Values.SkipWhile(x => x < 5.0m).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithDecimals { Values = [1.1m, 2.2m, 6.6m, 1.0m] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.TakeWhile), nameof(Enumerable.SkipWhile));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // TakeWhile < 5.0: [1.1, 2.2] -> Count 2
                        // SkipWhile < 5.0: [6.6, 1.0] -> Count 2
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where CountTaken = 2 and CountSkipped = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Aggregate_DecimalArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDecimals, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        SumAgg = doc.Values.Aggregate(0.0m, (acc, val) => acc + val)
                    }
            };

            var docs = new object[]
            {
                new DocWithDecimals { Values = [1.0m, 2.0m, 3.0m] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Aggregate)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where SumAgg = 6.0").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Zip_DecimalArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDecimals, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        ZipSum = doc.Values.Zip(doc.Values, (a, b) => a + b).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithDecimals { Values = [1.0m, 2.0m] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Zip)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ZipSum = 2.0").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Any_All_Count_WithPredicate_DecimalArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDecimals, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasBig = doc.Values.Any(x => x > 5.0m),
                        AllPos = doc.Values.All(x => x > 0m),
                        CountPos = doc.Values.Count(x => x > 1.5m)
                    }
            };

            var docs = new object[]
            {
                new DocWithDecimals { Values = [1.0m, 6.0m] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Any), nameof(Enumerable.All), nameof(Enumerable.Count));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where HasBig = true and AllPos = true and CountPos = 1").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_First_Last_Single_WithPredicate_DecimalArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDecimals, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        FirstVal = doc.Values.First(x => x > 2.0m),
                        LastVal = doc.Values.Last(x => x > 2.0m),
                        SingleVal = doc.Values.Single(x => x == 2.2m),
                        SingleOrDefaultVal = doc.Values.SingleOrDefault(x => x == 9.9m)
                    }
            };

            var docs = new object[]
            {
                new DocWithDecimals { Values = [1.1m, 2.2m, 3.3m] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.First), nameof(Enumerable.Last), nameof(Enumerable.Single), nameof(Enumerable.SingleOrDefault));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where FirstVal = 2.2 and LastVal = 3.3 and SingleVal = 2.2").ToList();
                        Assert.Equal(1, results.Count);

                        // SingleOrDefault null/0 check
                        var countZero = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where SingleOrDefaultVal = 0 or SingleOrDefaultVal = null").Count();
                        Assert.Equal(1, countZero);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_ElementAt_Indexer_DecimalArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDecimals, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        ValAt1 = doc.Values.ElementAt(1),
                        ValAtIdx = doc.Values[0],
                        ValDef = doc.Values.ElementAtOrDefault(5)
                    }
            };

            var docs = new object[]
            {
                new DocWithDecimals { Values = [1.0m, 2.0m, 3.0m] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.ElementAt), nameof(Enumerable.ElementAtOrDefault));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ValAt1 = 2.0 and ValAtIdx = 1.0 and ValDef = 0").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_OrderBy_DecimalArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDecimals, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Smallest = doc.Values.OrderBy(x => x).First(),
                        Largest = doc.Values.OrderByDescending(x => x).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithDecimals { Values = [3.0m, 1.0m, 2.0m] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.OrderBy), nameof(Enumerable.OrderByDescending));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Smallest = 1.0 and Largest = 3.0").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_GroupBy_DecimalArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDecimals, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Group by integer part. [1.1, 1.9, 2.5] -> Key 1 (2 items), Key 2 (1 item)
                        GroupsCount = doc.Values.GroupBy(x => (int)x).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithDecimals { Values = [1.1m, 1.9m, 2.5m] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.GroupBy)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where GroupsCount = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_ToDictionary_ToLookup_DecimalArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDecimals, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        DictCount = doc.Values.ToDictionary(k => k, v => v * 10).Count(),
                        // Lookup by integer part. [1.1, 1.9, 2.5] -> Keys: 1, 2
                        LookupCount = doc.Values.ToLookup(k => (int)k).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithDecimals { Values = [1.1m, 1.9m, 2.5m] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.ToDictionary), nameof(Enumerable.ToLookup));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where DictCount = 3 and LookupCount = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_DefaultIfEmpty_DecimalArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDecimals, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Val = doc.Values.DefaultIfEmpty(9.9m).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithDecimals { Values = [] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.DefaultIfEmpty)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Val = 9.9").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        #endregion

        #region Strings

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Intersect_StringArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithStrings, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasAny = doc.Tags.Intersect(new[] { "a", "z" }).Any()
                    }
            };

            var docs = new object[]
            {
                new DocWithStrings { Tags = ["a", "b"] },
                new DocWithStrings { Tags = ["z"] },
                new DocWithStrings { Tags = ["x", "y"] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Intersect), nameof(Enumerable.Any));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where HasAny = true")
                            .ToList();

                        Assert.Equal(2, results.Count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_SequenceEqual_StringArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithStrings, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        IsExact = doc.Tags.SequenceEqual(new[] { "a", "b" })
                    }
            };

            var docs = new object[]
            {
                new DocWithStrings { Tags = ["a", "b"] },
                new DocWithStrings { Tags = ["a"] },
                new DocWithStrings { Tags = ["b", "a"] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                    AssertMapContains(map, nameof(Enumerable.SequenceEqual)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where IsExact = true")
                            .ToList();

                        Assert.Single(results);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Except_StringArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithStrings, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasRemaining = doc.Tags
                            .Except(new[] { "a" })
                            .Contains("b")
                    }
            };

            var docs = new object[]
            {
                new DocWithStrings { Tags = ["a", "b"] },
                new DocWithStrings { Tags = ["a"] },
                new DocWithStrings { Tags = ["c"] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Except), nameof(Enumerable.Contains));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where HasRemaining = true")
                            .ToList();

                        Assert.Single(results);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Union_StringArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithStrings, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasUnionValue = doc.Tags
                            .Union(new[] { "c" })
                            .Contains("c")
                    }
            };

            var docs = new object[]
            {
                new DocWithStrings { Tags = ["a"] },
                new DocWithStrings { Tags = ["c"] },
                new DocWithStrings { Tags = ["d"] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Union), nameof(Enumerable.Contains));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where HasUnionValue = true")
                            .ToList();

                        Assert.Equal(3, results.Count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Contains_StringArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithStrings, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasValue = doc.Tags.Contains("b")
                    }
            };

            var docs = new object[]
            {
                new DocWithStrings { Tags = ["a", "b"] },
                new DocWithStrings { Tags = ["a", "c"] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Contains)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where HasValue = true")
                            .Count();

                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Distinct_StringArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithStrings, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        DistinctCount = doc.Tags.Distinct().Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithStrings { Tags = ["a", "a", "b"] }, // 2 distinct
                new DocWithStrings { Tags = ["a", "b", "c"] } // 3 distinct
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Distinct)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where DistinctCount = 2")
                            .ToList();

                        Assert.Single(results);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Reverse_StringArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithStrings, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        FirstAfterReverse = doc.Tags.Reverse().First()
                    }
            };

            var docs = new object[]
            {
                new DocWithStrings { Tags = ["a", "z"] },
                new DocWithStrings { Tags = ["b", "y"] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Reverse)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // Reverse of ["a", "z"] is ["z", "a"], First is "z"
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where FirstAfterReverse = 'z'").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Concat_StringArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithStrings, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        TotalLen = doc.Tags.Concat(new[] { "c" }).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithStrings { Tags = ["a", "b"] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Concat)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where TotalLen = 3").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MinMax_StringArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithStrings, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        MinTag = doc.Tags.Min(),
                        MaxTag = doc.Tags.Max()
                    }
            };

            var docs = new object[]
            {
                new DocWithStrings { Tags = ["apple", "banana", "cherry"] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Min), nameof(Enumerable.Max));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where MinTag = 'apple' and MaxTag = 'cherry'").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Where_StringArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithStrings, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // StartsWith 'a'
                        // ReSharper disable once ReplaceWithSingleCallToCount
                        ACount = doc.Tags.Where(x => x.StartsWith("a")).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithStrings { Tags = ["apple", "apricot", "banana"] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Where)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ACount = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_OrderBy_StringArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithStrings, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        SortedLast = doc.Tags.OrderBy(x => x).Last()
                    }
            };

            var docs = new object[]
            {
                new DocWithStrings { Tags = ["zebra", "ant", "bat"] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.OrderBy)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where SortedLast = 'zebra'").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Take_Skip_StringArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithStrings, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Val = doc.Tags.Skip(1).Take(1).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithStrings { Tags = ["a", "b", "c"] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Skip), nameof(Enumerable.Take));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Val = 'b'").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Aggregate_StringArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithStrings, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Concatenated = doc.Tags.Aggregate("", (acc, val) => acc + val)
                    }
            };

            var docs = new object[]
            {
                new DocWithStrings { Tags = ["a", "b", "c"] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Aggregate)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Concatenated = 'abc'").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Zip_StringArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithStrings, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        ZipVal = doc.Tags.Zip(doc.Tags, (a, b) => a + b).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithStrings { Tags = ["a", "b"] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Zip)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ZipVal = 'aa'").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_ElementAt_Indexer_StringArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithStrings, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        ValAtIndex = doc.Tags[1],
                        ValAt = doc.Tags.ElementAt(1),
                        ValAtDef = doc.Tags.ElementAtOrDefault(3)
                    }
            };

            var docs = new object[]
            {
                new DocWithStrings { Tags = ["a", "b", "c"] },
                new DocWithStrings { Tags = ["a", "b", "c", "d"] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.ElementAt), nameof(Enumerable.ElementAtOrDefault));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ValAtIndex = 'b' and ValAt = 'b' and not exists(ValAtDef)").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_GroupBy_StringArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithStrings, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        GroupCount = doc.Tags.GroupBy(x => x.Length).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithStrings { Tags = ["a", "bb", "c", "dd"] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.GroupBy)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // Length 1: a, c
                        // Length 2: bb, dd
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where GroupCount = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_DefaultIfEmpty_StringArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithStrings, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Val = doc.Tags.DefaultIfEmpty("default").First()
                    }
            };

            var docs = new object[]
            {
                new DocWithStrings { Tags = [] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.DefaultIfEmpty)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Val = 'default'").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_First_Last_Single_WithPredicate_StringArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithStrings, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        FirstMatch = doc.Tags.First(x => x.StartsWith("b")),
                        LastMatch = doc.Tags.Last(x => x.Length == 3),
                        SingleMatch = doc.Tags.Single(x => x == "apple"),
                        SingleDef = doc.Tags.SingleOrDefault(x => x == "nonexistent")
                    }
            };

            var docs = new object[]
            {
                new DocWithStrings { Tags = ["apple", "bat", "bar", "banana"] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                                        AssertMapContains(map, nameof(Enumerable.First), nameof(Enumerable.Last), nameof(Enumerable.Single), nameof(Enumerable.SingleOrDefault));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var firstMatchResult = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where FirstMatch = 'bat'").Count();
                        Assert.Equal(1, firstMatchResult);

                        var lastMatchResult = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where LastMatch = 'bar'").Count();
                        Assert.Equal(1, lastMatchResult);

                        var singleMatchResult = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where SingleMatch = 'apple'").Count();
                        Assert.Equal(1, singleMatchResult);

                        var singleDefResult = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where true and not exists(SingleDef)").Count();
                        Assert.Equal(1, singleDefResult);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Any_All_Count_WithPredicate_StringArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithStrings, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        AllStartWithA = doc.Tags.All(x => x.StartsWith("a")),
                        AnyLen3 = doc.Tags.Any(x => x.Length == 3),
                        CountLen5 = doc.Tags.Count(x => x.Length == 5)
                    }
            };

            var docs = new object[]
            {
                new DocWithStrings { Tags = ["apple", "ant", "apply"] } // apple(5), ant(3), apply(5)
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.All), nameof(Enumerable.Any), nameof(Enumerable.Count));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where AllStartWithA = true and AnyLen3 = true and CountLen5 = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Sum_Average_Min_Max_WithSelector_StringArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithStrings, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        MinLen = doc.Tags.Min(x => x.Length),
                        MaxLen = doc.Tags.Max(x => x.Length),
                        AvgLen = doc.Tags.Average(x => x.Length),
                        SumLen = doc.Tags.Sum(x => x.Length)
                    }
            };

            var docs = new object[]
            {
                new DocWithStrings { Tags = ["a", "bb"] } // 1, 2
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Min), nameof(Enumerable.Max), nameof(Enumerable.Average), nameof(Enumerable.Sum));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var result = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where MinLen == 1 and MaxLen == 2 and AvgLen == 1.5 and SumLen == 3").Count();
                        Assert.Equal(1, result);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_TakeWhile_SkipWhile_StringArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithStrings, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Taken = doc.Tags.TakeWhile(x => x.Length < 4).Count(),
                        Skipped = doc.Tags.SkipWhile(x => x.Length < 4).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithStrings { Tags = ["ant", "bat", "bear", "cat"] }
                // TakeWhile < 4: ant(3), bat(3) -> Stop at bear(4). Count 2.
                // SkipWhile < 4: skip ant, bat. Remain bear, cat. Count 2.
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.TakeWhile), nameof(Enumerable.SkipWhile));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Taken = 2 and Skipped = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_ToDictionary_ToLookup_StringArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithStrings, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Key is string, Value is Length.
                        DictCount = doc.Tags.ToDictionary(k => k, v => v.Length).Count(),
                        // Lookup by first char.
                        LookupCount = doc.Tags.ToLookup(k => k[0]).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithStrings { Tags = ["apple", "banana", "ant"] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.ToDictionary), nameof(Enumerable.ToLookup));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // Dictionary: 3 items.
                        // Lookup: 'a' (apple, ant), 'b' (banana) -> 2 groups.
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where DictCount = 3 and LookupCount = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        #endregion

        #region DateTime

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Intersect_DateTimeArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasCommon = doc.ImportantDates.Intersect(new[] { new DateTime(2023, 1, 1), new DateTime(2023, 12, 31) }).Any()
                    }
            };

            var docs = new object[]
            {
                new DocWithDates { ImportantDates = [new DateTime(2023, 1, 1), new DateTime(2023, 5, 5)] }, // Has date1
                new DocWithDates { ImportantDates = [new DateTime(2023, 5, 5)] }, // No intersection
                new DocWithDates { ImportantDates = [new DateTime(2023, 12, 31)] } // Has date3
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Intersect), nameof(Enumerable.Any));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where HasCommon = true").Count();
                        Assert.Equal(2, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Union_DateTimeArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasUnionValue = doc.ImportantDates.Union(new[] { new DateTime(2099, 12, 31) }).Contains(new DateTime(2099, 12, 31))
                    }
            };

            var docs = new object[]
            {
                new DocWithDates { ImportantDates = [new DateTime(2000, 1, 1)] }, // Will have date2 after Union
                new DocWithDates { ImportantDates = [new DateTime(2099, 12, 31)] } // Already has it
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Union), nameof(Enumerable.Contains));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where HasUnionValue = true").Count();
                        Assert.Equal(2, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Contains_DateTimeArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasDate = doc.ImportantDates.Contains(new DateTime(2022, 2, 24))
                    }
            };

            var docs = new object[]
            {
                new DocWithDates { ImportantDates = [new DateTime(2020, 1, 1), new DateTime(2022, 2, 24)] },
                new DocWithDates { ImportantDates = [new DateTime(2020, 1, 1)] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Contains)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where HasDate = true").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Distinct_DateTimeArray_ShouldWork(Options options)
        {
            var date = new DateTime(2020, 1, 1);

            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        DistinctCount = doc.ImportantDates.Distinct().Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithDates { ImportantDates = [date, date, date.AddDays(1)] }, // 2 distinct
                new DocWithDates { ImportantDates = [date, date] } // 1 distinct
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Distinct)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where DistinctCount = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Concat_DateTimeArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        TotalCount = doc.ImportantDates.Concat(new[] { new DateTime(2025, 1, 1) }).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithDates { ImportantDates = [new DateTime(2020, 1, 1)] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Concat)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where TotalCount = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Aggregate_MinMax_DateTimeArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Earliest = doc.ImportantDates.Min(),
                        Latest = doc.ImportantDates.Max()
                    }
            };

            var d1 = new DateTime(1990, 1, 1);
            var d2 = new DateTime(2000, 1, 1);
            var d3 = new DateTime(2010, 1, 1);

            var docs = new object[]
            {
                new DocWithDates { ImportantDates = [d2, d1, d3] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Min), nameof(Enumerable.Max));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // Using ISO string format for query comparison is usually safest for dates
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Earliest = '1990-01-01T00:00:00.0000000' and Latest = '2010-01-01T00:00:00.0000000'").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Min_Max_Average_Sum_WithSelector_DateTimeArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        MinYear = doc.ImportantDates.Min(x => x.Year),
                        MaxYear = doc.ImportantDates.Max(x => x.Year),
                        AvgYear = doc.ImportantDates.Average(x => x.Year),
                        SumYear = doc.ImportantDates.Sum(x => x.Year)
                    }
            };

            var docs = new object[]
            {
                new DocWithDates { ImportantDates = [new DateTime(2000, 1, 1), new DateTime(2010, 1, 1)] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Min), nameof(Enumerable.Max), nameof(Enumerable.Average), nameof(Enumerable.Sum));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // Min: 2000, Max: 2010, Avg: 2005, Sum: 4010
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where MinYear = 2000 and MaxYear = 2010 and AvgYear = 2005 and SumYear = 4010").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Any_All_Count_WithPredicate_DateTimeArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasFuture = doc.ImportantDates.Any(x => x.Year > 2050),
                        AllPast = doc.ImportantDates.All(x => x.Year < 2000),
                        Count20thCentury = doc.ImportantDates.Count(x => x.Year >= 1900 && x.Year < 2000)
                    }
            };

            var docs = new object[]
            {
                new DocWithDates { ImportantDates = [new DateTime(2060, 1, 1), new DateTime(1990, 1, 1)] }, // HasFuture=true, AllPast=false, Count20th=1
                new DocWithDates { ImportantDates = [new DateTime(1990, 1, 1), new DateTime(1995, 1, 1)] } // HasFuture=false, AllPast=true, Count20th=2
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Any), nameof(Enumerable.All), nameof(Enumerable.Count));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var res1 = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where HasFuture = true and AllPast = false and Count20thCentury = 1").Count();
                        Assert.Equal(1, res1);

                        var res2 = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where HasFuture = false and AllPast = true and Count20thCentury = 2").Count();
                        Assert.Equal(1, res2);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_First_Last_Single_WithPredicate_DateTimeArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        FirstVal = doc.ImportantDates.First(x => x.Year > 2000),
                        LastVal = doc.ImportantDates.Last(x => x.Year < 2005),
                        SingleVal = doc.ImportantDates.Single(x => x.Year == 2010),
                        SingleOrDefaultVal = doc.ImportantDates.SingleOrDefault(x => x.Year == 1999)
                    }
            };

            var docs = new object[]
            {
                new DocWithDates { ImportantDates = [new DateTime(2000, 1, 1), new DateTime(2002, 1, 1), new DateTime(2010, 1, 1)] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.First), nameof(Enumerable.Last), nameof(Enumerable.Single), nameof(Enumerable.SingleOrDefault));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // First > 2000 is 2002
                        // Last < 2005 is 2002
                        // Single == 2010 is 2010
                        // SingleOrDefault == 1999 is null

                        var results = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where FirstVal = '2002-01-01T00:00:00.0000000' and LastVal = '2002-01-01T00:00:00.0000000' and SingleVal = '2010-01-01T00:00:00.0000000'").ToList();
                        Assert.Single(results);

                        var countNull = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where SingleOrDefaultVal = '0001-01-01T00:00:00.0000000'").Count();
                        Assert.Equal(1, countNull);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_OrderBy_DateTimeArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Oldest = doc.ImportantDates.OrderBy(x => x).First(),
                        Newest = doc.ImportantDates.OrderByDescending(x => x).First()
                    }
            };

            var d1 = new DateTime(2020, 1, 1);
            var d2 = new DateTime(2010, 1, 1);

            var docs = new object[]
            {
                new DocWithDates { ImportantDates = [d1, d2] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.OrderBy), nameof(Enumerable.OrderByDescending));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Oldest = '2010-01-01T00:00:00.0000000' and Newest = '2020-01-01T00:00:00.0000000'").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Reverse_DateTimeArray_ShouldWork(Options options)
        {
            var d1 = new DateTime(2000, 1, 1);
            var d2 = new DateTime(2010, 1, 1);

            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        LastAfterReverse = doc.ImportantDates.Reverse().Last()
                    }
            };

            var docs = new object[]
            {
                new DocWithDates { ImportantDates = [d1, d2] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Reverse)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where LastAfterReverse = '2000-01-01T00:00:00.0000000'").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Take_Skip_DateTimeArray_ShouldWork(Options options)
        {
            var d1 = new DateTime(2000, 1, 1);
            var d2 = new DateTime(2010, 1, 1);
            var d3 = new DateTime(2020, 1, 1);

            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Val = doc.ImportantDates.Skip(1).Take(1).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithDates { ImportantDates = [d1, d2, d3] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Skip), nameof(Enumerable.Take));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Val = '2010-01-01T00:00:00.0000000'").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_TakeWhile_SkipWhile_DateTimeArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        CountTaken = doc.ImportantDates.TakeWhile(x => x.Year < 2005).Count(),
                        CountSkipped = doc.ImportantDates.SkipWhile(x => x.Year < 2005).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithDates { ImportantDates = [new DateTime(2000, 1, 1), new DateTime(2002, 1, 1), new DateTime(2010, 1, 1), new DateTime(2001, 1, 1)] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.TakeWhile), nameof(Enumerable.SkipWhile));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // TakeWhile < 2005: 2000, 2002 -> 2
                        // SkipWhile < 2005: 2010, 2001 -> 2
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where CountTaken = 2 and CountSkipped = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Aggregate_DateTimeArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        CountViaAgg = doc.ImportantDates.Aggregate(0, (acc, val) => acc + 1)
                    }
            };

            var docs = new object[]
            {
                new DocWithDates { ImportantDates = [new DateTime(2000, 1, 1), new DateTime(2001, 1, 1)] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Aggregate)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where CountViaAgg = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Zip_DateTimeArray_ShouldWork(Options options)
        {
            var d1 = new DateTime(2000, 1, 1);

            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        ZipEq = doc.ImportantDates.Zip(doc.ImportantDates, (a, b) => a == b).All(x => x)
                    }
            };

            var docs = new object[]
            {
                new DocWithDates { ImportantDates = [d1] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Zip)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ZipEq = true").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_ElementAt_Indexer_DateTimeArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Val0 = doc.ImportantDates.ElementAt(0),
                        Val1 = doc.ImportantDates[1],
                        ValDef = doc.ImportantDates.ElementAtOrDefault(5)
                    }
            };

            var docs = new object[]
            {
                new DocWithDates { ImportantDates = [new DateTime(2000, 1, 1), new DateTime(2010, 1, 1)] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.ElementAt), nameof(Enumerable.ElementAtOrDefault));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Val0 = '2000-01-01T00:00:00.0000000' and Val1 = '2010-01-01T00:00:00.0000000' and ValDef = '{DateTime.MinValue:o}'").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_GroupBy_DateTimeArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        GroupCount = doc.ImportantDates.GroupBy(x => x.Year).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithDates { ImportantDates = [new DateTime(2000, 1, 1), new DateTime(2000, 5, 5), new DateTime(2001, 1, 1)] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.GroupBy)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where GroupCount = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_DefaultIfEmpty_DateTimeArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Val = doc.ImportantDates.DefaultIfEmpty(new DateTime(1900, 1, 1)).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithDates { ImportantDates = [] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.DefaultIfEmpty)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Val = '1900-01-01T00:00:00.0000000'").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_ToDictionary_ToLookup_DateTimeArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        DictCount = doc.ImportantDates.ToDictionary(k => k.Year, v => v.Month).Count(),
                        LookupCount = doc.ImportantDates.ToLookup(k => k.Year).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithDates { ImportantDates = [new DateTime(2000, 1, 1), new DateTime(2001, 1, 1)] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.ToDictionary), nameof(Enumerable.ToLookup));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where DictCount = 2 and LookupCount = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        #endregion

        #region Chars

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Intersect_CharArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithChars, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasCommon = doc.Values.Intersect(new[] { 'a', 'z' }).Any()
                    }
            };

            var docs = new object[]
            {
                new DocWithChars { Values = ['a', 'b'] },
                new DocWithChars { Values = ['c', 'd'] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Intersect), nameof(Enumerable.Any));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where HasCommon = true").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Union_CharArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithChars, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasUnionValue = doc.Values
                            .Union(new[] { 'z' })
                            .Contains('z')
                    }
            };

            var docs = new object[]
            {
                new DocWithChars { Values = ['a', 'b'] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Union), nameof(Enumerable.Contains));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where HasUnionValue = true").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_OrderBy_CharArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithChars, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        FirstSorted = doc.Values.OrderBy(c => c).First(),
                        LastSorted = doc.Values.OrderByDescending(c => c).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithChars { Values = ['z', 'a', 'm'] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.OrderBy), nameof(Enumerable.OrderByDescending));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // 'a' should be first asc, 'z' first desc
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where FirstSorted = 'a' and LastSorted = 'z'").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Distinct_CharArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithChars, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        DistinctCount = doc.Values.Distinct().Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithChars { Values = ['a', 'a', 'b'] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Distinct)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where DistinctCount = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Reverse_CharArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithChars, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        LastAfterReverse = doc.Values.Reverse().Last()
                    }
            };

            var docs = new object[]
            {
                new DocWithChars { Values = ['a', 'z'] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Reverse)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where LastAfterReverse = 'a'").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Take_Skip_CharArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithChars, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Val = doc.Values.Skip(1).Take(1).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithChars { Values = ['a', 'b', 'c'] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Skip), nameof(Enumerable.Take));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Val = 'b'").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_MinMax_CharArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithChars, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        MinVal = doc.Values.Min(),
                        MaxVal = doc.Values.Max()
                    }
            };

            var docs = new object[]
            {
                new DocWithChars { Values = ['a', 'z', 'm'] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Min), nameof(Enumerable.Max));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where MinVal = 'a' and MaxVal = 'z'").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Concat_CharArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithChars, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        TotalLen = doc.Values.Concat(new[] { '!' }).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithChars { Values = ['H', 'i'] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Concat)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where TotalLen = 3").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_ElementAt_Indexer_CharArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithChars, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        ValAtIndex = doc.Values.ElementAt(1),
                        ValAtIdx = doc.Values[1],
                        ValDef = doc.Values.ElementAtOrDefault(10) // should be default char \0
                    }
            };

            var docs = new object[]
            {
                new DocWithChars { Values = ['a', 'b', 'c'] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.ElementAt), nameof(Enumerable.ElementAtOrDefault));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ValAtIndex = 'b' and ValAtIdx = 'b' and (ValDef = '\\u0000' or ValDef = null)").Count(); // TODO: We are getting different results for the default value with Lucene and Corax. Both of them should (?) return `'\u0000'`.
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Aggregate_CharArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithChars, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Concat chars to string
                        Str = doc.Values.Aggregate("", (acc, c) => acc + c)
                    }
            };

            var docs = new object[]
            {
                new DocWithChars { Values = ['H', 'e', 'y'] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Aggregate)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Str = 'Hey'").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Zip_CharArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithChars, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Zip 'a','b' with 'a','b' => "aa", "bb". First is "aa"
                        Zipped = doc.Values.Zip(doc.Values, (a, b) => "" + a + b).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithChars { Values = ['a', 'b'] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Zip)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Zipped = 'aa'").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_DefaultIfEmpty_CharArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithChars, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        FirstVal = doc.Values.DefaultIfEmpty('!').First()
                    }
            };

            var docs = new object[]
            {
                new DocWithChars { Values = [] },
                new DocWithChars { Values = ['a'] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.DefaultIfEmpty)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var countEmpty = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where FirstVal = '!'").Count();
                        Assert.Equal(1, countEmpty);

                        var countNormal = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where FirstVal = 'a'").Count();
                        Assert.Equal(1, countNormal);
                    }
                });
        }

        #endregion

        #region Bools

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Intersect_BoolArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithBools, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasTrue = doc.Values.Intersect(new[] { true }).Any()
                    }
            };

            var docs = new object[]
            {
                new DocWithBools { Values = [true, false] },
                new DocWithBools { Values = [false, false] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Intersect), nameof(Enumerable.Any));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where HasTrue = true").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Union_BoolArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithBools, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // [false] union [true] -> [false, true] (order depends on impl, but distinct items)
                        Count = doc.Values.Union(new[] { true }).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithBools { Values = [false] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Union)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Count = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Contains_BoolArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithBools, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasFalse = doc.Values.Contains(false)
                    }
            };

            var docs = new object[]
            {
                new DocWithBools { Values = [true, true] },
                new DocWithBools { Values = [true, false] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Contains)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where HasFalse = true").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Distinct_BoolArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithBools, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        DistinctCount = doc.Values.Distinct().Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithBools { Values = [true, true, true] }, // 1 distinct
                new DocWithBools { Values = [true, false] } // 2 distinct
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Distinct)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where DistinctCount = 1").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_TakeSkip_BoolArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithBools, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Val = doc.Values.Skip(1).Take(1).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithBools { Values = [true, false, true] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Skip), nameof(Enumerable.Take));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Val = false").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Concat_BoolArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithBools, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        TotalLen = doc.Values.Concat(new[] { true }).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithBools { Values = [false] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Concat)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where TotalLen = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Where_Select_BoolArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithBools, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Filter true, then invert to false
                        Result = doc.Values.Where(x => x).Select(x => !x).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithBools { Values = [false, true, false] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Where), nameof(Enumerable.Select));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Result = false").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_OrderBy_BoolArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithBools, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        FirstSorted = doc.Values.OrderBy(x => x).First(),
                        LastSorted = doc.Values.OrderByDescending(x => x).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithBools { Values = [true, false] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.OrderBy), nameof(Enumerable.OrderByDescending));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // OrderBy: false, true -> First is false
                        // OrderByDesc: true, false -> First is true
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where FirstSorted = false and LastSorted = true").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Sum_Average_Min_Max_WithSelector_BoolArray_ShouldWork(Options options)
        {
            // Note: Direct Sum/Min/Max on bools is not supported by standard LINQ or DynamicArray without a selector (it doesn't implement numeric interfaces).
            // We test with a selector that converts bool to int.
            var indexBuilder = new IndexDefinitionBuilder<DocWithBools, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        SumVal = doc.Values.Sum(x => x ? 1 : 0),
                        MinVal = doc.Values.Min(x => x ? 1 : 0),
                        MaxVal = doc.Values.Max(x => x ? 1 : 0),
                        AvgVal = doc.Values.Average(x => x ? 1.0 : 0.0)
                    }
            };

            var docs = new object[]
            {
                new DocWithBools { Values = [true, true, false, false] }
            };

            // Sum: 1+1+0+0 = 2
            // Min: 0
            // Max: 1
            // Avg: 2 / 4 = 0.5

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.Sum), nameof(Enumerable.Min), nameof(Enumerable.Max), nameof(Enumerable.Average));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where SumVal = 2 and MinVal = 0 and MaxVal = 1 and AvgVal = 0.5").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Reverse_BoolArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithBools, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        LastAfterReverse = doc.Values.Reverse().Last()
                    }
            };

            var docs = new object[]
            {
                new DocWithBools { Values = [true, false] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Reverse)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // Reverse: [false, true]. Last: true.
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where LastAfterReverse = true").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_TakeWhile_SkipWhile_BoolArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithBools, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        CountTaken = doc.Values.TakeWhile(x => x).Count(),
                        CountSkipped = doc.Values.SkipWhile(x => x).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithBools { Values = [true, true, false, true] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.TakeWhile), nameof(Enumerable.SkipWhile));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // TakeWhile true: [true, true] -> 2
                        // SkipWhile true: [false, true] -> 2
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where CountTaken = 2 and CountSkipped = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Zip_BoolArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithBools, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Zip with self, XOR pairs. [true, false] zip [true, false] -> (t^t=f, f^f=f) -> All false
                        AnyTrue = doc.Values.Zip(doc.Values, (a, b) => a ^ b).Any(x => x)
                    }
            };

            var docs = new object[]
            {
                new DocWithBools { Values = [true, false] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.Zip)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where AnyTrue = false").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_DefaultIfEmpty_BoolArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithBools, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Val = doc.Values.DefaultIfEmpty(true).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithBools { Values = [] },
                new DocWithBools { Values = [false] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.DefaultIfEmpty)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // Empty -> default true
                        var countDefault = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Val = true").Count();
                        Assert.Equal(1, countDefault);

                        // [false] -> first is false
                        var countNormal = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Val = false").Count();
                        Assert.Equal(1, countNormal);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_GroupBy_BoolArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithBools, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Group by self. [true, false, true] -> Keys: true (2), false (1)
                        GroupsCount = doc.Values.GroupBy(x => x).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithBools { Values = [true, false, true] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => AssertMapContains(map, nameof(Enumerable.GroupBy)),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where GroupsCount = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_First_Last_Single_WithPredicate_BoolArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithBools, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        FirstTrue = doc.Values.First(x => x == true),
                        LastFalse = doc.Values.Last(x => x == false),
                        SingleTrue = doc.Values.Single(x => x == true),
                        SingleOrDefault = doc.Values.SingleOrDefault(x => x == true) // Should succeed if only 1 true
                    }
            };

            var docs = new object[]
            {
                new DocWithBools { Values = [false, true, false] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.First), nameof(Enumerable.Last), nameof(Enumerable.Single), nameof(Enumerable.SingleOrDefault));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where FirstTrue = true and LastFalse = false and SingleTrue = true").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Any_All_Count_WithPredicate_BoolArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithBools, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        AllTrue = doc.Values.All(x => x),
                        AnyFalse = doc.Values.Any(x => !x),
                        CountTrue = doc.Values.Count(x => x)
                    }
            };

            var docs = new object[]
            {
                new DocWithBools { Values = [true, false, true] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.All), nameof(Enumerable.Any), nameof(Enumerable.Count));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // AllTrue: false (has false)
                        // AnyFalse: true
                        // CountTrue: 2
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where AllTrue = false and AnyFalse = true and CountTrue = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_ElementAt_Indexer_BoolArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithBools, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        ValAt0 = doc.Values.ElementAt(0),
                        ValAtIndex1 = doc.Values[1],
                        ValAtDef = doc.Values.ElementAtOrDefault(5) // Default for bool is false
                    }
            };

            var docs = new object[]
            {
                new DocWithBools { Values = [true, false] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.ElementAt), nameof(Enumerable.ElementAtOrDefault));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ValAt0 = true and ValAtIndex1 = false and ValAtDef = false").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_ToDictionary_ToLookup_BoolArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithBools, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Keys: true, false
                        DictCount = doc.Values.ToDictionary(k => k, v => v).Count(),
                        // Lookup: true group, false group
                        LookupCount = doc.Values.ToLookup(k => k).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithBools { Values = [true, false] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    AssertMapContains(map, nameof(Enumerable.ToDictionary), nameof(Enumerable.ToLookup));
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where DictCount = 2 and LookupCount = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        #endregion
    }
}
