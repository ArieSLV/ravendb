using System;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
// ReSharper disable InvokeAsExtensionMethod
// ReSharper disable CSharp14OverloadResolutionWithSpanBreakingChange
// ReSharper disable ConvertClosureToMethodGroup

namespace SlowTests.Client.Indexing
{
    public class DynamicArrayEqualityAndSetOperationsTests : IndexDefinitionTests
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
                        HasAny = doc.Values.Intersect(new long[] { 42L, 100L }).Any()
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
                    Assert.Contains(nameof(Enumerable.Intersect), map);
                    Assert.Contains(nameof(Enumerable.Any), map);
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
        public void MapIndex_SequenceEqual_LongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        IsExact = doc.Values.SequenceEqual(new long[] { 1L, 2L, 3L })
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { Values = [1L, 2L, 3L] },
                new DocWithLongs { Values = [1L, 2L] },
                new DocWithLongs { Values = [2L, 3L, 4L] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                    Assert.Contains(nameof(Enumerable.SequenceEqual), map),
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
        public void MapIndex_Except_LongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasRemaining = doc.Values
                            .Except(new long[] { 1L, 2L })
                            .Contains(3L)
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { Values = [1L, 2L, 3L] },
                new DocWithLongs { Values = [1L, 2L] },
                new DocWithLongs { Values = [3L] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Except), map);
                    Assert.Contains(nameof(Enumerable.Contains), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where HasRemaining = true")
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
                        HasUnionValue = doc.Values
                            .Union(new long[] { 3L })
                            .Contains(3L)
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
                    Assert.Contains(nameof(Enumerable.Union), map);
                    Assert.Contains(nameof(Enumerable.Contains), map);
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
        public void MapIndex_Contains_LongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasValue = doc.Values.Contains(42L)
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { Values = [1L, 42L] },
                new DocWithLongs { Values = [1L, 2L] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Contains), map),
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Distinct), map),
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
        public void MapIndex_TakeSkip_LongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Skip 1, Take 1. [10, 20, 30] -> [20] -> Sum = 20
                        Value = doc.Values.Skip(1).Take(1).Sum()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { Values = [10L, 20L, 30L] },
                new DocWithLongs { Values = [5L, 5L] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Skip), map);
                    Assert.Contains(nameof(Enumerable.Take), map);
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
        public void MapIndex_Concat_LongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        AllValues = doc.Values.Concat(new long[] { 999L }).ToArray()
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Concat), map),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // 1, 2 + 999 => Length 3
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where AllValues.Length = 3").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Where_Select_LongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Filter > 10, then multiply by 2, then sum
                        Result = doc.Values.Where(x => x > 10L).Select(x => x * 2).Sum()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { Values = [5L, 20L, 30L] } // 20*2 + 30*2 = 100
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Where), map);
                    Assert.Contains(nameof(Enumerable.Select), map);
                    Assert.Contains(nameof(Enumerable.Sum), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Result = 100").Count();
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
                    Assert.Contains(nameof(Enumerable.OrderBy), map);
                    Assert.Contains(nameof(Enumerable.OrderByDescending), map);
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
        public void MapIndex_Aggregate_MinMax_LongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        MinVal = doc.Values.Min(),
                        MaxVal = doc.Values.Max(),
                        AvgVal = doc.Values.Average()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { Values = [10L, 20L, 30L] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Min), map);
                    Assert.Contains(nameof(Enumerable.Max), map);
                    Assert.Contains(nameof(Enumerable.Average), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where MinVal = 10 and MaxVal = 30 and AvgVal = 20").Count();
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
                    Assert.Contains(nameof(Enumerable.Min), map);
                    Assert.Contains(nameof(Enumerable.Max), map);
                    Assert.Contains(nameof(Enumerable.Average), map);
                    Assert.Contains(nameof(Enumerable.Sum), map);
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Reverse), map),
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
                    Assert.Contains(nameof(Enumerable.TakeWhile), map);
                    Assert.Contains(nameof(Enumerable.SkipWhile), map);
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Aggregate), map),
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Zip), map),
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
                new DocWithLongs { Values = new long[0] },
                new DocWithLongs { Values = [1L] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.DefaultIfEmpty), map),
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.GroupBy), map),
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
                        SingleOrDefaultVal = doc.Values.SingleOrDefault(x => x == 999) // Should be null (or default)
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
                    Assert.Contains(nameof(Enumerable.First), map);
                    Assert.Contains(nameof(Enumerable.Last), map);
                    Assert.Contains(nameof(Enumerable.Single), map);
                    Assert.Contains(nameof(Enumerable.SingleOrDefault), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // First > 10 is 20
                        // Last > 10 is 50
                        // Single == 20 is 20
                        // SingleOrDefault == 999 is 0 (long default) or null in dynamic context, but here treated as 0 in value comparison often

                        var results = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where FirstGt = 20 and LastGt = 50 and SingleVal = 20").ToList();
                        Assert.Single(results);

                        // Checking SingleOrDefault returning null/0
                        var doc = results[0];
                        // In dynamic index context, usually null object or 0 for value types.
                        // Let's assert strictly on what we can query or if it's stored.
                        // Querying for it:
                        var countNull = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where SingleOrDefaultVal = null").Count();
                        if (countNull == 0)
                        {
                            // Could be 0
                            var countZero = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where SingleOrDefaultVal = 0").Count();
                            Assert.Equal(1, countZero);
                        }
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
                    Assert.Contains(nameof(Enumerable.All), map);
                    Assert.Contains(nameof(Enumerable.Any), map);
                    Assert.Contains(nameof(Enumerable.Count), map);
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
                    Assert.Contains(nameof(Enumerable.ElementAt), map);
                    Assert.Contains(nameof(Enumerable.ElementAtOrDefault), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ValAt0 = 10 and ValAtIndex1 = 20").Count();
                        Assert.Equal(1, count);

                        // Check default
                        var countDef = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ValAtDef = null or ValAtDef = 0").Count();
                        Assert.Equal(1, countDef);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_OfType_LongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        OfTypeCount = doc.Values.OfType<long>().Count()
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
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.OfType), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where OfTypeCount = 3").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Join_GroupJoin_LongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    let other = new long[] { 1L, 3L, 5L }
                    select new
                    {
                        doc.Id,
                        // Join on equality. [1, 2, 3] join [1, 3, 5] -> matches 1 and 3. Sum = 4.
                        JoinSum = doc.Values.Join(other, outer => outer, inner => inner, (o, i) => o).Sum(),

                        // GroupJoin. [1, 2, 3] into [1, 3, 5].
                        // 1 matches [1], 2 matches [], 3 matches [3].
                        // Count of matching groups that are not empty = 2.
                        GroupJoinCount = doc.Values.GroupJoin(other, o => o, i => i, (o, matches) => matches.Count()).Sum()
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
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Join), map);
                    Assert.Contains(nameof(Enumerable.GroupJoin), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // JoinSum: 1 + 3 = 4
                        // GroupJoinCount: matches for 1 has 1, for 2 has 0, for 3 has 1. Sum = 2.
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where JoinSum = 4 and GroupJoinCount = 2").Count();
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
                        DictCount = doc.Values.ToDictionary(k => k, v => v * 10).Count,
                        // Lookup by Modulo 2. [1, 2, 3, 4] -> Keys: 1 (vals 1,3), 0 (vals 2,4)
                        LookupCount = doc.Values.ToLookup(k => k % 2).Count
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
                    Assert.Contains(nameof(Enumerable.ToDictionary), map);
                    Assert.Contains(nameof(Enumerable.ToLookup), map);
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
                    Assert.Contains(nameof(Enumerable.Intersect), map);
                    Assert.Contains(nameof(Enumerable.Any), map);
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
        public void MapIndex_SequenceEqual_IntArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        IsExact = doc.IntValues.SequenceEqual(new[] { 1, 2, 3 })
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { IntValues = [1, 2, 3] },
                new DocWithLongs { IntValues = [1, 2] },
                new DocWithLongs { IntValues = [2, 3, 4] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                    Assert.Contains(nameof(Enumerable.SequenceEqual), map),
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
        public void MapIndex_Except_IntArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasRemaining = doc.IntValues
                            .Except(new[] { 1, 2 })
                            .Contains(3)
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { IntValues = [1, 2, 3] },
                new DocWithLongs { IntValues = [1, 2] },
                new DocWithLongs { IntValues = [3] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Except), map);
                    Assert.Contains(nameof(Enumerable.Contains), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where HasRemaining = true")
                            .ToList();

                        Assert.Equal(2, results.Count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Union_IntArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasUnionValue = doc.IntValues
                            .Union(new[] { 3 })
                            .Contains(3)
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { IntValues = [1] },
                new DocWithLongs { IntValues = [3] },
                new DocWithLongs { IntValues = [4] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Union), map);
                    Assert.Contains(nameof(Enumerable.Contains), map);
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
        public void MapIndex_Contains_IntArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasValue = doc.IntValues.Contains(42)
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { IntValues = [1, 42] },
                new DocWithLongs { IntValues = [1, 2] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Contains), map),
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Distinct), map),
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
        public void MapIndex_TakeSkip_IntArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Skip 1, Take 1. [10, 20, 30] -> [20] -> Sum = 20
                        Value = doc.IntValues.Skip(1).Take(1).Sum()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { IntValues = [10, 20, 30] },
                new DocWithLongs { IntValues = [5, 5] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Skip), map);
                    Assert.Contains(nameof(Enumerable.Take), map);
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
        public void MapIndex_Concat_IntArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        AllValues = doc.IntValues.Concat(new[] { 999 }).ToArray()
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Concat), map),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // 1, 2 + 999 => Length 3
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where AllValues.Length = 3").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Where_Select_IntArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Filter > 10, then multiply by 2, then sum
                        Result = doc.IntValues.Where(x => x > 10).Select(x => x * 2).Sum()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { IntValues = [5, 20, 30] } // 20*2 + 30*2 = 100
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Where), map);
                    Assert.Contains(nameof(Enumerable.Select), map);
                    Assert.Contains(nameof(Enumerable.Sum), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Result = 100").Count();
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
                    Assert.Contains(nameof(Enumerable.OrderBy), map);
                    Assert.Contains(nameof(Enumerable.OrderByDescending), map);
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
        public void MapIndex_Aggregate_MinMax_IntArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        MinVal = doc.IntValues.Min(),
                        MaxVal = doc.IntValues.Max(),
                        AvgVal = doc.IntValues.Average()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { IntValues = [10, 20, 30] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Min), map);
                    Assert.Contains(nameof(Enumerable.Max), map);
                    Assert.Contains(nameof(Enumerable.Average), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where MinVal = 10 and MaxVal = 30 and AvgVal = 20").Count();
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
                    Assert.Contains(nameof(Enumerable.Min), map);
                    Assert.Contains(nameof(Enumerable.Max), map);
                    Assert.Contains(nameof(Enumerable.Average), map);
                    Assert.Contains(nameof(Enumerable.Sum), map);
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Reverse), map),
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
                    Assert.Contains(nameof(Enumerable.TakeWhile), map);
                    Assert.Contains(nameof(Enumerable.SkipWhile), map);
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Aggregate), map),
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Zip), map),
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
                new DocWithLongs { IntValues = new int[0] },
                new DocWithLongs { IntValues = [1] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.DefaultIfEmpty), map),
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.GroupBy), map),
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
                        SingleOrDefaultVal = doc.IntValues.SingleOrDefault(x => x == 999) // Should be null (or default)
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
                    Assert.Contains(nameof(Enumerable.First), map);
                    Assert.Contains(nameof(Enumerable.Last), map);
                    Assert.Contains(nameof(Enumerable.Single), map);
                    Assert.Contains(nameof(Enumerable.SingleOrDefault), map);
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
                    Assert.Contains(nameof(Enumerable.All), map);
                    Assert.Contains(nameof(Enumerable.Any), map);
                    Assert.Contains(nameof(Enumerable.Count), map);
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
                    Assert.Contains(nameof(Enumerable.ElementAt), map);
                    Assert.Contains(nameof(Enumerable.ElementAtOrDefault), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ValAt0 = 10 and ValAtIndex1 = 20").Count();
                        Assert.Equal(1, count);

                        // Check default
                        var countDef = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ValAtDef = null or ValAtDef = 0").Count();
                        Assert.Equal(1, countDef);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_OfType_IntArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        OfTypeCount = doc.IntValues.OfType<int>().Count()
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
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.OfType), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where OfTypeCount = 3").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Join_GroupJoin_IntArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    let other = new int[] { 1, 3, 5 }
                    select new
                    {
                        doc.Id,
                        // Join on equality. [1, 2, 3] join [1, 3, 5] -> matches 1 and 3. Sum = 4.
                        JoinSum = doc.IntValues.Join(other, outer => outer, inner => inner, (o, i) => o).Sum(),

                        // GroupJoin. [1, 2, 3] into [1, 3, 5].
                        // 1 matches [1], 2 matches [], 3 matches [3].
                        // Count of matching groups that are not empty = 2.
                        GroupJoinCount = doc.IntValues.GroupJoin(other, o => o, i => i, (o, matches) => matches.Count()).Sum()
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
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Join), map);
                    Assert.Contains(nameof(Enumerable.GroupJoin), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // JoinSum: 1 + 3 = 4
                        // GroupJoinCount: matches for 1 has 1, for 2 has 0, for 3 has 1. Sum = 2.
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where JoinSum = 4 and GroupJoinCount = 2").Count();
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
                        DictCount = doc.IntValues.ToDictionary(k => k, v => v * 10).Count,
                        // Lookup by Modulo 2. [1, 2, 3, 4] -> Keys: 1 (vals 1,3), 0 (vals 2,4)
                        LookupCount = doc.IntValues.ToLookup(k => k % 2).Count
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
                    Assert.Contains(nameof(Enumerable.ToDictionary), map);
                    Assert.Contains(nameof(Enumerable.ToLookup), map);
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
                    Assert.Contains(nameof(Enumerable.Intersect), map);
                    Assert.Contains(nameof(Enumerable.Any), map);
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
        public void MapIndex_SequenceEqual_ULongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        IsExact = doc.ULongValues.SequenceEqual(new[] { 1UL, 2UL, 3UL })
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { ULongValues = [1UL, 2UL, 3UL] },
                new DocWithLongs { ULongValues = [1UL, 2UL] },
                new DocWithLongs { ULongValues = [2UL, 3UL, 4UL] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                    Assert.Contains(nameof(Enumerable.SequenceEqual), map),
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
        public void MapIndex_Except_ULongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasRemaining = doc.ULongValues
                            .Except(new[] { 1UL, 2UL })
                            .Contains(3UL)
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { ULongValues = [1UL, 2UL, 3UL] },
                new DocWithLongs { ULongValues = [1UL, 2UL] },
                new DocWithLongs { ULongValues = [3UL] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Except), map);
                    Assert.Contains(nameof(Enumerable.Contains), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session.Advanced
                            .RawQuery<dynamic>($"from index '{indexName}' where HasRemaining = true")
                            .ToList();

                        Assert.Equal(2, results.Count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Union_ULongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasUnionValue = doc.ULongValues
                            .Union(new[] { 3UL })
                            .Contains(3UL)
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { ULongValues = [1UL] },
                new DocWithLongs { ULongValues = [3UL] },
                new DocWithLongs { ULongValues = [4UL] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Union), map);
                    Assert.Contains(nameof(Enumerable.Contains), map);
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
        public void MapIndex_Contains_ULongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasValue = doc.ULongValues.Contains(42UL)
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { ULongValues = [1UL, 42UL] },
                new DocWithLongs { ULongValues = [1UL, 2UL] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Contains), map),
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Distinct), map),
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
                    Assert.Contains(nameof(Enumerable.Skip), map);
                    Assert.Contains(nameof(Enumerable.Take), map);
                    Assert.Contains(nameof(Enumerable.Sum), map);
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
                        AllValues = doc.ULongValues.Concat(new[] { 999UL }).ToArray()
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Concat), map),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where AllValues.Length = 3").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Where_Select_ULongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Result = doc.ULongValues.Where(x => x > 10UL).Select(x => (decimal)x * 2).Sum()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { ULongValues = [5UL, 20UL, 30UL] } // 20*2 + 30*2 = 100
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Where), map);
                    Assert.Contains(nameof(Enumerable.Select), map);
                    Assert.Contains(nameof(Enumerable.Sum), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Result = 100").Count();
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
                    Assert.Contains(nameof(Enumerable.OrderBy), map);
                    Assert.Contains(nameof(Enumerable.OrderByDescending), map);
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
        public void MapIndex_Aggregate_MinMax_ULongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        MinVal = doc.ULongValues.Min(),
                        MaxVal = doc.ULongValues.Max(),
                        // Average on ulong requires casting usually, or let's see if dynamic handles it if we cast
                        AvgVal = doc.ULongValues.Select(x => (decimal)x).Average()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { ULongValues = [10UL, 20UL, 30UL] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Min), map);
                    Assert.Contains(nameof(Enumerable.Max), map);
                    Assert.Contains(nameof(Enumerable.Average), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where MinVal = 10 and MaxVal = 30 and AvgVal = 20").Count();
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
                    Assert.Contains(nameof(Enumerable.Min), map);
                    Assert.Contains(nameof(Enumerable.Max), map);
                    Assert.Contains(nameof(Enumerable.Average), map);
                    Assert.Contains(nameof(Enumerable.Sum), map);
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Reverse), map),
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
                    Assert.Contains(nameof(Enumerable.TakeWhile), map);
                    Assert.Contains(nameof(Enumerable.SkipWhile), map);
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Aggregate), map),
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Zip), map),
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
                new DocWithLongs { ULongValues = new ulong[0] },
                new DocWithLongs { ULongValues = [1UL] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.DefaultIfEmpty), map),
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.GroupBy), map),
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
                    Assert.Contains(nameof(Enumerable.First), map);
                    Assert.Contains(nameof(Enumerable.Last), map);
                    Assert.Contains(nameof(Enumerable.Single), map);
                    Assert.Contains(nameof(Enumerable.SingleOrDefault), map);
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
                    Assert.Contains(nameof(Enumerable.All), map);
                    Assert.Contains(nameof(Enumerable.Any), map);
                    Assert.Contains(nameof(Enumerable.Count), map);
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
                    Assert.Contains(nameof(Enumerable.ElementAt), map);
                    Assert.Contains(nameof(Enumerable.ElementAtOrDefault), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ValAt0 = 10 and ValAtIndex1 = 20").Count();
                        Assert.Equal(1, count);

                        var countDef = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ValAtDef = null or ValAtDef = 0").Count();
                        Assert.Equal(1, countDef);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_OfType_ULongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        OfTypeCount = doc.ULongValues.OfType<ulong>().Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithLongs { ULongValues = [1UL, 2UL, ulong.MaxValue] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.OfType), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where OfTypeCount = 3").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Join_GroupJoin_ULongArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithLongs, object>
            {
                Map = docs => from doc in docs
                    let other = new ulong[] { 1UL, 3UL, 5UL }
                    select new
                    {
                        doc.Id,
                        // Join on equality. [1, 2, 3] join [1, 3, 5] -> matches 1 and 3. Sum = 4.
                        JoinSum = doc.ULongValues.Join(other, outer => outer, inner => inner, (o, i) => (decimal)o).Sum(),

                        // GroupJoin. [1, 2, 3] into [1, 3, 5].
                        // 1 matches [1], 2 matches [], 3 matches [3].
                        // Count of matching groups that are not empty = 2.
                        GroupJoinCount = doc.ULongValues.GroupJoin(other, o => o, i => i, (o, matches) => matches.Count()).Sum()
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
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Join), map);
                    Assert.Contains(nameof(Enumerable.GroupJoin), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where JoinSum = 4 and GroupJoinCount = 2").Count();
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
                        DictCount = doc.ULongValues.ToDictionary(k => k, v => v * 10).Count,
                        // Lookup by Modulo 2.
                        LookupCount = doc.ULongValues.ToLookup(k => k % 2).Count
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
                    Assert.Contains(nameof(Enumerable.ToDictionary), map);
                    Assert.Contains(nameof(Enumerable.ToLookup), map);
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
                    Assert.Contains(nameof(Enumerable.Intersect), map);
                    Assert.Contains(nameof(Enumerable.Any), map);
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
        public void MapIndex_SequenceEqual_FloatArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithFloats, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        IsExact = doc.Values.SequenceEqual(new[] { 1.5f, 2.5f })
                    }
            };

            var docs = new object[]
            {
                new DocWithFloats { Values = [1.5f, 2.5f] },
                new DocWithFloats { Values = [1.5f] },
                new DocWithFloats { Values = [2.5f, 3.5f] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                    Assert.Contains(nameof(Enumerable.SequenceEqual), map),
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
        public void MapIndex_Except_FloatArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithFloats, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasRemaining = doc.Values
                            .Except(new[] { 1.5f })
                            .Contains(2.5f)
                    }
            };

            var docs = new object[]
            {
                new DocWithFloats { Values = [1.5f, 2.5f] },
                new DocWithFloats { Values = [1.5f] },
                new DocWithFloats { Values = [3.5f] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Except), map);
                    Assert.Contains(nameof(Enumerable.Contains), map);
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
        public void MapIndex_Union_FloatArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithFloats, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasUnionValue = doc.Values
                            .Union(new[] { 3.5f })
                            .Contains(3.5f)
                    }
            };

            var docs = new object[]
            {
                new DocWithFloats { Values = [1.5f] },
                new DocWithFloats { Values = [3.5f] },
                new DocWithFloats { Values = [4.5f] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Union), map);
                    Assert.Contains(nameof(Enumerable.Contains), map);
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Contains), map),
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Distinct), map),
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
        public void MapIndex_TakeSkip_FloatArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithFloats, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Skip 1, Take 1. [1.1, 2.2, 3.3] -> [2.2]
                        Val = doc.Values.Skip(1).Take(1).Sum()
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
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Skip), map);
                    Assert.Contains(nameof(Enumerable.Take), map);
                    Assert.Contains(nameof(Enumerable.Sum), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // 2.2f is approximate, but Sum() of 1 item is that item.
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Val > 2.1 and Val < 2.3").Count();
                        Assert.Equal(1, count);
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Concat), map),
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
        public void MapIndex_Where_Select_FloatArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithFloats, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // > 2.0 -> [2.5, 3.5] -> * 2 -> [5.0, 7.0] -> Sum = 12.0
                        Result = doc.Values.Where(x => x > 2.0f).Select(x => x * 2).Sum()
                    }
            };

            var docs = new object[]
            {
                new DocWithFloats { Values = [1.5f, 2.5f, 3.5f] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Where), map);
                    Assert.Contains(nameof(Enumerable.Select), map);
                    Assert.Contains(nameof(Enumerable.Sum), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Result = 12.0").Count();
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
                    Assert.Contains(nameof(Enumerable.OrderBy), map);
                    Assert.Contains(nameof(Enumerable.OrderByDescending), map);
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
        public void MapIndex_Aggregate_MinMax_FloatArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithFloats, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        SumVal = doc.Values.Sum(),
                        AvgVal = doc.Values.Average(),
                        MinVal = doc.Values.Min(),
                        MaxVal = doc.Values.Max()
                    }
            };

            var docs = new object[]
            {
                new DocWithFloats { Values = [1.0f, 3.0f] } // Sum=4, Avg=2, Min=1, Max=3
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Sum), map);
                    Assert.Contains(nameof(Enumerable.Average), map);
                    Assert.Contains(nameof(Enumerable.Min), map);
                    Assert.Contains(nameof(Enumerable.Max), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where SumVal = 4 and AvgVal = 2 and MinVal = 1 and MaxVal = 3").Count();
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
                    Assert.Contains(nameof(Enumerable.Min), map);
                    Assert.Contains(nameof(Enumerable.Max), map);
                    Assert.Contains(nameof(Enumerable.Average), map);
                    Assert.Contains(nameof(Enumerable.Sum), map);
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Reverse), map),
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
                    Assert.Contains(nameof(Enumerable.TakeWhile), map);
                    Assert.Contains(nameof(Enumerable.SkipWhile), map);
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Aggregate), map),
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Zip), map),
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
        public void MapIndex_Join_GroupJoin_FloatArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithFloats, object>
            {
                Map = docs => from doc in docs
                    let other = new float[] { 2.0f, 5.0f }
                    select new
                    {
                        doc.Id,
                        // Join on exact float equality (safe here as we use exact values).
                        // [1.0, 2.0, 3.0] join [2.0, 5.0]. Match on 2.0.
                        JoinRes = doc.Values.Join(other, o => o, i => i, (o, i) => o).FirstOrDefault(),

                        // GroupJoin.
                        // 1.0 -> [], 2.0 -> [2.0], 3.0 -> [].
                        // Sum of counts = 1.
                        GroupJoinCount = doc.Values.GroupJoin(other, o => o, i => i, (o, matches) => matches.Count()).Sum()
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
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Join), map);
                    Assert.Contains(nameof(Enumerable.GroupJoin), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where JoinRes = 2.0 and GroupJoinCount = 1").Count();
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
                new DocWithFloats { Values = new float[0] },
                new DocWithFloats { Values = [1.1f] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.DefaultIfEmpty), map),
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.GroupBy), map),
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
                    Assert.Contains(nameof(Enumerable.First), map);
                    Assert.Contains(nameof(Enumerable.Last), map);
                    Assert.Contains(nameof(Enumerable.Single), map);
                    Assert.Contains(nameof(Enumerable.SingleOrDefault), map);
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
                    Assert.Contains(nameof(Enumerable.All), map);
                    Assert.Contains(nameof(Enumerable.Any), map);
                    Assert.Contains(nameof(Enumerable.Count), map);
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
                    Assert.Contains(nameof(Enumerable.ElementAt), map);
                    Assert.Contains(nameof(Enumerable.ElementAtOrDefault), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ValAt0 = 10.5 and ValAtIndex1 = 20.5").Count();
                        Assert.Equal(1, count);

                        var countDef = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ValAtDef = null or ValAtDef = 0.0").Count();
                        Assert.Equal(1, countDef);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_OfType_FloatArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithFloats, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        OfTypeCount = doc.Values.OfType<float>().Count()
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
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.OfType), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where OfTypeCount = 2").Count();
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
                        DictCount = doc.Values.ToDictionary(k => k, v => v + 1.0f).Count,
                        // Lookup by integer part
                        LookupCount = doc.Values.ToLookup(k => (int)k).Count
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
                    Assert.Contains(nameof(Enumerable.ToDictionary), map);
                    Assert.Contains(nameof(Enumerable.ToLookup), map);
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
                    Assert.Contains(nameof(Enumerable.Intersect), map);
                    Assert.Contains(nameof(Enumerable.Any), map);
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
        public void MapIndex_SequenceEqual_DoubleArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDoubles, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        IsExact = doc.Values.SequenceEqual(new[] { 1.1, 2.2 })
                    }
            };

            var docs = new object[]
            {
                new DocWithDoubles { Values = [1.1, 2.2] },
                new DocWithDoubles { Values = [1.1] },
                new DocWithDoubles { Values = [2.2, 3.3] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                    Assert.Contains(nameof(Enumerable.SequenceEqual), map),
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
        public void MapIndex_Except_DoubleArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDoubles, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasRemaining = doc.Values
                            .Except(new[] { 1.1 })
                            .Contains(2.2)
                    }
            };

            var docs = new object[]
            {
                new DocWithDoubles { Values = [1.1, 2.2] },
                new DocWithDoubles { Values = [1.1] },
                new DocWithDoubles { Values = [3.3] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Except), map);
                    Assert.Contains(nameof(Enumerable.Contains), map);
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
                    Assert.Contains(nameof(Enumerable.Union), map);
                    Assert.Contains(nameof(Enumerable.Contains), map);
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Contains), map),
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Distinct), map),
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
        public void MapIndex_TakeSkip_DoubleArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDoubles, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Skip 1, Take 1. [1.1, 2.2, 3.3] -> [2.2]
                        Val = doc.Values.Skip(1).Take(1).Sum()
                    }
            };

            var docs = new object[]
            {
                new DocWithDoubles { Values = [1.1, 2.2, 3.3] },
                new DocWithDoubles { Values = [5.5, 5.5] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Skip), map);
                    Assert.Contains(nameof(Enumerable.Take), map);
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Concat), map),
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
        public void MapIndex_Where_Select_DoubleArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDoubles, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Filter > 2.0, multiply by 2, sum. [1.1, 3.3, 4.4] -> [3.3, 4.4] -> [6.6, 8.8] -> 15.4
                        Result = doc.Values.Where(x => x > 2.0).Select(x => x * 2.0).Sum()
                    }
            };

            var docs = new object[]
            {
                new DocWithDoubles { Values = [1.1, 3.3, 4.4] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Where), map);
                    Assert.Contains(nameof(Enumerable.Select), map);
                    Assert.Contains(nameof(Enumerable.Sum), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // 15.4 might have float precision issues, usually safe in these simple tests but using range for double is better practice
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Result > 15.39 and Result < 15.41").Count();
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
                    Assert.Contains(nameof(Enumerable.OrderBy), map);
                    Assert.Contains(nameof(Enumerable.OrderByDescending), map);
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
        public void MapIndex_Aggregate_MinMax_DoubleArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDoubles, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        MinVal = doc.Values.Min(),
                        MaxVal = doc.Values.Max(),
                        AvgVal = doc.Values.Average()
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
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Min), map);
                    Assert.Contains(nameof(Enumerable.Max), map);
                    Assert.Contains(nameof(Enumerable.Average), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where MinVal = 1.0 and MaxVal = 3.0 and AvgVal = 2.0").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Sum_Average_Min_Max_WithSelector_DoubleArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDoubles, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        MinVal = doc.Values.Min(x => x * 2),
                        MaxVal = doc.Values.Max(x => x - 0.5),
                        AvgVal = doc.Values.Average(x => x + 10),
                        SumVal = doc.Values.Sum(x => x * 2)
                    }
            };

            var docs = new object[]
            {
                new DocWithDoubles { Values = [1.5, 2.5] }
            };

            // Expected:
            // Min: (1.5*2)=3.0, (2.5*2)=5.0 -> Min 3.0
            // Max: (1.5-0.5)=1.0, (2.5-0.5)=2.0 -> Max 2.0
            // Avg: (1.5+10)=11.5, (2.5+10)=12.5 -> Avg 12.0
            // Sum: (1.5*2)=3.0, (2.5*2)=5.0 -> Sum 8.0

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Min), map);
                    Assert.Contains(nameof(Enumerable.Max), map);
                    Assert.Contains(nameof(Enumerable.Average), map);
                    Assert.Contains(nameof(Enumerable.Sum), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where MinVal = 3.0 and MaxVal = 2.0 and AvgVal = 12.0 and SumVal = 8.0").Count();
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Reverse), map),
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
                    Assert.Contains(nameof(Enumerable.TakeWhile), map);
                    Assert.Contains(nameof(Enumerable.SkipWhile), map);
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Aggregate), map),
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Zip), map),
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
                new DocWithDoubles { Values = new double[0] },
                new DocWithDoubles { Values = [1.1] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.DefaultIfEmpty), map),
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
        public void MapIndex_GroupBy_DoubleArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDoubles, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Group by Math.Floor. 1.1 and 1.9 -> 1 (count 2). 2.2 -> 2 (count 1).
                        GroupsCount = doc.Values.GroupBy(x => Math.Floor((double)x)).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithDoubles { Values = [1.1, 1.9, 2.2] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.GroupBy), map),
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
                        SingleVal = doc.Values.Single(x => x == 3.3),
                        SingleOrDefaultVal = doc.Values.SingleOrDefault(x => x == 99.9) // Should be null (or default)
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
                    Assert.Contains(nameof(Enumerable.First), map);
                    Assert.Contains(nameof(Enumerable.Last), map);
                    Assert.Contains(nameof(Enumerable.Single), map);
                    Assert.Contains(nameof(Enumerable.SingleOrDefault), map);
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
                    Assert.Contains(nameof(Enumerable.All), map);
                    Assert.Contains(nameof(Enumerable.Any), map);
                    Assert.Contains(nameof(Enumerable.Count), map);
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
                    Assert.Contains(nameof(Enumerable.ElementAt), map);
                    Assert.Contains(nameof(Enumerable.ElementAtOrDefault), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ValAt0 = 1.1 and ValAtIndex1 = 2.2").Count();
                        Assert.Equal(1, count);

                        var countDef = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ValAtDef = null or ValAtDef = 0").Count();
                        Assert.Equal(1, countDef);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_OfType_DoubleArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDoubles, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        OfTypeCount = doc.Values.OfType<double>().Count()
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
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.OfType), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where OfTypeCount = 3").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Join_GroupJoin_DoubleArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDoubles, object>
            {
                Map = docs => from doc in docs
                    let other = new double[] { 1.1, 3.3, 5.5 }
                    select new
                    {
                        doc.Id,
                        // Join on equality.
                        JoinSum = doc.Values.Join(other, outer => outer, inner => inner, (o, i) => o).Sum(),

                        // GroupJoin.
                        GroupJoinCount = doc.Values.GroupJoin(other, o => o, i => i, (o, matches) => matches.Count()).Sum()
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
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Join), map);
                    Assert.Contains(nameof(Enumerable.GroupJoin), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // JoinSum: matches 1.1 and 3.3 -> 1.1 + 3.3 = 4.4
                        // GroupJoinCount: 1.1->1 match, 2.2->0 matches, 3.3->1 match. Sum = 2.
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where JoinSum > 4.3 and JoinSum < 4.5 and GroupJoinCount = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_ToDictionary_ToLookup_DoubleArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDoubles, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        DictCount = doc.Values.ToDictionary(k => k, v => v * 10).Count,
                        // Lookup by Math.Floor. [1.1, 1.9, 2.2] -> Keys: 1 (vals 1.1,1.9), 2 (val 2.2)
                        LookupCount = doc.Values.ToLookup(k => Math.Floor((double)k)).Count
                    }
            };

            var docs = new object[]
            {
                new DocWithDoubles { Values = [1.1, 1.9, 2.2] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.ToDictionary), map);
                    Assert.Contains(nameof(Enumerable.ToLookup), map);
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
                    Assert.Contains(nameof(Enumerable.Intersect), map);
                    Assert.Contains(nameof(Enumerable.Any), map);
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
        public void MapIndex_SequenceEqual_DecimalArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDecimals, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        IsExact = doc.Values.SequenceEqual(new[] { 1.1m, 2.2m })
                    }
            };

            var docs = new object[]
            {
                new DocWithDecimals { Values = [1.1m, 2.2m] },
                new DocWithDecimals { Values = [1.1m] },
                new DocWithDecimals { Values = [2.2m, 3.3m] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.SequenceEqual), map),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where IsExact = true").Count();
                        Assert.Equal(1, count);
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
                    Assert.Contains(nameof(Enumerable.Union), map);
                    Assert.Contains(nameof(Enumerable.Contains), map);
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
        public void MapIndex_Except_DecimalArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDecimals, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasRemaining = doc.Values.Except(new[] { 1.1m }).Contains(2.2m)
                    }
            };

            var docs = new object[]
            {
                new DocWithDecimals { Values = [1.1m, 2.2m] },
                new DocWithDecimals { Values = [1.1m] },
                new DocWithDecimals { Values = [3.3m] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Except), map);
                    Assert.Contains(nameof(Enumerable.Contains), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where HasRemaining = true").Count();
                        Assert.Equal(1, count);
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Concat), map),
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
                    Assert.Contains(nameof(Enumerable.Contains), map);
                    Assert.Contains(nameof(Enumerable.Distinct), map);
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
        public void MapIndex_Aggregate_SumAvgMinMax_DecimalArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDecimals, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        SumVal = doc.Values.Sum(),
                        AvgVal = doc.Values.Average(),
                        MinVal = doc.Values.Min(),
                        MaxVal = doc.Values.Max()
                    }
            };

            var docs = new object[]
            {
                new DocWithDecimals { Values = [10.0m, 20.0m] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Sum), map);
                    Assert.Contains(nameof(Enumerable.Average), map);
                    Assert.Contains(nameof(Enumerable.Min), map);
                    Assert.Contains(nameof(Enumerable.Max), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where SumVal = 30.0 and AvgVal = 15.0 and MinVal = 10.0 and MaxVal = 20.0").Count();
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
                    Assert.Contains(nameof(Enumerable.Min), map);
                    Assert.Contains(nameof(Enumerable.Max), map);
                    Assert.Contains(nameof(Enumerable.Average), map);
                    Assert.Contains(nameof(Enumerable.Sum), map);
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
                    Assert.Contains(nameof(Enumerable.Where), map);
                    Assert.Contains(nameof(Enumerable.Select), map);
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Reverse), map),
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
                    Assert.Contains(nameof(Enumerable.Skip), map);
                    Assert.Contains(nameof(Enumerable.Take), map);
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
                    Assert.Contains(nameof(Enumerable.TakeWhile), map);
                    Assert.Contains(nameof(Enumerable.SkipWhile), map);
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Aggregate), map),
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Zip), map),
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
        public void MapIndex_Join_GroupJoin_DecimalArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDecimals, object>
            {
                Map = docs => from doc in docs
                    let other = new decimal[] { 2.0m, 5.0m }
                    select new
                    {
                        doc.Id,
                        // Join [1, 2, 3] with [2, 5]. Match is 2.
                        JoinRes = doc.Values.Join(other, o => o, i => i, (o, i) => o).FirstOrDefault(),
                        // GroupJoin.
                        // 1 matches [], 2 matches [2], 3 matches [].
                        // Count of non-empty groups = 1.
                        GroupJoinCount = doc.Values.GroupJoin(other, o => o, i => i, (o, matches) => matches.Count()).Sum()
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
                    Assert.Contains(nameof(Enumerable.Join), map);
                    Assert.Contains(nameof(Enumerable.GroupJoin), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where JoinRes = 2.0 and GroupJoinCount = 1").Count();
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
                    Assert.Contains(nameof(Enumerable.Any), map);
                    Assert.Contains(nameof(Enumerable.All), map);
                    Assert.Contains(nameof(Enumerable.Count), map);
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
                    Assert.Contains(nameof(Enumerable.First), map);
                    Assert.Contains(nameof(Enumerable.Last), map);
                    Assert.Contains(nameof(Enumerable.Single), map);
                    Assert.Contains(nameof(Enumerable.SingleOrDefault), map);
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
                    Assert.Contains(nameof(Enumerable.ElementAt), map);
                    Assert.Contains(nameof(Enumerable.ElementAtOrDefault), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ValAt1 = 2.0 and ValAtIdx = 1.0").Count();
                        Assert.Equal(1, count);

                        var countDef = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ValDef = null or ValDef = 0").Count();
                        Assert.Equal(1, countDef);
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
                    Assert.Contains(nameof(Enumerable.OrderBy), map);
                    Assert.Contains(nameof(Enumerable.OrderByDescending), map);
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.GroupBy), map),
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
        public void MapIndex_OfType_DecimalArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDecimals, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        OfTypeCount = doc.Values.OfType<decimal>().Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithDecimals { Values = [1.1m, 2.2m] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.OfType), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where OfTypeCount = 2").Count();
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
                        DictCount = doc.Values.ToDictionary(k => k, v => v * 10).Count,
                        // Lookup by integer part. [1.1, 1.9, 2.5] -> Keys: 1, 2
                        LookupCount = doc.Values.ToLookup(k => (int)k).Count
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
                    Assert.Contains(nameof(Enumerable.ToDictionary), map);
                    Assert.Contains(nameof(Enumerable.ToLookup), map);
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
                new DocWithDecimals { Values = new decimal[0] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.DefaultIfEmpty), map),
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
                    Assert.Contains(nameof(Enumerable.Intersect), map);
                    Assert.Contains(nameof(Enumerable.Any), map);
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
                    Assert.Contains(nameof(Enumerable.SequenceEqual), map),
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
                    Assert.Contains(nameof(Enumerable.Except), map);
                    Assert.Contains(nameof(Enumerable.Contains), map);
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
                    Assert.Contains(nameof(Enumerable.Union), map);
                    Assert.Contains(nameof(Enumerable.Contains), map);
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Contains), map),
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Distinct), map),
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Reverse), map),
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Concat), map),
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
                    Assert.Contains(nameof(Enumerable.Min), map);
                    Assert.Contains(nameof(Enumerable.Max), map);
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Where), map),
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.OrderBy), map),
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
                    Assert.Contains(nameof(Enumerable.Skip), map);
                    Assert.Contains(nameof(Enumerable.Take), map);
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Aggregate), map),
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Zip), map),
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
        public void MapIndex_Join_GroupJoin_StringArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithStrings, object>
            {
                Map = docs => from doc in docs
                    let other = new[] { "b", "c" }
                    select new
                    {
                        doc.Id,
                        JoinRes = doc.Tags.Join(other, o => o, i => i, (o, i) => o).FirstOrDefault(),
                        GroupJoinCount = doc.Tags.GroupJoin(other, o => o, i => i, (o, matches) => matches.Count()).Sum()
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
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Join), map);
                    Assert.Contains(nameof(Enumerable.GroupJoin), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // Join: matches "b".
                        // GroupJoin: "a" matches nothing (0), "b" matches "b" (1). Sum = 1.
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where JoinRes = 'b' and GroupJoinCount = 1").Count();
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
                        ValAtDef = doc.Tags.ElementAtOrDefault(99)
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
                    Assert.Contains(nameof(Enumerable.ElementAt), map);
                    Assert.Contains(nameof(Enumerable.ElementAtOrDefault), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ValAtIndex = 'b' and ValAt = 'b' and ValAtDef = null").Count();
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.GroupBy), map),
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
                new DocWithStrings { Tags = new string[0] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.DefaultIfEmpty), map),
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
                    Assert.Contains(nameof(Enumerable.First), map);
                    Assert.Contains(nameof(Enumerable.Last), map);
                    Assert.Contains(nameof(Enumerable.Single), map);
                    Assert.Contains(nameof(Enumerable.SingleOrDefault), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var result = session.Advanced.RawQuery<dynamic>($"from index '{indexName}'").First();
                        Assert.Equal("bat", result.FirstMatch);
                        Assert.Equal("bar", result.LastMatch);
                        Assert.Equal("apple", result.SingleMatch);
                        Assert.Null(result.SingleDef);
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
                    Assert.Contains(nameof(Enumerable.All), map);
                    Assert.Contains(nameof(Enumerable.Any), map);
                    Assert.Contains(nameof(Enumerable.Count), map);
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
                    Assert.Contains(nameof(Enumerable.Min), map);
                    Assert.Contains(nameof(Enumerable.Max), map);
                    Assert.Contains(nameof(Enumerable.Average), map);
                    Assert.Contains(nameof(Enumerable.Sum), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var result = session.Advanced.RawQuery<dynamic>($"from index '{indexName}'").First();
                        Assert.Equal(1, result.MinLen);
                        Assert.Equal(2, result.MaxLen);
                        Assert.Equal(1.5, result.AvgLen);
                        Assert.Equal(3, result.SumLen);
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
                    Assert.Contains(nameof(Enumerable.TakeWhile), map);
                    Assert.Contains(nameof(Enumerable.SkipWhile), map);
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
        public void MapIndex_OfType_StringArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithStrings, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        OfTypeCount = doc.Tags.OfType<string>().Count()
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
                    Assert.Contains(nameof(Enumerable.OfType), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where OfTypeCount = 3").Count();
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
                        DictCount = doc.Tags.ToDictionary(k => k, v => v.Length).Count,
                        // Lookup by first char.
                        LookupCount = doc.Tags.ToLookup(k => k[0]).Count
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
                    Assert.Contains(nameof(Enumerable.ToDictionary), map);
                    Assert.Contains(nameof(Enumerable.ToLookup), map);
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
            var date1 = new DateTime(2023, 1, 1);
            var date2 = new DateTime(2023, 5, 5);
            var date3 = new DateTime(2023, 12, 31);

            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasCommon = doc.ImportantDates.Intersect(new[] { date1, date3 }).Any()
                    }
            };

            var docs = new object[]
            {
                new DocWithDates { ImportantDates = [date1, date2] }, // Has date1
                new DocWithDates { ImportantDates = [date2] }, // No intersection
                new DocWithDates { ImportantDates = [date3] } // Has date3
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Intersect), map);
                    Assert.Contains(nameof(Enumerable.Any), map);
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
        public void MapIndex_SequenceEqual_DateTimeArray_ShouldWork(Options options)
        {
            var date1 = new DateTime(2020, 1, 1);
            var date2 = new DateTime(2021, 1, 1);

            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        IsExact = doc.ImportantDates.SequenceEqual(new[] { date1, date2 })
                    }
            };

            var docs = new object[]
            {
                new DocWithDates { ImportantDates = [date1, date2] },
                new DocWithDates { ImportantDates = [date1] },
                new DocWithDates { ImportantDates = [date2, date1] } // Wrong order
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.SequenceEqual), map),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where IsExact = true").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Except_DateTimeArray_ShouldWork(Options options)
        {
            var date1 = new DateTime(2020, 1, 1);
            var date2 = new DateTime(2020, 2, 2);

            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasRemaining = doc.ImportantDates.Except(new[] { date1 }).Contains(date2)
                    }
            };

            var docs = new object[]
            {
                new DocWithDates { ImportantDates = [date1, date2] },
                new DocWithDates { ImportantDates = [date1] },
                new DocWithDates { ImportantDates = [date2] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Except), map);
                    Assert.Contains(nameof(Enumerable.Contains), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where HasRemaining = true").Count();
                        Assert.Equal(2, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Union_DateTimeArray_ShouldWork(Options options)
        {
            var date1 = new DateTime(2000, 1, 1);
            var date2 = new DateTime(2099, 12, 31);

            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasUnionValue = doc.ImportantDates.Union(new[] { date2 }).Contains(date2)
                    }
            };

            var docs = new object[]
            {
                new DocWithDates { ImportantDates = [date1] }, // Will have date2 after Union
                new DocWithDates { ImportantDates = [date2] } // Already has it
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Union), map);
                    Assert.Contains(nameof(Enumerable.Contains), map);
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
            var target = new DateTime(2022, 2, 24);

            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasDate = doc.ImportantDates.Contains(target)
                    }
            };

            var docs = new object[]
            {
                new DocWithDates { ImportantDates = [new DateTime(2020, 1, 1), target] },
                new DocWithDates { ImportantDates = [new DateTime(2020, 1, 1)] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Contains), map),
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Distinct), map),
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
            var extraDate = new DateTime(2025, 1, 1);

            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        TotalCount = doc.ImportantDates.Concat(new[] { extraDate }).Count()
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Concat), map),
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
                    Assert.Contains(nameof(Enumerable.Min), map);
                    Assert.Contains(nameof(Enumerable.Max), map);
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
                    Assert.Contains(nameof(Enumerable.Min), map);
                    Assert.Contains(nameof(Enumerable.Max), map);
                    Assert.Contains(nameof(Enumerable.Average), map);
                    Assert.Contains(nameof(Enumerable.Sum), map);
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
        public void MapIndex_Where_Select_DateTimeArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Filter years > 2000, select Year, then sum (2001 + 2002 = 4003)
                        YearsSum = doc.ImportantDates
                            .Where(x => x.Year > 2000)
                            .Select(x => x.Year)
                            .Sum()
                    }
            };

            var docs = new object[]
            {
                new DocWithDates { ImportantDates = [new DateTime(1999, 1, 1), new DateTime(2001, 1, 1), new DateTime(2002, 1, 1)] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Where), map);
                    Assert.Contains(nameof(Enumerable.Select), map);
                    Assert.Contains(nameof(Enumerable.Sum), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where YearsSum = 4003").Count();
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
                    Assert.Contains(nameof(Enumerable.Any), map);
                    Assert.Contains(nameof(Enumerable.All), map);
                    Assert.Contains(nameof(Enumerable.Count), map);
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
                    Assert.Contains(nameof(Enumerable.First), map);
                    Assert.Contains(nameof(Enumerable.Last), map);
                    Assert.Contains(nameof(Enumerable.Single), map);
                    Assert.Contains(nameof(Enumerable.SingleOrDefault), map);
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

                        var countNull = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where SingleOrDefaultVal = null").Count();
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
                    Assert.Contains(nameof(Enumerable.OrderBy), map);
                    Assert.Contains(nameof(Enumerable.OrderByDescending), map);
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Reverse), map),
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
                    Assert.Contains(nameof(Enumerable.Skip), map);
                    Assert.Contains(nameof(Enumerable.Take), map);
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
                    Assert.Contains(nameof(Enumerable.TakeWhile), map);
                    Assert.Contains(nameof(Enumerable.SkipWhile), map);
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Aggregate), map),
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Zip), map),
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
        public void MapIndex_Join_GroupJoin_DateTimeArray_ShouldWork(Options options)
        {
            var d1 = new DateTime(2000, 1, 1);
            var d2 = new DateTime(2010, 1, 1);

            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                    let other = new DateTime[] { d1, d2 }
                    select new
                    {
                        doc.Id,
                        JoinCount = doc.ImportantDates.Join(other, outer => outer, inner => inner, (o, i) => o).Count(),
                        GroupJoinCount = doc.ImportantDates.GroupJoin(other, o => o, i => i, (o, matches) => matches.Count()).Sum()
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
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Join), map);
                    Assert.Contains(nameof(Enumerable.GroupJoin), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where JoinCount = 2 and GroupJoinCount = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_ElementAt_Indexer_DateTimeArray_ShouldWork(Options options)
        {
            var d1 = new DateTime(2000, 1, 1);
            var d2 = new DateTime(2010, 1, 1);

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
                new DocWithDates { ImportantDates = [d1, d2] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.ElementAt), map);
                    Assert.Contains(nameof(Enumerable.ElementAtOrDefault), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Val0 = '2000-01-01T00:00:00.0000000' and Val1 = '2010-01-01T00:00:00.0000000'").Count();
                        Assert.Equal(1, count);

                        var countDef = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ValDef = null").Count();
                        Assert.Equal(1, countDef);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_OfType_DateTimeArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        OfTypeCount = doc.ImportantDates.OfType<DateTime>().Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithDates { ImportantDates = [new DateTime(2000, 1, 1)] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.OfType), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where OfTypeCount = 1").Count();
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.GroupBy), map),
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
            var defaultDate = new DateTime(1900, 1, 1);

            var indexBuilder = new IndexDefinitionBuilder<DocWithDates, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Val = doc.ImportantDates.DefaultIfEmpty(defaultDate).First()
                    }
            };

            var docs = new object[]
            {
                new DocWithDates { ImportantDates = new DateTime[0] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.DefaultIfEmpty), map),
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
                        DictCount = doc.ImportantDates.ToDictionary(k => k.Year, v => v.Month).Count,
                        LookupCount = doc.ImportantDates.ToLookup(k => k.Year).Count
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
                    Assert.Contains(nameof(Enumerable.ToDictionary), map);
                    Assert.Contains(nameof(Enumerable.ToLookup), map);
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
                    Assert.Contains(nameof(Enumerable.Intersect), map);
                    Assert.Contains(nameof(Enumerable.Any), map);
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
                            .Union(new char[] { 'z' })
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
                    Assert.Contains(nameof(Enumerable.Union), map);
                    Assert.Contains(nameof(Enumerable.Contains), map);
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
        public void MapIndex_Except_CharArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithChars, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasRemaining = doc.Values
                            .Except(new char[] { 'a' })
                            .Contains('b')
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
                    Assert.Contains(nameof(Enumerable.Except), map);
                    Assert.Contains(nameof(Enumerable.Contains), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where HasRemaining = true").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_SequenceEqual_CharArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithChars, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        IsExact = doc.Values.SequenceEqual(new[] { 'x', 'y' })
                    }
            };

            var docs = new object[]
            {
                new DocWithChars { Values = ['x', 'y'] },
                new DocWithChars { Values = ['x'] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.SequenceEqual), map),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where IsExact = true").Count();
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
                    Assert.Contains(nameof(Enumerable.OrderBy), map);
                    Assert.Contains(nameof(Enumerable.OrderByDescending), map);
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
        public void MapIndex_Contains_CharArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithChars, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        HasVal = doc.Values.Contains('!')
                    }
            };

            var docs = new object[]
            {
                new DocWithChars { Values = ['H', 'e', 'l', 'l', 'o', '!'] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Contains), map),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where HasVal = true").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Where_Select_CharArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithChars, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Digits only, then cast to int, then sum. '1'=49, '2'=50. Sum=99.
                        SumDigits = doc.Values.Where(c => char.IsDigit(c)).Select(c => (int)c).Sum()
                    }
            };

            var docs = new object[]
            {
                new DocWithChars { Values = ['a', '1', 'b', '2'] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Where), map);
                    Assert.Contains(nameof(Enumerable.Select), map);
                    Assert.Contains(nameof(Enumerable.Sum), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where SumDigits = 99").Count();
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Distinct), map),
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Reverse), map),
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
                    Assert.Contains(nameof(Enumerable.Skip), map);
                    Assert.Contains(nameof(Enumerable.Take), map);
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
                    Assert.Contains(nameof(Enumerable.Min), map);
                    Assert.Contains(nameof(Enumerable.Max), map);
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Concat), map),
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
                        ValDef = doc.Values.ElementAtOrDefault(10) // should be default char \0 or null in dynamic
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
                    Assert.Contains(nameof(Enumerable.ElementAt), map);
                    Assert.Contains(nameof(Enumerable.ElementAtOrDefault), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ValAtIndex = 'b' and ValAtIdx = 'b'").Count();
                        Assert.Equal(1, count);

                        var countDef = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ValDef = null or ValDef = '\\u0000'").Count();
                        Assert.Equal(1, countDef);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_First_Last_Single_WithPredicate_CharArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithChars, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        FirstMatch = doc.Values.First(c => c > 'a'),
                        LastMatch = doc.Values.Last(c => c < 'z'),
                        SingleMatch = doc.Values.Single(c => c == 'b'),
                        SingleDefMatch = doc.Values.SingleOrDefault(c => c == '!') // null/default
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
                    Assert.Contains(nameof(Enumerable.First), map);
                    Assert.Contains(nameof(Enumerable.Last), map);
                    Assert.Contains(nameof(Enumerable.Single), map);
                    Assert.Contains(nameof(Enumerable.SingleOrDefault), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // First > 'a' is 'b'
                        // Last < 'z' is 'c'
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where FirstMatch = 'b' and LastMatch = 'c' and SingleMatch = 'b'").Count();
                        Assert.Equal(1, count);

                        var countDef = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where SingleDefMatch = null or SingleDefMatch = '\\u0000'").Count();
                        Assert.Equal(1, countDef);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Any_All_Count_WithPredicate_CharArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithChars, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        AllLetters = doc.Values.All(c => char.IsLetter(c)),
                        AnyDigit = doc.Values.Any(c => char.IsDigit(c)),
                        CountVowels = doc.Values.Count(c => c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u')
                    }
            };

            var docs = new object[]
            {
                new DocWithChars { Values = ['a', 'b', '1'] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.All), map);
                    Assert.Contains(nameof(Enumerable.Any), map);
                    Assert.Contains(nameof(Enumerable.Count), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        // AllLetters: False (contains '1')
                        // AnyDigit: True
                        // CountVowels: 1 ('a')
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where AllLetters = false and AnyDigit = true and CountVowels = 1").Count();
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Aggregate), map),
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Zip), map),
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
                new DocWithChars { Values = new char[0] },
                new DocWithChars { Values = ['a'] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.DefaultIfEmpty), map),
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

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_TakeWhile_SkipWhile_CharArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithChars, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Taken = doc.Values.TakeWhile(c => c < 'c').Count(),
                        Skipped = doc.Values.SkipWhile(c => c < 'c').Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithChars { Values = ['a', 'b', 'c', 'd'] }
            };

            // TakeWhile < 'c' : 'a','b' -> 2
            // SkipWhile < 'c' : 'c','d' -> 2

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.TakeWhile), map);
                    Assert.Contains(nameof(Enumerable.SkipWhile), map);
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
        public void MapIndex_ToDictionary_ToLookup_CharArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithChars, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        DictSize = doc.Values.ToDictionary(c => c.ToString(), c => (int)c).Count,
                        LookupSize = doc.Values.ToLookup(c => char.IsDigit(c)).Count
                    }
            };

            var docs = new object[]
            {
                new DocWithChars { Values = ['a', '1', 'b'] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.ToDictionary), map);
                    Assert.Contains(nameof(Enumerable.ToLookup), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where DictSize = 3 and LookupSize = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Join_GroupJoin_CharArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithChars, object>
            {
                Map = docs => from doc in docs
                    let other = new char[] { 'a', 'c' }
                    select new
                    {
                        doc.Id,
                        JoinCount = doc.Values.Join(other, o => o, i => i, (o, i) => o).Count(),
                        GroupJoinCount = doc.Values.GroupJoin(other, o => o, i => i, (o, matches) => matches.Count()).Sum()
                    }
            };

            var docs = new object[]
            {
                new DocWithChars { Values = ['a', 'b', 'c'] }
            };

            // Join: 'a'=='a', 'c'=='c' -> 2
            // GroupJoin: 'a'->1, 'b'->0, 'c'->1 -> Sum 2

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map =>
                {
                    Assert.Contains(nameof(Enumerable.Join), map);
                    Assert.Contains(nameof(Enumerable.GroupJoin), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where JoinCount = 2 and GroupJoinCount = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_GroupBy_CharArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithChars, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Groups = doc.Values.GroupBy(c => char.IsDigit(c)).Count()
                    }
            };

            var docs = new object[]
            {
                new DocWithChars { Values = ['a', '1', 'b', '2'] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.GroupBy), map),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Groups = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Average_WithSelector_CharArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithChars, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        Avg = doc.Values.Average(c => (int)c)
                    }
            };

            // 'a'=97, 'c'=99. Avg = 98 ('b')

            var docs = new object[]
            {
                new DocWithChars { Values = ['a', 'c'] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Average), map),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where Avg = 98").Count();
                        Assert.Equal(1, count);
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
                    Assert.Contains(nameof(Enumerable.Intersect), map);
                    Assert.Contains(nameof(Enumerable.Any), map);
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
        public void MapIndex_SequenceEqual_BoolArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithBools, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        IsExact = doc.Values.SequenceEqual(new[] { true, false })
                    }
            };

            var docs = new object[]
            {
                new DocWithBools { Values = [true, false] },
                new DocWithBools { Values = [true, true] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.SequenceEqual), map),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where IsExact = true").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Except_BoolArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithBools, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // [true, false] except [false] -> [true]
                        OnlyTrueRemains = doc.Values.Except(new[] { false }).Single()
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
                    Assert.Contains(nameof(Enumerable.Except), map);
                    Assert.Contains(nameof(Enumerable.Single), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where OnlyTrueRemains = true").Count();
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Union), map),
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Contains), map),
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Distinct), map),
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
                    Assert.Contains(nameof(Enumerable.Skip), map);
                    Assert.Contains(nameof(Enumerable.Take), map);
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Concat), map),
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
                    Assert.Contains(nameof(Enumerable.Where), map);
                    Assert.Contains(nameof(Enumerable.Select), map);
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
                    Assert.Contains(nameof(Enumerable.OrderBy), map);
                    Assert.Contains(nameof(Enumerable.OrderByDescending), map);
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
                    Assert.Contains(nameof(Enumerable.Sum), map);
                    Assert.Contains(nameof(Enumerable.Min), map);
                    Assert.Contains(nameof(Enumerable.Max), map);
                    Assert.Contains(nameof(Enumerable.Average), map);
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Reverse), map),
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
                    Assert.Contains(nameof(Enumerable.TakeWhile), map);
                    Assert.Contains(nameof(Enumerable.SkipWhile), map);
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
        public void MapIndex_Aggregate_BoolArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithBools, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        // Logical AND all items. true & true & false = false
                        AllAnd = doc.Values.Aggregate(true, (acc, val) => acc & val)
                    }
            };

            var docs = new object[]
            {
                new DocWithBools { Values = [true, true, false] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Aggregate), map),
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where AllAnd = false").Count();
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.Zip), map),
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
                new DocWithBools { Values = new bool[0] },
                new DocWithBools { Values = [false] }
            };

            AssertIndexBuilderRewritesAndRunsCorrectly(
                options,
                indexBuilder,
                docs,
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.DefaultIfEmpty), map),
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
                additionalMapAsserts: map => Assert.Contains(nameof(Enumerable.GroupBy), map),
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
                    Assert.Contains(nameof(Enumerable.First), map);
                    Assert.Contains(nameof(Enumerable.Last), map);
                    Assert.Contains(nameof(Enumerable.Single), map);
                    Assert.Contains(nameof(Enumerable.SingleOrDefault), map);
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
                    Assert.Contains(nameof(Enumerable.All), map);
                    Assert.Contains(nameof(Enumerable.Any), map);
                    Assert.Contains(nameof(Enumerable.Count), map);
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
                    Assert.Contains(nameof(Enumerable.ElementAt), map);
                    Assert.Contains(nameof(Enumerable.ElementAtOrDefault), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ValAt0 = true and ValAtIndex1 = false").Count();
                        Assert.Equal(1, count);

                        var countDef = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where ValAtDef = false or ValAtDef = null").Count();
                        Assert.Equal(1, countDef);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_OfType_BoolArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithBools, object>
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        OfTypeCount = doc.Values.OfType<bool>().Count()
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
                    Assert.Contains(nameof(Enumerable.OfType), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where OfTypeCount = 2").Count();
                        Assert.Equal(1, count);
                    }
                });
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapIndex_Join_GroupJoin_BoolArray_ShouldWork(Options options)
        {
            var indexBuilder = new IndexDefinitionBuilder<DocWithBools, object>
            {
                Map = docs => from doc in docs
                    let other = new bool[] { true }
                    select new
                    {
                        doc.Id,
                        // Join [true, false] with [true]. Match on true.
                        JoinCount = doc.Values.Join(other, o => o, i => i, (o, i) => o).Count(),

                        // GroupJoin.
                        // true matches [true], false matches [].
                        // Count of groups with elements = 1.
                        GroupJoinCount = doc.Values.GroupJoin(other, o => o, i => i, (o, matches) => matches.Count()).Sum()
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
                    Assert.Contains(nameof(Enumerable.Join), map);
                    Assert.Contains(nameof(Enumerable.GroupJoin), map);
                },
                additionalRunAsserts: (store, indexName) =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Advanced.RawQuery<dynamic>($"from index '{indexName}' where JoinCount = 1 and GroupJoinCount = 1").Count();
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
                        DictCount = doc.Values.ToDictionary(k => k, v => v).Count,
                        // Lookup: true group, false group
                        LookupCount = doc.Values.ToLookup(k => k).Count
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
                    Assert.Contains(nameof(Enumerable.ToDictionary), map);
                    Assert.Contains(nameof(Enumerable.ToLookup), map);
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
