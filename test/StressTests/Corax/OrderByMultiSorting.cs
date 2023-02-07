﻿using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Corax;
using Corax.Mappings;
using Corax.Queries;
using Corax.Utils;
using FastTests.Voron;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Xunit;
using Xunit.Abstractions;
using static Corax.Queries.SortingMatch;

namespace StressTests.Corax
{

    public class OrderByMultiSortingTests : StorageTest
    {
        private readonly List<IndexSingleNumericalEntry<long, long>> longList = new();
        private const int IndexId = 0, Content1 = 1, Content2 = 2;
        private readonly long[] _buffer = new long[200_005];
        public OrderByMultiSortingTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void OrderByNoRepetitions()
        {
            PrepareData();

            IndexEntries();

            // Since there are no repetition, Ascending must not trigger, if it does it is showing an error in the implementation
            // of the sorter logic.
            longList.Sort(CompareDescendingThenAscending);

            using var searcher = new IndexSearcher(Env);
            {
                var match1 = searcher.AllEntries();

                var comparer1 = new DescendingMatchComparer(searcher, new OrderMetadata(searcher.FieldMetadataBuilder("Content1", Content1), false, MatchCompareFieldType.Integer));
                var comparer2 = new AscendingMatchComparer(searcher, new OrderMetadata(searcher.FieldMetadataBuilder("Content2", Content2), true, MatchCompareFieldType.Integer));
                var match = SortingMultiMatch.Create(searcher, match1, comparer1, comparer2);

                List<string> sortedByCorax = new();
                Span<long> ids = _buffer;
                int read = 0;
                do
                {
                    read = match.Fill(ids);
                    for (int i = 0; i < read; ++i)
                        sortedByCorax.Add(searcher.GetIdentityFor(ids[i]));
                }
                while (read != 0);

                for (int i = 0; i < longList.Count; ++i)
                    Assert.Equal(longList[i].Id, sortedByCorax[i]);

                Assert.Equal(100_000, sortedByCorax.Count);
            }
        }

        [Fact]
        public void OrderByWithRepetitions()
        {
            PrepareData();
            PrepareData(inverse:true);

            IndexEntries();
            longList.Sort(CompareAscendingThenDescending);
            using var searcher = new IndexSearcher(Env);
            {
                var match1 = searcher.AllEntries();

                var comparer1 = new AscendingMatchComparer(searcher, new OrderMetadata(searcher.FieldMetadataBuilder("Content1", Content1), true, MatchCompareFieldType.Integer));
                var comparer2 = new DescendingMatchComparer(searcher, new OrderMetadata(searcher.FieldMetadataBuilder("Content2", Content2), false, MatchCompareFieldType.Integer));

                var match = SortingMultiMatch.Create(searcher, match1, comparer1, comparer2);

                List<string> sortedByCorax = new();
                Span<long> ids = _buffer;
                int read = 0;
                do
                {
                    read = match.Fill(ids);
                    for (int i = 0; i < read; ++i)
                        sortedByCorax.Add(searcher.GetIdentityFor(ids[i]));
                }
                while (read != 0);

                for (int i = 0; i < longList.Count; ++i)
                {                    
                    Assert.Equal(longList[i].Id, sortedByCorax[i]);
                }
                    

                Assert.Equal(100_000 * 2, sortedByCorax.Count);
            }
        }

        [Fact]
        public void WithBoosting()
        {
            for (int i = 0; i < 100_000; ++i)            
            {
                longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/{i}", Content1 = i % 2137, });
            }

            IndexEntries();
            longList.Sort(CompareDescending);
            using var searcher = new IndexSearcher(Env);
            {
                //var match = searcher.Or(searcher.Boost(searcher.GreaterThan(searcher.GetCommandEntries(), Content1, 2137), 1000),
                //    searcher.LessThan(searcher.GetCommandEntries(), Content1, 99L));
<<<<<<< Updated upstream
<<<<<<< Updated upstream
                var match = searcher.Boost(searcher.UnaryQuery(searcher.AllEntries(), searcher.FieldMetadataBuilder("Content1", Content1), 2137, UnaryMatchOperation.GreaterThanOrEqual), 1000);
=======
=======
>>>>>>> Stashed changes
                var match = searcher.Boost(searcher.UnaryQuery(searcher.AllEntries(), Content1, 2137, UnaryMatchOperation.GreaterThanOrEqual), 1000);
>>>>>>> Stashed changes
                var sorted = SortingMultiMatch.Create(searcher, match, default(BoostingComparer),
                    new AscendingMatchComparer(searcher, new OrderMetadata(searcher.FieldMetadataBuilder("Id", IndexId), true, MatchCompareFieldType.Sequence)));
                var read = sorted.Fill(_buffer);

                var localResult = longList.Where(x => x.Content1 >= 2137).OrderBy(o => o.Content1).ThenBy(o => o.Id).Select(ll => ll.Id).ToList();
                Assert.Equal(localResult.Count, read);

                var realIds = new List<string>();
                for (var i = 0; i < localResult.Count; ++i)
                    realIds.Add(searcher.GetIdentityFor(_buffer[i]));

                Assert.True(localResult.SequenceEqual(realIds));
            }
        }

        private static int CompareAscending(IndexSingleNumericalEntry<long, long> value1, IndexSingleNumericalEntry<long, long> value2)
        {
            return value1.Content1.CompareTo(value2.Content1);
        }

        private static int CompareAscendingThenDescending(IndexSingleNumericalEntry<long, long> value1, IndexSingleNumericalEntry<long, long> value2)
        {
            var result = value1.Content1.CompareTo(value2.Content1);
            if (result == 0)
                return value2.Content2.CompareTo(value1.Content2);
            return result;
        }

        private static int CompareDescending(IndexSingleNumericalEntry<long, long> value1, IndexSingleNumericalEntry<long, long> value2)
        {
            return value2.Content1.CompareTo(value1.Content1);
        }

        private static int CompareDescendingThenAscending(IndexSingleNumericalEntry<long, long> value1, IndexSingleNumericalEntry<long, long> value2)
        {
            var result = value2.Content1.CompareTo(value1.Content1);
            if (result == 0)
                return value1.Content2.CompareTo(value2.Content2);
            return result;
        }

        private void PrepareData(bool inverse = false)
        {
            for (int i = 0; i < 100_000; ++i)
            {
                longList.Add(new IndexSingleNumericalEntry<long, long>
                {
                    Id = inverse ? $"list/1000-{i}" : $"list/{i}",
                    Content1 = i,
                    Content2 = inverse ? 100_000 - i : i
                });
            }
        }

        private void IndexEntries()
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            using var knownFields = CreateKnownFields(bsc);

            {
                using var indexWriter = new IndexWriter(Env, knownFields);
                var entryWriter = new IndexEntryWriter(bsc, knownFields);

                foreach (var entry in longList)
                {
                    using (var _ = CreateIndexEntry(ref entryWriter, entry, out var data))
                    {
                        indexWriter.Index(entry.Id, data.ToSpan());
                    }
                }
                indexWriter.Commit();
            }
        }

        private ByteStringContext<ByteStringMemoryCache>.InternalScope CreateIndexEntry(
            ref IndexEntryWriter entryWriter, IndexSingleNumericalEntry<long, long> entry, out ByteString output)
        {
            entryWriter.Write(IndexId, Encoding.UTF8.GetBytes(entry.Id));
            entryWriter.Write(Content1, Encoding.UTF8.GetBytes(entry.Content1.ToString()), entry.Content1, Convert.ToDouble(entry.Content1));
            entryWriter.Write(Content2, Encoding.UTF8.GetBytes(entry.Content2.ToString()), entry.Content2, Convert.ToDouble(entry.Content2));
            return entryWriter.Finish(out output);
        }

        private IndexFieldsMapping CreateKnownFields(ByteStringContext bsc)
        {
            Slice.From(bsc, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(bsc, "Content1", ByteStringType.Immutable, out Slice content1Slice);
            Slice.From(bsc, "Content2", ByteStringType.Immutable, out Slice content2Slice);

            using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
                        .AddBinding(IndexId, idSlice)
                        .AddBinding(Content1, content1Slice)
                        .AddBinding(Content2, content2Slice);
            return builder.Build();
        }

        private class IndexSingleNumericalEntry<T1, T2>
        {
            public string Id { get; set; }
            public T1 Content1 { get; set; }
            public T2 Content2 { get; set; }
        }
    }
}
