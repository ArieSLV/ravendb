﻿using System.Collections.Generic;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;
using static Raven.Server.Documents.Schemas.DeletedRanges;
using static Raven.Server.Documents.Schemas.TimeSeries;

namespace Raven.Server.Documents.TimeSeries
{
    public partial class TimeSeriesStorage
    {
        public IEnumerable<TimeSeriesReplicationItem> GetSegmentsByBucketFrom(DocumentsOperationContext context, int bucket, long etag)
        {
            var table = new Table(TimeSeriesSchema, context.Transaction.InnerTransaction);

            foreach (var result in ShardedDocumentsStorage.GetItemsByBucket(context.Allocator, table, TimeSeriesSchema.DynamicKeyIndexes[TimeSeriesBucketAndEtagSlice], bucket, etag))
            {
                yield return CreateTimeSeriesSegmentItem(context, ref result.Result.Reader);
            }
        }

        public IEnumerable<TimeSeriesDeletedRangeItem> GetDeletedRangesByBucketFrom(DocumentsOperationContext context, int bucket, long etag)
        {
            var table = new Table(DeleteRangesSchema, context.Transaction.InnerTransaction);

            foreach (var result in ShardedDocumentsStorage.GetItemsByBucket(context.Allocator, table, DeleteRangesSchema.DynamicKeyIndexes[DeletedRangesBucketAndEtagSlice], bucket, etag))
            {
                yield return CreateDeletedRangeItem(context, ref result.Result.Reader);
            }
        }

        [StorageIndexEntryKeyGenerator]
        internal static ByteStringContext.Scope GenerateBucketAndEtagIndexKeyForTimeSeries(ByteStringContext context, ref TableValueReader tvr, out Slice slice)
        {
            return ShardedDocumentsStorage.ExtractIdFromKeyAndGenerateBucketAndEtagIndexKey(context, keyIndex: (int)TimeSeriesTable.TimeSeriesKey,
                etagIndex: (int)TimeSeriesTable.Etag, ref tvr, out slice);
        }

        [StorageIndexEntryKeyGenerator]
        internal static ByteStringContext.Scope GenerateBucketAndEtagIndexKeyForDeletedRanges(ByteStringContext context, ref TableValueReader tvr, out Slice slice)
        {
            return ShardedDocumentsStorage.ExtractIdFromKeyAndGenerateBucketAndEtagIndexKey(context, keyIndex: (int)DeletedRangeTable.RangeKey,
                etagIndex: (int)DeletedRangeTable.Etag, ref tvr, out slice);
        }
    }
}