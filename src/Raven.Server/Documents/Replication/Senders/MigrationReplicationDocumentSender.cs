﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Logging;

namespace Raven.Server.Documents.Replication.Senders
{
    public class MigrationReplicationDocumentSender : ReplicationDocumentSenderBase
    {
        public readonly BucketMigrationReplication Destination;
        public readonly ShardedDocumentDatabase Database;
        public OutgoingMigrationReplicationHandler Parent;
        private ChangeVector _lastBatchChangeVector;

        public MigrationReplicationDocumentSender(Stream stream, OutgoingMigrationReplicationHandler parent, Logger log) : base(stream, parent, log)
        {
            Destination = parent.BucketMigrationNode;
            Database = (ShardedDocumentDatabase)parent._parent.Database;
            Parent = parent;
        }

        protected override IEnumerable<ReplicationBatchItem> GetReplicationItems(DocumentsOperationContext ctx, long etag, ReplicationStats stats, bool caseInsensitiveCounters)
        {
            var database = ShardedDocumentDatabase.CastToShardedDocumentDatabase(ctx.DocumentDatabase);
            var documentsStorage = database.ShardedDocumentsStorage;

            var bucket = Destination.Bucket;
            _lastBatchChangeVector = ctx.GetChangeVector(string.Empty);

            var docs = documentsStorage.GetDocumentsByBucketFrom(ctx, bucket, etag + 1).Select(DocumentReplicationItem.From);
            var tombs = documentsStorage.GetTombstonesByBucketFrom(ctx, bucket, etag + 1);
            var conflicts = documentsStorage.ConflictsStorage.GetConflictsByBucketFrom(ctx, bucket, etag + 1).Select(DocumentReplicationItem.From);
            var revisionsStorage = documentsStorage.RevisionsStorage;
            var revisions = revisionsStorage.GetRevisionsByBucketFrom(ctx, bucket, etag + 1).Select(DocumentReplicationItem.From);
            var attachments = documentsStorage.AttachmentsStorage.GetAttachmentsByBucketFrom(ctx, bucket, etag + 1);
            var counters = documentsStorage.CountersStorage.GetCountersByBucketFrom(ctx, bucket, etag + 1);
            var timeSeries = documentsStorage.TimeSeriesStorage.GetSegmentsByBucketFrom(ctx, bucket, etag + 1);
            var deletedTimeSeriesRanges = documentsStorage.TimeSeriesStorage.GetDeletedRangesByBucketFrom(ctx, bucket, etag + 1);

            using (var docsIt = docs.GetEnumerator())
            using (var tombsIt = tombs.GetEnumerator())
            using (var conflictsIt = conflicts.GetEnumerator())
            using (var versionsIt = revisions.GetEnumerator())
            using (var attachmentsIt = attachments.GetEnumerator())
            using (var countersIt = counters.GetEnumerator())
            using (var timeSeriesIt = timeSeries.GetEnumerator())
            using (var deletedTimeSeriesRangesIt = deletedTimeSeriesRanges.GetEnumerator())
            using (var mergedInEnumerator = new MergedReplicationBatchEnumerator(stats.DocumentRead, stats.AttachmentRead, stats.TombstoneRead, stats.CounterRead, stats.TimeSeriesRead))
            {
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.Document, docsIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.DocumentTombstone, tombsIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.Document, conflictsIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.Document, versionsIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.Attachment, attachmentsIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.CounterGroup, countersIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.TimeSeriesSegment, timeSeriesIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.DeletedTimeSeriesRange, deletedTimeSeriesRangesIt);

                while (mergedInEnumerator.MoveNext())
                {
                    yield return mergedInEnumerator.Current;
                }

                if (_lastBatchChangeVector.IsNullOrEmpty == false)
                    Parent.LastChangeVectorInBucket = _lastBatchChangeVector;
            }

        }

        protected override bool ShouldSkip(DocumentsOperationContext context, ReplicationBatchItem item, OutgoingReplicationStatsScope stats, SkippedReplicationItemsInfo skippedReplicationItemsInfo)
        {
            var flags = item switch
            {
                DocumentReplicationItem doc => doc.Flags,
                AttachmentTombstoneReplicationItem attachmentTombstone => attachmentTombstone.Flags,
                RevisionTombstoneReplicationItem revisionTombstone => revisionTombstone.Flags,
                _ => DocumentFlags.None
            };

            if (flags.Contain(DocumentFlags.Artificial))
            {
                stats.RecordArtificialDocumentSkip();
                skippedReplicationItemsInfo.Update(item, isArtificial: true);
                return true;
            }

            _lastBatchChangeVector = _lastBatchChangeVector.MergeWith(item.ChangeVector, context);
            return false;
        }
    }
}