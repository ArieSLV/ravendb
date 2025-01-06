﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Config.Settings;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using Bits = Sparrow.Binary.Bits;

namespace Raven.Server.Documents.PeriodicBackup.BackupHistory;

public unsafe class BackupHistoryStorage
{
    private const string JsonDocumentId = "backup-history-entry";

    private StorageEnvironment _environment;
    private TransactionContextPool _contextPool;

    private readonly Logger _logger = LoggingSource.Instance.GetLogger<BackupHistoryStorage>("Server");

    private static readonly TableSchema BackupHistoryTableSchema = new();
    private static readonly TableSchema BackupResultDetailsTableSchema = new();

    private static readonly Slice ByFullBackupIdSlice;
    private static readonly Slice ByBackupKindSlice;

    public SystemTime Time = new SystemTime();


    static BackupHistoryStorage()
    {
        using (StorageEnvironment.GetStaticContext(out var ctx))
        {
            Slice.From(ctx, "by-backup-kind", out ByBackupKindSlice);
            Slice.From(ctx, "by-full-backup-id", out ByFullBackupIdSlice);
        }

        BackupHistoryTableSchema.DefineKey(new TableSchema.SchemaIndexDef
        {
            StartIndex = (int)BackupHistorySchema.BackupHistoryColumns.PrimaryKey,
            Count = 1
        });

        BackupHistoryTableSchema.DefineIndex(new TableSchema.SchemaIndexDef
        {
            Name = ByFullBackupIdSlice,
            StartIndex = (int)BackupHistorySchema.BackupHistoryColumns.ByFullBackupIdKey,
            Count = 1
        });

        BackupHistoryTableSchema.DefineIndex(new TableSchema.SchemaIndexDef
        {
            Name = ByBackupKindSlice,
            StartIndex = (int)BackupHistorySchema.BackupHistoryColumns.ByBackupKindKey,
            Count = 1
        });
        
        BackupResultDetailsTableSchema.DefineKey(new TableSchema.SchemaIndexDef
        {
            StartIndex = (int)BackupResultDetailsSchema.BackupResultDetailsColumns.PrimaryKey,
            Count = 1
        });
    }

    public void Initialize(StorageEnvironment environment, TransactionContextPool contextPool)
    {
        _environment = environment;
        _contextPool = contextPool;

        using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (var tx = _environment.WriteTransaction(context.PersistentContext))
        {
            BackupHistoryTableSchema.Create(tx, BackupHistorySchema.TableName, 16);
            BackupResultDetailsTableSchema.Create(tx, BackupResultDetailsSchema.TableName, 16);
            tx.Commit();
        }
    }

    public void StoreBackupStatus(string databaseName, PeriodicBackupStatus status, TimeSetting backupHistoryRetentionPeriod)
    {
        var entry = new BackupHistoryEntry(status);
        var taskId = status.TaskId;

        using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (var tx = context.OpenWriteTransaction())
        {
            var table = tx.InnerTransaction.OpenTable(BackupHistoryTableSchema, BackupHistorySchema.TableName);

            StoreNewBackupEntry(table, entry, databaseName, taskId, context);

            if (entry.BackupKind == BackupKind.Full && entry.Error == null)
                EnforceBackupRetentionPolicy(table, tx, databaseName, taskId, backupHistoryRetentionPeriod);

            tx.Commit();
        }
    }

    private static void StoreNewBackupEntry(Table table, BackupHistoryEntry entry, string databaseName, long taskId, TransactionOperationContext context)
    {
        var createdAtTicks = entry.CreatedAt.Ticks;
        var fullBackupCreatedAtTicks = entry.BackupKind is BackupKind.Full
            ? createdAtTicks
            : entry.LastFullBackup?.Ticks ?? 0;

        var json = context.ReadObject(entry.ToJson(), JsonDocumentId);

        using (BackupHistorySchema.GetPrimaryKey(context.Allocator, databaseName, taskId, fullBackupCreatedAtTicks, createdAtTicks, out var primaryKeySlice))
        using (BackupHistorySchema.GetByBackupKindIndexKey(context.Allocator, databaseName, taskId, entry.BackupKind, fullBackupCreatedAtTicks, createdAtTicks, out var byBackupKindKeySlice))
        using (BackupHistorySchema.GetByFullBackupIdIndexKey(context.Allocator, databaseName, taskId, fullBackupCreatedAtTicks, entry.BackupKind, createdAtTicks, out var byFullBackupIdKeySlice))
        using (table.Allocate(out TableValueBuilder tvb))
        {
            tvb.Add(primaryKeySlice);
            tvb.Add(byBackupKindKeySlice);
            tvb.Add(byFullBackupIdKeySlice);
            tvb.Add(Bits.SwapBytes(createdAtTicks));
            tvb.Add(json.BasePointer, json.Size);

            table.Set(tvb);
        }
    }

    private void EnforceBackupRetentionPolicy(Table backupHistoryTable, RavenTransaction tx, string databaseName, long taskId, TimeSetting backupHistoryRetentionPeriod)
    {
        var cutoffTime = Time.GetUtcNow() - (_forTestingPurposes?.CustomBackupHistoryRetentionConfiguration ?? backupHistoryRetentionPeriod.AsTimeSpan);
        Console.WriteLine($"SystemTime.UtcNow: {Time.GetUtcNow():O}");
        var backupResultDetailsTable = tx.InnerTransaction.OpenTable(BackupResultDetailsTableSchema, BackupResultDetailsSchema.TableName);

        var tableIdsList = CollectBackupsToDelete(backupHistoryTable, tx, cutoffTime, databaseName, taskId);

        foreach ((long historyTableId, long resultDetailsId) in tableIdsList)
        {
            backupHistoryTable.Delete(historyTableId);

            if (resultDetailsId > 0)
                backupResultDetailsTable.Delete(resultDetailsId);
        }
    }

    private List<(long HistoryTableId, long ResultDetailsId)> CollectBackupsToDelete(Table backupHistoryTable, RavenTransaction tx, DateTime cutoffTime, string databaseName, long taskId)
    {
        var result = new List<(long, long)>();

        using (BackupHistorySchema.GetByBackupKindIndexKeyPrefix(tx.InnerTransaction.Allocator, databaseName, taskId, BackupKind.Full, out var fullBackupPrefixSlice))
        using (BackupHistorySchema.GetByBackupKindIndexKey(tx.InnerTransaction.Allocator, databaseName, taskId, BackupKind.Full, fullBackupCreatedAtTicks: -1, createdAtTicks: -1, out var fullBackupLastSlice))
        using (BackupHistorySchema.GetByBackupKindIndexKey(tx.InnerTransaction.Allocator, databaseName, taskId, BackupKind.Full, fullBackupCreatedAtTicks: 1, createdAtTicks: 1, out var fullBackupStartSlice))
        {
            var latestFullBackupValue = backupHistoryTable.SeekOneBackwardFrom(BackupHistoryTableSchema.Indexes[ByBackupKindSlice], fullBackupPrefixSlice, fullBackupLastSlice);
            var latestFullBackupCreatedAtTicks = Bits.SwapBytes(*(long*)latestFullBackupValue.Reader.Read((int)BackupHistorySchema.BackupHistoryColumns.CreatedAtTicks, out _));
            Console.WriteLine();
            Console.WriteLine($"Latest full backup created at: {new DateTime(latestFullBackupCreatedAtTicks):O}.");
            foreach (var fullBackupSeekResult in backupHistoryTable.SeekForwardFromPrefix(BackupHistoryTableSchema.Indexes[ByBackupKindSlice], fullBackupStartSlice, fullBackupPrefixSlice, skip: 0))
            {
                var fullBackupCreatedAtTicks = Bits.SwapBytes(*(long*)fullBackupSeekResult.Result.Reader.Read((int)BackupHistorySchema.BackupHistoryColumns.CreatedAtTicks, out _));
                // If the full backup is newer than the cutoff time, we can stop searching as entries are sorted chronologically
                // If the full backup is the latest one, we won't delete it
                if (fullBackupCreatedAtTicks >= cutoffTime.Ticks || fullBackupCreatedAtTicks == latestFullBackupCreatedAtTicks)
                {
                    Console.WriteLine($"Full backup created at: {new DateTime(fullBackupCreatedAtTicks):O} is newer than the cutoff time: {cutoffTime:O}, fullBackupCreatedAtTicks == latestFullBackupCreatedAtTicks: {fullBackupCreatedAtTicks == latestFullBackupCreatedAtTicks}.");
                    break;
                }

                Console.WriteLine($"Full backup created at: {new DateTime(fullBackupCreatedAtTicks):O} is older than the cutoff time: {cutoffTime:O}, fullBackupCreatedAtTicks == latestFullBackupCreatedAtTicks: {fullBackupCreatedAtTicks == latestFullBackupCreatedAtTicks}.");

                var fullBackupResultDetailsId = -1L;
                if (TryGetBackupResultDetailsReader(tx, databaseName, taskId, fullBackupCreatedAtTicks, out var fullBackupReader))
                    fullBackupResultDetailsId = fullBackupReader.Id;

                result.Add((fullBackupSeekResult.Result.Reader.Id, fullBackupResultDetailsId));

                using (BackupHistorySchema.GetByFullBackupIdIndexKeyPrefix(tx.InnerTransaction.Allocator, databaseName, taskId, fullBackupCreatedAtTicks, BackupKind.Incremental, out var prefixSlice))
                using (BackupHistorySchema.GetByFullBackupIdIndexKey(tx.InnerTransaction.Allocator, databaseName, taskId, fullBackupCreatedAtTicks, BackupKind.Incremental, createdAtTicks: 1, out var firstSlice))
                {
                    foreach (var incrementalBackupSeekResult in backupHistoryTable.SeekForwardFromPrefix(BackupHistoryTableSchema.Indexes[ByFullBackupIdSlice], firstSlice, prefixSlice, skip: 0))
                    {
                        var incrementalBackupCreatedAtTicks = Bits.SwapBytes(*(long*)incrementalBackupSeekResult.Result.Reader.Read((int)BackupHistorySchema.BackupHistoryColumns.CreatedAtTicks, out _));
                        Console.WriteLine($"Incremental backup created at: {new DateTime(incrementalBackupCreatedAtTicks):O}.");

                        var incrementalBackupResultDetailsId = -1L;
                        if (TryGetBackupResultDetailsReader(tx, databaseName, taskId, incrementalBackupCreatedAtTicks, out var incrementalBackupReader))
                            incrementalBackupResultDetailsId = incrementalBackupReader.Id;

                        result.Add((incrementalBackupSeekResult.Result.Reader.Id, incrementalBackupResultDetailsId));
                    }
                }
            }
        }

        return result;
    }

    public void StoreBackupResultDetails(BackupResult result, PeriodicBackupStatus status, string databaseName)
    {
        var key = BackupResultDetailsSchema.GenerateKey(databaseName, status);

        if (_logger.IsInfoEnabled)
            _logger.Info($"Saving to backup history storage result details with key: `{key}`.");

        using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (var transaction = context.OpenWriteTransaction())
        {
            var table = transaction.InnerTransaction.OpenTable(BackupResultDetailsTableSchema, BackupResultDetailsSchema.TableName);

            var lsKey = context.GetLazyString(key);
            using (var json = context.ReadObject(result.ToJson(), key, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
            using (table.Allocate(out TableValueBuilder tvb))
            {
                tvb.Add(lsKey.Buffer, lsKey.Size);
                tvb.Add(json.BasePointer, json.Size);

                table.Set(tvb);
            }

            transaction.Commit();
        }
    }

    public static BlittableJsonReaderObject GetBackupResult(TransactionOperationContext context, string databaseName, long taskId, long createdAtTicks)
    {
        using var tx = context.OpenReadTransaction();
        if (TryGetBackupResultDetailsReader(tx, databaseName, taskId, createdAtTicks, out var tvr) == false)
            return null;

        var jsonPointer = tvr.Read((int)BackupResultDetailsSchema.BackupResultDetailsColumns.Data, out var size);
        var json = new BlittableJsonReaderObject(jsonPointer, size, context);

        return json;
    }

    private static bool TryGetBackupResultDetailsReader(RavenTransaction tx, string databaseName, long taskId, long createdAtTicks, out TableValueReader tvr)
    {
        var key = BackupResultDetailsSchema.GenerateKey(databaseName, taskId, createdAtTicks);

        var table = tx.InnerTransaction.OpenTable(BackupResultDetailsTableSchema, BackupResultDetailsSchema.TableName);
        using (Slice.From(tx.InnerTransaction.Allocator, key, out Slice slice))
            return table.ReadByKey(slice, out tvr);
    }

    public static BlittableJsonReaderObject GetBackupHistory(
        TransactionOperationContext context,
        DatabaseRecord databaseRecord,
        bool includeIncrementals,
        long? requestedTaskId = null,
        long? requestedFullBackupCreatedAtTicks = null)
    {
        var backupHistory = new BackupHistory(databaseRecord.DatabaseName);
        var taskIds = requestedTaskId.HasValue
            ? [requestedTaskId.Value]
            : databaseRecord.PeriodicBackups.Select(configuration => configuration.TaskId);

        using (var tx = context.OpenReadTransaction())
        {
            var table = tx.InnerTransaction.OpenTable(BackupHistoryTableSchema, BackupHistorySchema.TableName);
            foreach (var taskId in taskIds)
                PopulateBackupHistory(backupHistory, context, table, includeIncrementals, taskId, requestedFullBackupCreatedAtTicks);
        }

        backupHistory.UpdateTaskNames(databaseRecord);
        return context.ReadObject(backupHistory.ToJson(), nameof(BackupHistory));
    }

    private static void PopulateBackupHistory(BackupHistory backupHistory, TransactionOperationContext context, Table table, bool includeIncrementals, long taskId, long? fullBackupCreatedAtTicks)
    {
        if (includeIncrementals)
        {
            using (BackupHistorySchema.GetPrimaryKeyPrefix(context.Allocator, backupHistory.DatabaseName, taskId, fullBackupCreatedAtTicks, out Slice prefixSlice))
            {
                foreach ((_, Table.TableValueHolder result) in table.SeekByPrimaryKeyPrefix(prefixSlice, Slices.Empty, skip: 0))
                {
                    var entry = GetBackupEntry(context, result.Reader);
                    backupHistory.Add(entry, taskId);
                }
            }
        }
        else if (fullBackupCreatedAtTicks.HasValue)
        {
            using (BackupHistorySchema.GetPrimaryKey(context.Allocator, backupHistory.DatabaseName, taskId, fullBackupCreatedAtTicks.Value, fullBackupCreatedAtTicks.Value, out Slice keySlice))
            {
                if (table.ReadByKey(keySlice, out var value))
                    AddBackupEntryWithIncrementalsCountOnly(context, table, backupHistory, taskId, value);
            }
        }
        else
        {
            using (BackupHistorySchema.GetByBackupKindIndexKeyPrefix(context.Allocator, backupHistory.DatabaseName, taskId, BackupKind.Full, out var fullBackupPrefixSlice))
            using (BackupHistorySchema.GetByBackupKindIndexKey(context.Allocator, backupHistory.DatabaseName, taskId, BackupKind.Full, fullBackupCreatedAtTicks: 1, createdAtTicks: 1, out var fullBackupStartSlice))
            {
                foreach (var fullBackupSeekResult in table.SeekForwardFromPrefix(BackupHistoryTableSchema.Indexes[ByBackupKindSlice], fullBackupStartSlice, fullBackupPrefixSlice, skip: 0))
                    AddBackupEntryWithIncrementalsCountOnly(context, table, backupHistory, taskId, fullBackupSeekResult.Result.Reader);
            }
        }
    }

    private static void AddBackupEntryWithIncrementalsCountOnly(TransactionOperationContext context, Table table, BackupHistory backupHistory, long taskId, TableValueReader result)
    {
        var entry = GetBackupEntry(context, result);
        backupHistory.Add(entry, taskId);

        using (BackupHistorySchema.GetByFullBackupIdIndexKeyPrefix(context.Allocator, backupHistory.DatabaseName, taskId, entry.CreatedAt.Ticks, BackupKind.Incremental, out var prefixSlice))
        using (BackupHistorySchema.GetByFullBackupIdIndexKey(context.Allocator, backupHistory.DatabaseName, taskId, entry.CreatedAt.Ticks, BackupKind.Incremental, createdAtTicks: 1, out var startSlice))
        {
            var count = table.SeekForwardFromPrefix(BackupHistoryTableSchema.Indexes[ByFullBackupIdSlice], startSlice, prefixSlice, skip: 0).Count();
            backupHistory.Groups.Single(group => group.FullBackup?.CreatedAt == entry.CreatedAt).IncrementalBackupsCount = count;
        }
    }

    private static BackupHistoryEntry GetBackupEntry(TransactionOperationContext context, TableValueReader result)
    {
        var ptr = result.Read((int)BackupHistorySchema.BackupHistoryColumns.Data, out var size);
        var json = new BlittableJsonReaderObject(ptr, size, context);
        Transaction.DebugDisposeReaderAfterTransaction(context.Transaction.InnerTransaction, json);
        return JsonDeserializationServer.BackupHistoryEntry(json);
    }

    private static long GetBackupHistoryEntriesNumber(TransactionOperationContext context)
    {
        using var tx = context.OpenReadTransaction();
        var table = tx.InnerTransaction.OpenTable(BackupHistoryTableSchema, BackupHistorySchema.TableName);

        return table.NumberOfEntries;
    }

    private static long GetBackupResultDetailsNumber(TransactionOperationContext context)
    {
        using var tx = context.OpenReadTransaction();
        var table = tx.InnerTransaction.OpenTable(BackupResultDetailsTableSchema, BackupResultDetailsSchema.TableName);

        return table.NumberOfEntries;
    }

    private static class BackupHistorySchema
    {
        public const string TableName = "BackupHistoryTable";
        private const string ValuesPrefix = "values/";

        public enum BackupHistoryColumns
        {
            PrimaryKey = 0,
            ByBackupKindKey = 1,
            ByFullBackupIdKey = 2,
            CreatedAtTicks = 3,
            Data = 4
        }

        /// <summary>
        /// Key structure: values/{databaseName}/{taskId}/{fullBackupCreatedAtTicks}/{createdAtTicks}
        /// </summary>
        public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetPrimaryKey(
            ByteStringContext allocator,
            string databaseName,
            long taskId,
            long fullBackupCreatedAtTicks,
            long createdAtTicks,
            out Slice keySlice)
        {
            return GetKey(allocator, databaseName, taskId, fullBackupCreatedAtTicks, createdAtTicks, out keySlice);
        }

        /// <summary>
        /// Key prefix structure: values/{databaseName}/{taskId}/{fullBackupCreatedAtTicks}
        /// </summary>
        public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetPrimaryKeyPrefix(
            ByteStringContext allocator,
            string databaseName,
            long taskId,
            long? fullBackupCreatedAtTicks,
            out Slice keySlice)
        {
            return fullBackupCreatedAtTicks.HasValue
                ? GetKey(allocator, databaseName, taskId, fullBackupCreatedAtTicks.Value, out keySlice)
                : GetKey(allocator, databaseName, taskId, out keySlice);
        }

        /// <summary>
        /// Key structure: values/{databaseName}/{taskId}/{backupKindValue}/{fullBackupCreatedAtTicks}/{createdAtTicks}
        /// </summary>
        public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetByBackupKindIndexKey(
            ByteStringContext allocator,
            string databaseName,
            long taskId,
            BackupKind backupKind,
            long fullBackupCreatedAtTicks,
            long createdAtTicks,
            out Slice keySlice)
        {
            return GetKey(allocator, databaseName, taskId, (long)backupKind, fullBackupCreatedAtTicks, createdAtTicks, out keySlice);
        }

        /// <summary>
        /// Key prefix structure: values/{databaseName}/{taskId}/{backupKindValue}
        /// </summary>
        public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetByBackupKindIndexKeyPrefix(
            ByteStringContext allocator,
            string databaseName,
            long taskId,
            BackupKind backupKind,
            out Slice keySlice)
        {
            return GetKey(allocator, databaseName, taskId, (long)backupKind, out keySlice);
        }

        /// <summary>
        /// Key structure: values/{databaseName}/{taskId}/{fullBackupCreatedAtTicks}/{backupKindValue}/{createdAtTicks}
        /// </summary>
        public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetByFullBackupIdIndexKey(
            ByteStringContext allocator,
            string databaseName,
            long taskId,
            long fullBackupCreatedAtTicks,
            BackupKind backupKind,
            long createdAtTicks,
            out Slice keySlice)
        {
            return GetKey(allocator, databaseName, taskId, fullBackupCreatedAtTicks, (long)backupKind, createdAtTicks, out keySlice);
        }

        /// <summary>
        /// Key prefix structure: values/{databaseName}/{taskId}/{fullBackupCreatedAtTicks}/{BackupKind}
        /// </summary>
        public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetByFullBackupIdIndexKeyPrefix(
            ByteStringContext allocator,
            string databaseName,
            long taskId,
            long fullBackupCreatedAtTicks,
            BackupKind backupKind,
            out Slice keySlice)
        {
            return GetKey(allocator, databaseName, taskId, fullBackupCreatedAtTicks, (long)backupKind, out keySlice);
        }

        private static ByteStringContext<ByteStringMemoryCache>.InternalScope GetKey(
            ByteStringContext allocator,
            string databaseName,
            long taskId,
            out Slice keySlice)
        {
            Span<long> values = stackalloc long[] { taskId };
            return GetKey(allocator, databaseName, values, out keySlice);
        }

        private static ByteStringContext<ByteStringMemoryCache>.InternalScope GetKey(
            ByteStringContext allocator,
            string databaseName,
            long taskId,
            long value1,
            out Slice keySlice)
        {
            Span<long> values = stackalloc long[] { taskId, value1 };
            return GetKey(allocator, databaseName, values, out keySlice);
        }

        private static ByteStringContext<ByteStringMemoryCache>.InternalScope GetKey(
            ByteStringContext allocator,
            string databaseName,
            long taskId,
            long value1,
            long value2,
            out Slice keySlice)
        {
            Span<long> values = stackalloc long[] { taskId, value1, value2 };
            return GetKey(allocator, databaseName, values, out keySlice);
        }

        private static ByteStringContext<ByteStringMemoryCache>.InternalScope GetKey(
            ByteStringContext allocator,
            string databaseName,
            long taskId,
            long value1,
            long value2,
            long value3,
            out Slice keySlice)
        {
            Span<long> values = stackalloc long[] { taskId, value1, value2, value3 };
            return GetKey(allocator, databaseName, values, out keySlice);
        }

        private static ByteStringContext<ByteStringMemoryCache>.InternalScope GetKey(
            ByteStringContext allocator,
            string databaseName,
            Span<long> values,
            out Slice keySlice)
        {
            var prefixLength = Encoding.UTF8.GetByteCount(ValuesPrefix);
            var dbNameLength = Encoding.UTF8.GetByteCount(databaseName);
            var numbersLength = sizeof(long) * (1 + values.Length);
            var separatorsLength = 1 + values.Length;

            var totalLength = prefixLength + dbNameLength + numbersLength + separatorsLength;

            var scope = allocator.Allocate(totalLength, out var buffer);

            var pos = buffer.Ptr;

            foreach (var ch in ValuesPrefix)
                *pos++ = (byte)char.ToLowerInvariant(ch);

            foreach (var ch in databaseName)
                *pos++ = (byte)char.ToLowerInvariant(ch);

            foreach (var value in values)
            {
                *pos++ = (byte)'/';
                var swappedValue = Bits.SwapBytes(value);
                *(long*)pos = swappedValue;
                pos += sizeof(long);
            }

            var size = (int)(pos - buffer.Ptr);
            buffer.Truncate(size);

            keySlice = new Slice(buffer);
            return scope;
        }
    }

    private static class BackupResultDetailsSchema
    {
        public const string TableName = "BackupResultDetailsTable";

        public enum BackupResultDetailsColumns
        {
            PrimaryKey = 0,
            Data = 1
        }

        /// <summary>
        /// Key structure: values/{databaseName}/{taskId}/{createdAtTicks}
        /// </summary>
        public static string GenerateKey(string databaseName, PeriodicBackupStatus status)
        {
            var createdAt = status.IsFull
                ? status.LastFullBackup ?? status.Error.At
                : status.LastIncrementalBackup ?? status.Error.At;

            return GenerateKey(databaseName, status.TaskId, createdAt.Ticks);
        }

        /// <summary>
        /// Key structure: values/{databaseName}/{taskId}/{createdAtTicks}
        /// </summary>
        public static string GenerateKey(string databaseName, long taskId, long createdAtTicks) =>
            $"values/{databaseName}/{taskId}/{createdAtTicks}";
    }

    private TestingStuff _forTestingPurposes;

    internal TestingStuff ForTestingPurposesOnly()
    {
        if (_forTestingPurposes != null)
            return _forTestingPurposes;

        return _forTestingPurposes = new TestingStuff();
    }

    internal class TestingStuff
    {
        internal TimeSpan? CustomBackupHistoryRetentionConfiguration { get; set; }
        internal long BackupHistoryEntriesNumber(TransactionOperationContext context) => GetBackupHistoryEntriesNumber(context);
        internal long BackupResultDetailsNumber(TransactionOperationContext context) => GetBackupResultDetailsNumber(context);
    }
}
