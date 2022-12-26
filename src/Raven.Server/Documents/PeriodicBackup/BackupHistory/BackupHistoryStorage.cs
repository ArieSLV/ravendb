using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Util;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.PeriodicBackup.BackupHistory;

public unsafe class BackupHistoryStorage
{
    private StorageEnvironment _environment;
    private TransactionContextPool _contextPool;

    protected readonly Logger Logger;

    private static readonly Slice ByCreatedAt;

    internal readonly TableSchema _entriesSchema = new();
    private const long BackupHistoryEntriesCountLimit = 50;

    static BackupHistoryStorage()
    {
        using (StorageEnvironment.GetStaticContext(out var ctx))
        {
            Slice.From(ctx, nameof(ByCreatedAt), ByteStringType.Immutable, out ByCreatedAt);
        }
    }

    public BackupHistoryStorage()
    {
        Logger = LoggingSource.Instance.GetLogger<BackupHistoryStorage>("Server");

        _entriesSchema.DefineKey(new TableSchema.SchemaIndexDef
        {
            StartIndex = BackupHistorySchema.BackupHistoryTable.IdIndex, 
            Count = 1
        });

        _entriesSchema.DefineIndex(new TableSchema.SchemaIndexDef
        {
            StartIndex = BackupHistorySchema.BackupHistoryTable.CreatedAtIndex, 
            Name = ByCreatedAt
        });
    }

    public void Initialize(StorageEnvironment environment, TransactionContextPool contextPool)
    {
        _environment = environment;
        _contextPool = contextPool;

        using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (var tx = _environment.WriteTransaction(context.PersistentContext))
        {
            _entriesSchema.Create(tx, BackupHistorySchema.BackupHistoryTree, 16);
            tx.Commit();
        }
    }

    public void Store(string backupName, string databaseName, Task<IOperationResult> task, PeriodicBackupStatus periodicBackupStatus)
    {
        if (task.Result is not BackupResult result)
            return;

        var backupHistoryEntry = new BackupHistoryEntry
        {
            BackupName = backupName,
            BackupType = periodicBackupStatus?.BackupType,
            DatabaseName = databaseName,
            DurationInMs = periodicBackupStatus?.DurationInMs,
            Error = periodicBackupStatus?.Error,
            IsCompletedSuccessfully = task.IsCompletedSuccessfully,
            IsFull = periodicBackupStatus?.IsFull,
            NodeTag = periodicBackupStatus?.NodeTag,
            Messages = result.Messages,
        };

        Store(backupHistoryEntry);
    }

    public void Store(BackupHistoryEntry backupHistoryEntry)
    {
        using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"Saving information about backup of `{backupHistoryEntry.DatabaseName}` ({backupHistoryEntry.BackupName}) to backup history.");

            using (var json = context.ReadObject(backupHistoryEntry.ToJson(), nameof(BackupHistoryEntry), BlittableJsonDocumentBuilder.UsageMode.ToDisk))
            using (var tx = context.OpenWriteTransaction())
            {
                StoreInternal(backupHistoryEntry.CreatedAt, json, tx);
                tx.Commit();
            }

            // ComplyWithLimit(context);
        }
    }


    private void StoreInternal(DateTime createdAt, BlittableJsonReaderObject json, RavenTransaction tx)
    {
        var table = tx.InnerTransaction.OpenTable(_entriesSchema, BackupHistorySchema.BackupHistoryTree);

        var createdAtTicks = Bits.SwapBytes(createdAt.Ticks);

        using (table.Allocate(out TableValueBuilder tvb))
        {
            tvb.Add((byte*)&createdAtTicks, sizeof(long));
            tvb.Add(json.BasePointer, json.Size);

            table.Set(tvb);
        }
    }

    public IDisposable ReadEntriesOrderedByCreationDate(out IEnumerable<BackupHistoryTableValue> entries)
    {
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(context.OpenReadTransaction());

            entries = ReadEntriesByCreatedAtIndex(context);

            return scope.Delay();
        }
    }

    private IEnumerable<BackupHistoryTableValue> ReadEntriesByCreatedAtIndex(TransactionOperationContext context)
    {

        var table = context.Transaction.InnerTransaction.OpenTable(_entriesSchema, BackupHistorySchema.BackupHistoryTree);

        foreach (var tvr in table.SeekForwardFrom(_entriesSchema.Indexes[ByCreatedAt], Slices.BeforeAllKeys, 0))
        {
            yield return Read(context, ref tvr.Result.Reader);
        }

    }

    private BackupHistoryTableValue Read(TransactionOperationContext context, ref TableValueReader resultReader)
    {
        var createdAt = new DateTime(Bits.SwapBytes(*(long*)resultReader.Read(BackupHistorySchema.BackupHistoryTable.CreatedAtIndex, out int size)));
        var jsonPointer = resultReader.Read(BackupHistorySchema.BackupHistoryTable.JsonIndex, out size);

        return new BackupHistoryTableValue
        {
            CreatedAt = createdAt,
            Json = new BlittableJsonReaderObject(jsonPointer,size, context)
        };
    }

    private long GetBackupHistoryEntriesCount(TransactionOperationContext context)
    {
        var table = context.Transaction.InnerTransaction.OpenTable(_entriesSchema, BackupHistorySchema.BackupHistoryTree);
        
        return table?.GetNumberOfEntriesFor(_entriesSchema.FixedSizeIndexes[ByCreatedAt]) ?? 0;
    }

    private void ComplyWithLimit(TransactionOperationContext context)
    {
        using (context.OpenReadTransaction())
        {
            var extra = GetBackupHistoryEntriesCount(context) - BackupHistoryEntriesCountLimit;
            if (extra <= 0) 
                return;

            var table = context.Transaction.InnerTransaction.OpenTable(_entriesSchema, BackupHistorySchema.BackupHistoryTree);
            if (table == null) 
                return;

            table.DeleteBackwardFrom(_entriesSchema.FixedSizeIndexes[ByCreatedAt], long.MaxValue, extra);
        }
    }

    public static class BackupHistorySchema
    {
        public const string BackupHistoryTree = nameof(BackupHistory);

        public static class BackupHistoryTable
        {
            public const int IdIndex = 0;
            public const int CreatedAtIndex = 1;
            public const int JsonIndex = 2;
        }
    }
}

public class BackupHistoryTableValue
{
    public BlittableJsonReaderObject Json;

    public DateTime CreatedAt;
}
