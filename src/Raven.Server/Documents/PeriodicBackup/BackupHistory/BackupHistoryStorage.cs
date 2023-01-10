using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide.Commands;
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
    private readonly int _backupHistoryEntriesCountLimit;

    protected readonly Logger Logger;

    private static readonly Slice ByCreatedAt;
    private static readonly Slice ByCreatedAtFixedSizeIndex;

    private readonly TableSchema _entriesSchema = new();

    static BackupHistoryStorage()
    {
        using (StorageEnvironment.GetStaticContext(out var ctx))
        {
            Slice.From(ctx, nameof(ByCreatedAtFixedSizeIndex), ByteStringType.Immutable, out ByCreatedAtFixedSizeIndex);
            Slice.From(ctx, nameof(ByCreatedAt), ByteStringType.Immutable, out ByCreatedAt);
        }
    }

    public BackupHistoryStorage(int backupMaxNumberOfBackupHistoryEntries)
    {
        _backupHistoryEntriesCountLimit = backupMaxNumberOfBackupHistoryEntries;
        Logger = LoggingSource.Instance.GetLogger<BackupHistoryStorage>("Server");

        _entriesSchema.DefineKey(new TableSchema.SchemaIndexDef
        {
            StartIndex = BackupHistorySchema.BackupHistoryTable.CreatedAtIndex, 
            Count = 1
        }); 

        _entriesSchema.DefineIndex(new TableSchema.SchemaIndexDef
        {
            StartIndex = BackupHistorySchema.BackupHistoryTable.CreatedAtIndex, 
            Name = ByCreatedAt
        });

        _entriesSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef()
        {
            StartIndex = BackupHistorySchema.BackupHistoryTable.CreatedAtIndex,
            Name = ByCreatedAtFixedSizeIndex
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
        var backupHistoryEntry = new BackupHistoryEntry
        {
            BackupName = backupName,
            BackupType = periodicBackupStatus?.BackupType,
            DatabaseName = databaseName,
            DurationInMs = periodicBackupStatus?.DurationInMs,
            Error = periodicBackupStatus?.Error?.Exception,
            IsCompletedSuccessfully = task.IsCompletedSuccessfully,
            IsFull = periodicBackupStatus?.IsFull,
            NodeTag = periodicBackupStatus?.NodeTag
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
                var table = context.Transaction.InnerTransaction.OpenTable(_entriesSchema, BackupHistorySchema.BackupHistoryTree);
                var createdAtTicks = Bits.SwapBytes(backupHistoryEntry.CreatedAt.Ticks);

                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add((byte*)&createdAtTicks, sizeof(long));
                    tvb.Add(json.BasePointer, json.Size);

                    table.Set(tvb);
                }

                var extra = table.NumberOfEntries - _backupHistoryEntriesCountLimit;
                
                if (extra > 0)
                    table.DeleteBackwardFrom(_entriesSchema.FixedSizeIndexes[ByCreatedAtFixedSizeIndex], DateTime.MaxValue.Ticks, extra);

                tx.Commit();
            }
        }
    }

    public IDisposable ReadEntriesOrderedByCreationDate(out IEnumerable<BackupHistoryTableValue> entries)
    {
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(context.OpenReadTransaction());

            entries = ReadEntries(context);

            return scope.Delay();
        }
    }

    private IEnumerable<BackupHistoryTableValue> ReadEntries(TransactionOperationContext context)
    {
        var table = context.Transaction.InnerTransaction.OpenTable(_entriesSchema, BackupHistorySchema.BackupHistoryTree);

        foreach (var tvr in table.SeekForwardFrom(_entriesSchema.Indexes[ByCreatedAt], Slices.BeforeAllKeys,  0))
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
            Json = new BlittableJsonReaderObject(jsonPointer, size, context)
        };
    }

    public static class BackupHistorySchema
    {
        public const string BackupHistoryTree = nameof(BackupHistory);

        public static class BackupHistoryTable
        {
            public const int CreatedAtIndex = 0;
            public const int JsonIndex = 1;
        }
    }
}

public class BackupHistoryTableValue
{
    public DateTime CreatedAt;

    public BlittableJsonReaderObject Json;
}
