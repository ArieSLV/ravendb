using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide.Commands;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using static Raven.Server.Documents.TransactionCommands.JsonPatchCommand;
using static Raven.Client.ServerWide.Commands.BackupHistory;
using Raven.Server.ServerWide.Commands;
using static Raven.Server.NotificationCenter.NotificationsStorage;
using static Raven.Server.Documents.PeriodicBackup.BackupHistory.BackupHistoryStorage;

namespace Raven.Server.Documents.PeriodicBackup.BackupHistory;

public unsafe class BackupHistoryStorage
{
    private readonly string _databaseName;
    private StorageEnvironment _environment;
    private TransactionContextPool _contextPool;

    protected readonly Logger Logger;
    
    private readonly TableSchema _backupHistorySchema = new();

    public BackupHistoryStorage(string databaseName)
    {
        _databaseName = databaseName;
        Logger = LoggingSource.Instance.GetLogger<BackupHistoryStorage>("Server");
        
        _backupHistorySchema.DefineKey(new TableSchema.SchemaIndexDef
        {
            StartIndex = BackupHistorySchema.BackupHistoryTable.ItemKey,
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
            _backupHistorySchema.Create(tx, BackupHistorySchema.BackupHistory, 16);
            tx.Commit();
        }
    }

    public void TemporaryStoreBackupHistoryEntries(UpdatePeriodicBackupStatusCommand command)
    {
        using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (var tx = context.OpenWriteTransaction())
        {
            foreach (var entry in command.GetCommandEntries())
            {
                var key = BackupHistoryTableValue.GenerateKey(entry);

                if (Logger.IsInfoEnabled)
                    Logger.Info($"Saving to {nameof(BackupHistoryStorage)} {nameof(BackupHistoryItemType.HistoryEntry)} with `{nameof(BackupHistoryTableValue.Key)}`: `{key}`.");

                StoreInternal(key, entry.ToJson(), tx, context);
            }

            tx.Commit();
        }
    }

    private void StoreInternal(string key, DynamicJsonValue value, RavenTransaction transaction = null, TransactionOperationContext context = null)
    {
        if (transaction == null && context == null)
        {
            using (_contextPool.AllocateOperationContext(out context))
            using (transaction = context.OpenWriteTransaction())
            {
                Store(transaction, context);
                transaction.Commit();
            }
        }
        else
        {
            Store(transaction, context);
        }

        void Store(RavenTransaction tx, TransactionOperationContext ctx)
        {
            var table = tx.InnerTransaction.OpenTable(_backupHistorySchema, BackupHistorySchema.BackupHistory);
            
            var lsKey = ctx.GetLazyString(key);
            using (var json = ctx.ReadObject(value, nameof(BackupHistorySchema.BackupHistory), BlittableJsonDocumentBuilder.UsageMode.ToDisk))
            using (table.Allocate(out TableValueBuilder tvb))
            {
                tvb.Add(lsKey.Buffer, lsKey.Size);
                tvb.Add(json.BasePointer, json.Size);

                table.Set(tvb);
            }
        }
    }

    public void StoreBackupDetails(BackupResult result, PeriodicBackupStatus status)
    {
        var key = BackupHistoryTableValue.GenerateKey(_databaseName, status, BackupHistoryItemType.Details);

        if (Logger.IsInfoEnabled)
            Logger.Info($"Saving to {nameof(BackupHistoryStorage)} {nameof(BackupHistoryItemType.Details)} with `{nameof(BackupHistoryTableValue.Key)}`: `{key}`.");

        StoreInternal(key, result.ToJson());
    }

    public List<BlittableJsonReaderObject> ReadTemporaryStoredBackupHistoryEntries(TransactionOperationContext context)
    {
        var prefix = $"values/{_databaseName}/backup-history/{BackupHistoryItemType.HistoryEntry}/";
        
        using (Slice.From(context.Allocator, prefix, out Slice slice))
        {
            var items = context.Transaction.InnerTransaction.OpenTable(_backupHistorySchema, BackupHistorySchema.BackupHistory);
            var list = new List<BlittableJsonReaderObject>();
            foreach (var result in items.SeekByPrimaryKeyPrefix(slice, Slices.Empty, 0))
            {
                var entry = GetCurrentHistoryEntryBlittable(context, result.Value);
                list.Add(entry);
            }

            return list;
        }
    }

    public List<(string, BlittableJsonReaderObject)> ReadBackupHistoryDetails(TransactionOperationContext context)
    {
        var prefix = $"values/{_databaseName}/backup-history/{BackupHistoryItemType.Details}/";

        using (Slice.From(context.Allocator, prefix, out Slice slice))
        {
            var items = context.Transaction.InnerTransaction.OpenTable(_backupHistorySchema, BackupHistorySchema.BackupHistory);
            var list = new List<(string, BlittableJsonReaderObject)>();
            foreach (var result in items.SeekByPrimaryKeyPrefix(slice, Slices.Empty, 0))
            {
                var entry = GetCurrentBackupHistoryDetailsBlittable(context, result.Value);
                list.Add(entry);
            }

            return list;
        }
    }

    public IDisposable ReadTemporaryStoredBackupHistoryItems(out List<BlittableJsonReaderObject> entries)
    {
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(context.OpenReadTransaction());

            entries = ReadTemporaryStoredBackupHistoryEntries(context);

            return scope.Delay();
        }
    }

    public IDisposable ReadBackupHistoryDetails(out List<(string, BlittableJsonReaderObject)> details)
    {
        using (var scope = new DisposableScope())
        {
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(context.OpenReadTransaction());

            details = ReadBackupHistoryDetails(context);

            return scope.Delay();
        }
    }

    private static BlittableJsonReaderObject GetCurrentHistoryEntryBlittable(TransactionOperationContext context, Table.TableValueHolder result)
    {
        var ptr = result.Reader.Read(1, out int size);
        var doc = new BlittableJsonReaderObject(ptr, size, context);

        return doc;
    }

    private static (string, BlittableJsonReaderObject) GetCurrentBackupHistoryDetailsBlittable(TransactionOperationContext context, Table.TableValueHolder result)
    {
        var ptr = result.Reader.Read(1, out int size);
        var doc = new BlittableJsonReaderObject(ptr, size, context);
        var key = Encoding.UTF8.GetString(result.Reader.Read(0, out size), size);

        return (key, doc);
    }



    public IEnumerable<BackupHistoryEntry> RetractTemporaryStoredBackupHistoryEntries()
    {
        var prefix = $"values/{_databaseName}/backup-history/{BackupHistoryItemType.HistoryEntry}/";

        using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (var tx = context.OpenWriteTransaction())
        using (Slice.From(context.Allocator, prefix, out Slice loweredPrefix))
        {
            var items = context.Transaction.InnerTransaction.OpenTable(_backupHistorySchema, BackupHistorySchema.BackupHistory);

            foreach (var result in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, 0))
            {
                var entry = GetCurrentHistoryEntry(context, result.Value);

                Delete(entry.Key, tx);
                // using (Slice.From(tx.InnerTransaction.Allocator, entry.Key, out Slice slice))
                // {
                //     items.DeleteByKey(slice);
                // }

                yield return entry.Entry;
            }
            
            tx.Commit();
        }
    }
    
    private static (string Key, BackupHistoryEntry Entry) GetCurrentHistoryEntry(TransactionOperationContext context, Table.TableValueHolder result)
    {
        var ptr = result.Reader.Read(1, out int size);
        var doc = new BlittableJsonReaderObject(ptr, size, context);
        var key = Encoding.UTF8.GetString(result.Reader.Read(0, out size), size);
        
        Transaction.DebugDisposeReaderAfterTransaction(context.Transaction.InnerTransaction, doc);
        return (key, JsonDeserializationClient.BackupHistoryEntry(doc));
    }

    public bool Delete(string id, RavenTransaction existingTransaction = null)
    {
        bool deleteResult;

        if (existingTransaction != null)
        {
            deleteResult = DeleteFromTable(existingTransaction);
        }
        else
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                deleteResult = DeleteFromTable(tx);
                tx.Commit();
            }
        }

        return deleteResult;

        bool DeleteFromTable(RavenTransaction tx)
        {
            var table = tx.InnerTransaction.OpenTable(_backupHistorySchema, BackupHistorySchema.BackupHistory);

            using (Slice.From(tx.InnerTransaction.Allocator, id, out Slice alertSlice))
            {
                return table.DeleteByKey(alertSlice);
            }
        }
    }

    public IDisposable ReadBackupHistoryItem(string id, out BackupHistoryTableValue entry)
    {
        using (var scope = new DisposableScope())
        {
            RavenTransaction tx;
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(tx = context.OpenReadTransaction());
    
            entry = GetBackupHistoryItem(id, context, tx);
    
            return scope.Delay();
        }
    }
    
    private BackupHistoryTableValue GetBackupHistoryItem(string id, TransactionOperationContext context, RavenTransaction tx)
    {
        var table = tx.InnerTransaction.OpenTable(_backupHistorySchema, BackupHistorySchema.BackupHistory);
        using (Slice.From(tx.InnerTransaction.Allocator, id, out Slice slice))
        {
            if (table.ReadByKey(slice, out TableValueReader tvr) == false)
                return null;

            var key = tvr.ReadString(BackupHistorySchema.BackupHistoryTable.ItemKey);
            var jsonPointer = tvr.Read(BackupHistorySchema.BackupHistoryTable.ItemJsonIndex, out var size);
            var json = new BlittableJsonReaderObject(jsonPointer, size, context);

            return new BackupHistoryTableValue(key, json);
        }
    }

    public IDisposable ReadBackupHistoryDetails(string key, out BlittableJsonReaderObject details)
    {
        using (var scope = new DisposableScope())
        {
            RavenTransaction tx;
            scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
            scope.EnsureDispose(tx = context.OpenReadTransaction());

            details = GetBackupHistoryDetails(key, context, tx);

            return scope.Delay();
        }
    }

    private BlittableJsonReaderObject GetBackupHistoryDetails(string key, TransactionOperationContext context, RavenTransaction tx)
    {
        var table = tx.InnerTransaction.OpenTable(_backupHistorySchema, BackupHistorySchema.BackupHistory);
        using (Slice.From(tx.InnerTransaction.Allocator, key, out Slice slice))
        {
            if (table.ReadByKey(slice, out TableValueReader tvr) == false)
                return null;

            var jsonPointer = tvr.Read(BackupHistorySchema.BackupHistoryTable.ItemJsonIndex, out var size);
            var json = new BlittableJsonReaderObject(jsonPointer, size, context);

            return json;
        }
    }

    public static class BackupHistorySchema
    {
        public const string BackupHistory = nameof(BackupHistoryTable);

        public static class BackupHistoryTable
        {
            public const int ItemKey = 0;
            public const int ItemJsonIndex = 1;
        }
    }

    public void HandleDatabaseValueChanged(string type, object changeState)
    {
        if (type != nameof(UpdatePeriodicBackupStatusCommand) || changeState == null)
            return;

        var list = (List<string>)changeState;

        foreach (var id in list)
        {
            var a = Delete(id);
        }
    }
}

public class BackupHistoryTableValue : IDynamicJsonValueConvertible
{
    public string Key;
    public BlittableJsonReaderObject Value;

    public BackupHistoryTableValue()
    {
        
    }

    public BackupHistoryTableValue(string key, BlittableJsonReaderObject value)
    {
        Key = key;
        Value = value;
    }

    public static string GenerateKey(BackupHistoryEntry entry)
    {
        return GenerateKey(entry.DatabaseName, entry.TaskId, entry.NodeTag, entry.CreatedAt, BackupHistoryItemType.HistoryEntry);
    }

    public static string GenerateKey(string databaseName, PeriodicBackupStatus status, BackupHistoryItemType type)
    {
        var createdAt = status.IsFull 
            ? status.LastFullBackup ?? status.Error.At
            : status.LastIncrementalBackup ?? status.Error.At;

        return GenerateKey(databaseName, status.TaskId, status.NodeTag, createdAt, type);
    }

    public static string GenerateKey(string databaseName, long taskId, string nodeTag, DateTime createdAt, BackupHistoryItemType type)
    {
        return $"values/{databaseName}/backup-history/{type}/{taskId}/{nodeTag}/{createdAt.Ticks}";
    }

    public static List<string> GenerateEntriesKeys(BlittableJsonReaderObject backupEntry, string databaseName, BackupHistoryItemType type)
    {
        var list = new List<string>();
        backupEntry.TryGet(nameof(BackupHistoryItem.FullBackup), out BlittableJsonReaderObject fullBackup);
        
        fullBackup.TryGet(nameof(BackupHistoryEntry.TaskId), out long taskId);
        fullBackup.TryGet(nameof(BackupHistoryEntry.NodeTag), out string nodeTag);
        fullBackup.TryGet(nameof(BackupHistoryEntry.CreatedAt), out DateTime fullBackupCreatedAt);
        list.Add(GenerateKey(databaseName, taskId, nodeTag, fullBackupCreatedAt, type));

        backupEntry.TryGet(nameof(BackupHistoryItem.IncrementalBackups), out BlittableJsonReaderArray increments);
        foreach (BlittableJsonReaderObject increment in increments)
        {
            increment.TryGet(nameof(BackupHistoryEntry.CreatedAt), out DateTime incrementCreatedAt);

            list.Add(GenerateKey(databaseName, taskId, nodeTag, incrementCreatedAt, type));
        }

        return list;
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Key)] = Key,
            [nameof(Value)] = Value
        };
    }
}

public enum BackupHistoryItemType
{
    Details,
    HistoryEntry
}
