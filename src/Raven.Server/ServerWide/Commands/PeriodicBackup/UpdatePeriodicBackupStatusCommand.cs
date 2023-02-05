using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide.Commands;
using Raven.Server.Documents.PeriodicBackup.BackupHistory;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.PeriodicBackup
{
    public class UpdatePeriodicBackupStatusCommand : UpdateValueForDatabaseCommand
    {
        public PeriodicBackupStatus PeriodicBackupStatus;
        public List<BackupHistoryEntry> BackupHistoryEntries;
        
        // ReSharper disable once UnusedMember.Local
        private UpdatePeriodicBackupStatusCommand()
        {
            // for deserialization
        }

        public UpdatePeriodicBackupStatusCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
        }

        public override string GetItemId()
        {
            return PeriodicBackupStatus.GenerateItemName(DatabaseName, PeriodicBackupStatus.TaskId);
        }

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, RawDatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue)
        {
            return context.ReadObject(PeriodicBackupStatus.ToJson(), GetItemId());
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(BackupHistoryEntries)] = new DynamicJsonArray(BackupHistoryEntries);
            json[nameof(PeriodicBackupStatus)] = PeriodicBackupStatus.ToJson();
        }

        public List<BackupHistoryEntry> GetCommandEntries()
        {
            var entryFromBackupStatus = new BackupHistoryEntry
            {
                BackupType = PeriodicBackupStatus.BackupType,
                CreatedAt = PeriodicBackupStatus.IsFull ? PeriodicBackupStatus.LastFullBackup.Value : PeriodicBackupStatus.LastIncrementalBackup.Value,
                DatabaseName = DatabaseName,
                DurationInMs = PeriodicBackupStatus.DurationInMs,
                Error = PeriodicBackupStatus.Error?.Exception,
                IsFull = PeriodicBackupStatus.IsFull,
                NodeTag = PeriodicBackupStatus.NodeTag,
                LastFullBackup = PeriodicBackupStatus.LastFullBackup,
                TaskId = PeriodicBackupStatus.TaskId
            };

            return new List<BackupHistoryEntry>(BackupHistoryEntries) { entryFromBackupStatus };
        }
    }

    
}
