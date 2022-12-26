using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.PeriodicBackup.BackupHistory;

public class BackupHistoryEntry
{
    public string BackupName { get; set; }
    public BackupType? BackupType { get; set; }
    public DateTime CreatedAt { get; private set; }
    public string DatabaseName { get; set; }
    public long? DurationInMs { get; set; }
    public Error Error { get; set; }
    public bool IsCompletedSuccessfully { get; set; }
    public bool? IsFull { get; set; }
    public string NodeTag { get; set; }
    public IReadOnlyList<string> Messages { get; set; }

    public BackupHistoryEntry()
    {
        CreatedAt = SystemTime.UtcNow;
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(BackupName)] = BackupName,
            [nameof(BackupType)] = BackupType,
            [nameof(CreatedAt)] = CreatedAt,
            [nameof(DatabaseName)] = DatabaseName,
            [nameof(DurationInMs)] = DurationInMs,
            [nameof(Error)] = Error != null ? $"{Error.Exception} at {Error.At.ToLocalTime()}" : null,
            [nameof(IsCompletedSuccessfully)] = IsCompletedSuccessfully,
            [nameof(IsFull)] = IsFull,
            [nameof(NodeTag)] = NodeTag,
            [nameof(Messages)] = Messages,
        };
    }
}
