using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.Backups;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details;

public class BackupHistoryDetails : INotificationDetails
{
    public const int MaxNumberOfBackupNotifications = 50;

    public Queue<BackupHistoryDetailsEntry> BackupHistory { get; set; }

    public BackupHistoryDetails()
    {
        BackupHistory = new Queue<BackupHistoryDetailsEntry>();
    }
    
    public DynamicJsonValue ToJson()
    {
        var result = new DynamicJsonValue();
        
        var backupHistory = new DynamicJsonArray();
        foreach (var details in BackupHistory)
        {
            backupHistory.Add(new DynamicJsonValue
            {
                [nameof(BackupHistoryDetailsEntry.BackupName)] = details.BackupName, 
                [nameof(BackupHistoryDetailsEntry.BackupType)] = details.BackupType,
                [nameof(BackupHistoryDetailsEntry.DatabaseName)] = details.DatabaseName, 
                [nameof(BackupHistoryDetailsEntry.Date)] = details.Date, 
                [nameof(BackupHistoryDetailsEntry.DurationInMs)] = details.DurationInMs,
                [nameof(BackupHistoryDetailsEntry.Error)] = details.Error != null ? $"{details.Error.Exception} at {details.Error.At.ToLocalTime()}" : null,
                [nameof(BackupHistoryDetailsEntry.IsCompletedSuccessfully)] = details.IsCompletedSuccessfully,
                [nameof(BackupHistoryDetailsEntry.IsFull)] = details.IsFull,
                [nameof(BackupHistoryDetailsEntry.NodeTag)] = details.NodeTag,
            });
        }

        result[nameof(BackupHistory)] = backupHistory;
        
        return result;
    }

    public void Add(BackupHistoryDetailsEntry backupDetails)
    {
        BackupHistory.Enqueue(backupDetails);

        while (BackupHistory.Count > MaxNumberOfBackupNotifications)
        {
            BackupHistory.TryDequeue(out _);
        }
    }

    public class BackupHistoryDetailsEntry
    {
        public string BackupName { get; set; }
        public BackupType? BackupType { get; set; }
        public string DatabaseName { get; set; }
        public DateTime Date { get; set; }
        public long? DurationInMs { get; set; }
        public Error Error { get; set; }
        public bool IsCompletedSuccessfully { get; set; }
        public bool? IsFull { get; set; }
        public string NodeTag { get; set; }
    }
}
