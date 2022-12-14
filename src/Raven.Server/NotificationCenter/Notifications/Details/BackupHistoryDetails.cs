using System;
using System.Collections.Generic;
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
                [nameof(BackupHistoryDetailsEntry.BackupTaskId)] = details.BackupTaskId, 
                [nameof(BackupHistoryDetailsEntry.Date)] = details.Date, 
                [nameof(BackupHistoryDetailsEntry.DurationInMs)] = details.DurationInMs
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
        public long BackupTaskId { get; set; }
        public DateTime Date { get; set; }
        public long DurationInMs { get; set; }
    }
}
