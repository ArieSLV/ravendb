using System;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Util;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.NotificationCenter;

public class BackupHistory : IDisposable
{
    private readonly NotificationCenter _notificationCenter;
    private readonly NotificationsStorage _notificationsStorage;
    private readonly string _database;
    private readonly Logger _logger;

    public BackupHistory(NotificationCenter notificationCenter, NotificationsStorage notificationsStorage, string database)
    {
        _notificationCenter = notificationCenter;
        _notificationsStorage = notificationsStorage;
        _database = database;
        _logger = LoggingSource.Instance.GetLogger(database, GetType().FullName);
    }

    public void Add(PeriodicBackupStatus status)
    {
        var notification = GetBackupHistoryNotification(nameof(BackupHistoryNotification));
        var backup = new BackupHistoryDetails.BackupHistoryDetailsEntry
        {
            BackupTaskId = status.TaskId,
            Date = SystemTime.UtcNow,
            DurationInMs = status.DurationInMs ?? throw new NullReferenceException(nameof(status.DurationInMs))
        };
        notification.Details.Add(backup);
        _notificationCenter.Add(notification);
    }

    private BackupHistoryNotification GetBackupHistoryNotification(string id)
    {
        using (_notificationsStorage.Read(id, out NotificationTableValue ntv))
        {
            BackupHistoryDetails details;
            if (ntv == null || ntv.Json.TryGet(nameof(BackupHistoryNotification.Details), out BlittableJsonReaderObject detailsJson) == false || detailsJson == null)
            {
                details = new BackupHistoryDetails();
            }
            else
            {
                details = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<BackupHistoryDetails>(detailsJson, id);
            }

            return BackupHistoryNotification.Create(_database, "Backup history", "New entries in the backup history appeared", NotificationSeverity.Success, details);
        }
    }

    public void Dispose()
    {
        throw new NotImplementedException();
    }
}
