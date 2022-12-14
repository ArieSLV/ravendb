using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications;

public class BackupHistoryNotification : Notification
{
    public BackupHistoryDetails Details { get; protected set; }
    public override string Id => nameof(BackupHistoryNotification);

    public BackupHistoryNotification(string database) : base(NotificationType.OperationChanged, database)
    {
    }

    public static BackupHistoryNotification Create(string database, string title, string msg, NotificationSeverity notificationSeverity, BackupHistoryDetails details)
    {
        return new BackupHistoryNotification(database)
        {
            IsPersistent = true,
            Title = title,
            Message = msg,
            Details = details
        };
    }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();
        json[nameof(Details)] = Details?.ToJson();
        return json;
    }
}
