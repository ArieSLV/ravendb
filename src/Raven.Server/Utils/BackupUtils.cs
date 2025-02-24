using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;
using NCrontab.Advanced;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.DirectUpload;
using Raven.Server.NotificationCenter;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Server.Utils;
using Sparrow.Utils;
using BackupConfiguration = Raven.Client.Documents.Operations.Backups.BackupConfiguration;

namespace Raven.Server.Utils;

internal static class BackupUtils
{
    internal static bool IgnoreHealthChecksBeforeBackup = false;

    internal static BackupTask GetBackupTask(DocumentDatabase database, BackupParameters backupParameters, BackupConfiguration configuration, OperationCancelToken token, Logger logger, PeriodicBackupRunner.TestingStuff forTestingPurposes = null)
    {
        return configuration.BackupUploadMode == BackupUploadMode.DirectUpload
            ? new DirectUploadBackupTask(database, backupParameters, configuration, token, logger, forTestingPurposes) 
            : new BackupTask(database, backupParameters, configuration, token, logger, forTestingPurposes);
    }

    internal static async Task<Stream> GetDecompressionStreamAsync(Stream stream, CancellationToken token = default)
    {
        var buffer = new byte[4];

        var read = await stream.ReadAsync(buffer, 0, buffer.Length, token).ConfigureAwait(false);
        if (read == 0)
            throw new InvalidOperationException("Empty stream");

        var backupStream = new BackupStream(stream, buffer);

        if (read != buffer.Length)
            return backupStream;

        const uint zstdMagicNumber = 0xFD2FB528;
        var uintMagicNumber = BitConverter.ToUInt32(buffer);
        if (uintMagicNumber == zstdMagicNumber)
            return ZstdStream.Decompress(backupStream);

        if (buffer[0] == 0x1F && buffer[1] == 0x8B)
            return new GZipStream(backupStream, CompressionMode.Decompress);

        return backupStream;
    }

    internal static Stream GetCompressionStream(Stream stream, ExportCompressionAlgorithm compressionAlgorithm, CompressionLevel compressionLevel)
    {
        switch (compressionAlgorithm)
        {
            case ExportCompressionAlgorithm.Gzip:
                return new GZipStream(stream, compressionLevel, leaveOpen: true);
            case ExportCompressionAlgorithm.Zstd:
                if (compressionLevel == CompressionLevel.NoCompression)
                    return new LeaveOpenStream(stream);

                return ZstdStream.Compress(stream, compressionLevel, leaveOpen: true);
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    internal static BackupInfo GetBackupInfo(BackupInfoParameters parameters)
    {
        var oneTimeBackupStatus = GetBackupStatusFromCluster(parameters.ServerStore, parameters.Context, parameters.DatabaseName, taskId: 0L);

        if (parameters.PeriodicBackups.Count == 0 && oneTimeBackupStatus == null)
            return null;

        var lastBackup = 0L;
        PeriodicBackupStatus lastBackupStatus = null;
        var intervalUntilNextBackupInSec = long.MaxValue;
        if (oneTimeBackupStatus?.LastFullBackup != null && oneTimeBackupStatus.LastFullBackup.Value.Ticks > lastBackup)
        {
            lastBackup = oneTimeBackupStatus.LastFullBackup.Value.Ticks;
            lastBackupStatus = oneTimeBackupStatus;
        }

        foreach (var periodicBackup in parameters.PeriodicBackups)
        {
            var status = ComparePeriodicBackupStatus(periodicBackup.Configuration.TaskId,
                backupStatus: GetBackupStatusFromCluster(parameters.ServerStore, parameters.Context, parameters.DatabaseName, periodicBackup.Configuration.TaskId),
                inMemoryBackupStatus: periodicBackup.BackupStatus);

            if (status.LastFullBackup != null && status.LastFullBackup.Value.Ticks > lastBackup)
            {
                lastBackup = status.LastFullBackup.Value.Ticks;
                lastBackupStatus = status;
            }

            if (status.LastIncrementalBackup != null && status.LastIncrementalBackup.Value.Ticks > lastBackup)
            {
                lastBackup = status.LastIncrementalBackup.Value.Ticks;
                lastBackupStatus = status;
            }

            var nextBackup = GetNextBackupDetails(new NextBackupDetailsParameters
            {
                // OnParsingError and OnMissingNextBackupInfo are null's - for skipping error messages notification and log
                Configuration = periodicBackup.Configuration,
                BackupStatus = status,
                ResponsibleNodeTag = parameters.ServerStore.NodeTag,
                NodeTag = parameters.ServerStore.NodeTag
            });
            if (nextBackup == null)
                continue;

            if (nextBackup.TimeSpan.Ticks < intervalUntilNextBackupInSec)
                intervalUntilNextBackupInSec = nextBackup.TimeSpan.Ticks;
        }

        return new BackupInfo
        {
            LastBackup = lastBackup == 0L ? null : new DateTime(lastBackup),
            IntervalUntilNextBackupInSec = intervalUntilNextBackupInSec == long.MaxValue ? 0 : new TimeSpan(intervalUntilNextBackupInSec).TotalSeconds,
            BackupTaskType = lastBackupStatus?.TaskId == 0 ? BackupTaskType.OneTime : BackupTaskType.Periodic,
            Destinations = AddDestinations(lastBackupStatus)
        };
    }

    internal static PeriodicBackupStatus GetBackupStatusFromCluster(ServerStore serverStore, ClusterOperationContext context, string databaseName, long taskId)
    {
        var statusBlittable = serverStore.Cluster.Read(context, PeriodicBackupStatus.GenerateItemName(databaseName, taskId));

        if (statusBlittable == null)
            return null;

        var periodicBackupStatusJson = JsonDeserializationClient.PeriodicBackupStatus(statusBlittable);
        return periodicBackupStatusJson;
    }

    internal static BlittableJsonReaderObject GetResponsibleNodeInfoFromCluster(ServerStore serverStore, ClusterOperationContext context, string databaseName, long taskId)
    {
        var responsibleNodeBlittable = serverStore.Cluster.Read(context, ResponsibleNodeInfo.GenerateItemName(databaseName, taskId));
        return responsibleNodeBlittable;
    }

    internal static long GetTasksCountOnNode(ServerStore serverStore, string databaseName, ClusterOperationContext context)
    {
        var count = 0L;

        var prefix = ResponsibleNodeInfo.GetPrefix(databaseName);
        foreach (var keyValue in ClusterStateMachine.ReadValuesStartingWith(context, prefix))
        {
            if (keyValue.Value.TryGet(nameof(ResponsibleNodeInfo.ResponsibleNode), out string currentResponsibleNode) == false)
                continue;

            if (currentResponsibleNode != serverStore.NodeTag)
                continue;

            count++;
        }

        return count;
    }

    internal static string GetResponsibleNodeTag(ServerStore serverStore, string databaseName, long taskId)
    {
        using (serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        {
            var blittable = GetResponsibleNodeInfoFromCluster(serverStore, context, databaseName, taskId);
            if (blittable == null)
                return null;

            blittable.TryGet(nameof(ResponsibleNodeInfo.ResponsibleNode), out string responsibleNodeTag);
            return responsibleNodeTag;
        }
    }

    internal static async ValueTask<string> WaitAndGetResponsibleNodeAsync(long taskId, DocumentDatabase database)
    {
        // task id == raft index
        // we must wait here to ensure that the task was actually created on this node
        await database.ServerStore.Cluster.WaitForIndexNotification(taskId);

        var nodeTag = database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);
        if (nodeTag == null)
        {
            // this can happen if the database was just created or if a new task that was just created
            // we'll wait for the cluster observer to give more time for the database stats to become stable,
            // and then we'll wait for the cluster observer to determine the responsible node for the backup

            var task = Task.Delay(database.Configuration.Cluster.StabilizationTime.AsTimeSpan + database.Configuration.Cluster.StabilizationTime.AsTimeSpan);

            while (true)
            {
                if (Task.WaitAny(new[] { task }, millisecondsTimeout: 100) == 0)
                {
                    throw new InvalidOperationException($"Couldn't find a node which is responsible for backup task id: {taskId}");
                }

                nodeTag = database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);
                if (nodeTag != null)
                    break;
            }
        }

        return nodeTag;
    }

    internal static PeriodicBackupStatus ComparePeriodicBackupStatus(long taskId, PeriodicBackupStatus backupStatus, PeriodicBackupStatus inMemoryBackupStatus)
    {
        if (backupStatus == null)
        {
            backupStatus = inMemoryBackupStatus ?? new PeriodicBackupStatus { TaskId = taskId };
        }
        else if (inMemoryBackupStatus?.Version > backupStatus.Version && inMemoryBackupStatus.NodeTag == backupStatus.NodeTag)
        {
            // the in memory backup status is more updated
            // and is of the same node (current one)
            backupStatus = inMemoryBackupStatus;
        }

        return backupStatus;
    }

    private static List<string> AddDestinations(PeriodicBackupStatus backupStatus)
    {
        if (backupStatus == null)
            return null;

        var destinations = new List<string>();
        if (backupStatus.UploadToAzure?.Skipped == false)
            destinations.Add(nameof(BackupConfiguration.BackupDestination.Azure));
        if (backupStatus.UploadToGlacier?.Skipped == false)
            destinations.Add(nameof(BackupConfiguration.BackupDestination.AmazonGlacier));
        if (backupStatus.UploadToFtp?.Skipped == false)
            destinations.Add(nameof(BackupConfiguration.BackupDestination.FTP));
        if (backupStatus.UploadToGoogleCloud?.Skipped == false)
            destinations.Add(nameof(BackupConfiguration.BackupDestination.GoogleCloud));
        if (backupStatus.UploadToS3?.Skipped == false)
            destinations.Add(nameof(BackupConfiguration.BackupDestination.AmazonS3));
        if (backupStatus.LocalBackup?.TempFolderUsed == false)
            destinations.Add(nameof(BackupConfiguration.BackupDestination.Local));
        if (destinations.Count == 0)
            destinations.Add(nameof(BackupConfiguration.BackupDestination.None));

        return destinations;
    }

    public static NextBackup GetNextBackupDetails(NextBackupDetailsParameters parameters)
    {
        var baseLine = DateTime.UtcNow;
        Console.WriteLine($"{DateTime.Now:O}: GetNextBackupDetails -> baseLine: {baseLine:O}");

        var lastFullBackupUtc = parameters.BackupStatus?.LastFullBackupInternal ?? parameters.DatabaseWakeUpTimeUtc ?? parameters.Configuration.CreatedAt ?? baseLine;
        var lastIncrementalBackupUtc = parameters.BackupStatus?.LastIncrementalBackupInternal ?? parameters.BackupStatus?.LastFullBackupInternal ?? parameters.DatabaseWakeUpTimeUtc ?? baseLine;
        var nextFullBackup = GetNextBackupOccurrence(new NextBackupOccurrenceParameters
        {
            BackupFrequency = parameters.Configuration.FullBackupFrequency,
            Configuration = parameters.Configuration,
            LastBackupUtc = lastFullBackupUtc,
            OnParsingError = parameters.OnParsingError,
        });
        var nextIncrementalBackup = GetNextBackupOccurrence(new NextBackupOccurrenceParameters
        {
            BackupFrequency = parameters.Configuration.IncrementalBackupFrequency,
            Configuration = parameters.Configuration,
            LastBackupUtc = lastIncrementalBackupUtc,
            OnParsingError = parameters.OnParsingError
        });

        if (nextFullBackup == null && nextIncrementalBackup == null)
        {
            parameters.OnMissingNextBackupInfo?.Invoke(parameters.Configuration);
            return null;
        }

        Debug.Assert(parameters.Configuration.TaskId != 0);

        var isFullBackup = IsFullBackup(parameters.BackupStatus, parameters.Configuration, nextFullBackup, nextIncrementalBackup, parameters.ResponsibleNodeTag);
        Console.WriteLine($"{DateTime.Now:O}: GetNextBackupDetails -> isFullBackup: {isFullBackup}, nextFullBackup: {(nextFullBackup.HasValue ? $"{nextFullBackup.Value:O}" : "null")}, nextIncrementalBackup: {(nextIncrementalBackup.HasValue ? $"{nextIncrementalBackup.Value:O}" : "null")}");

        var nextBackupTimeLocal = GetNextBackupDateTime(nextFullBackup, nextIncrementalBackup, parameters.BackupStatus?.DelayUntil);
        Console.WriteLine($"{DateTime.Now:O}: GetNextBackupDetails -> nextBackupTimeLocal: {nextBackupTimeLocal:O}");

        var nextBackupTimeUtc = nextBackupTimeLocal.ToUniversalTime();

        var timeSpan = nextBackupTimeUtc - baseLine;
        Console.WriteLine($"{DateTime.Now:O}: GetNextBackupDetails -> timeSpan: {timeSpan}");

        TimeSpan nextBackupTimeSpan;
        if (timeSpan.Ticks <= 0 && parameters.BackupStatus != null)
        {
            // overdue backup of current node or first backup
            if (parameters.BackupStatus.NodeTag == parameters.NodeTag || parameters.BackupStatus.NodeTag == null)
            {
                // the backup will run now
                nextBackupTimeSpan = TimeSpan.Zero;
                nextBackupTimeUtc = baseLine;

                Console.WriteLine($"{DateTime.Now:O}: GetNextBackupDetails -> overdue backup of current node or first backup -> nextBackupTimeSpan: {nextBackupTimeSpan}, nextBackupTimeUtc: {nextBackupTimeUtc:O}");
            }
            else
            {
                // overdue backup from other node
                nextBackupTimeSpan = TimeSpan.FromMinutes(1);
                nextBackupTimeUtc = baseLine + nextBackupTimeSpan;
                Console.WriteLine($"{DateTime.Now:O}: GetNextBackupDetails -> overdue backup from other node -> nextBackupTimeSpan: {nextBackupTimeSpan}, nextBackupTimeUtc: {nextBackupTimeUtc:O}");
            }
        }
        else
        {
            nextBackupTimeSpan = timeSpan;
            Console.WriteLine($"{DateTime.Now:O}: GetNextBackupDetails -> nextBackupTimeSpan: {nextBackupTimeSpan}");
        }
        nextBackupTimeUtc = DateTime.SpecifyKind(nextBackupTimeUtc, DateTimeKind.Utc);

        Console.WriteLine($"{DateTime.Now:O}: GetNextBackupDetails -> nextBackupTimeUtc: {nextBackupTimeUtc:O}");
        return new NextBackup
        {
            TimeSpan = nextBackupTimeSpan,
            DateTime = nextBackupTimeUtc,
            OriginalBackupTime = parameters.BackupStatus?.OriginalBackupTime,
            IsFull = isFullBackup,
            TaskId = parameters.Configuration.TaskId
        };
    }

    internal static DateTime? GetNextBackupOccurrence(NextBackupOccurrenceParameters parameters)
    {
        if (string.IsNullOrWhiteSpace(parameters.BackupFrequency))
            return null;

        try
        {
            var backupParser = CrontabSchedule.Parse(parameters.BackupFrequency);
            return backupParser.GetNextOccurrence(parameters.LastBackupUtc.ToLocalTime());
        }
        catch (Exception e)
        {
            parameters.OnParsingError?.Invoke(new OnParsingErrorParameters
            {
                Exception = e,
                BackupFrequency = parameters.BackupFrequency,
                Configuration = parameters.Configuration
            });

            return null;
        }
    }

    public static DateTime GetNextBackupDateTime(DateTime? nextFullBackup, DateTime? nextIncrementalBackup, DateTime? delayUntil)
    {
        Debug.Assert(nextFullBackup != null || nextIncrementalBackup != null);
        DateTime? nextBackup;

        if (nextFullBackup == null)
            nextBackup = nextIncrementalBackup;
        else if (nextIncrementalBackup == null)
            nextBackup = nextFullBackup;
        else
            nextBackup = nextFullBackup <= nextIncrementalBackup ? nextFullBackup.Value : nextIncrementalBackup.Value;

        return delayUntil.HasValue && delayUntil.Value.ToLocalTime() > nextBackup.Value
            ? delayUntil.Value.ToLocalTime() : nextBackup.Value;
    }

    private static bool IsFullBackup(PeriodicBackupStatus backupStatus,
        PeriodicBackupConfiguration configuration,
        DateTime? nextFullBackup, DateTime? nextIncrementalBackup, string responsibleNodeTag)
    {
        if (backupStatus?.LastFullBackup == null ||
            backupStatus.NodeTag != responsibleNodeTag ||
            backupStatus.BackupType != configuration.BackupType ||
            backupStatus.LastEtag == null)
        {
            // Reasons to start a new full backup:
            // 1. there is no previous full backup, we are going to create one now
            // 2. the node which is responsible for the backup was replaced
            // 3. the backup type changed (e.g. from backup to snapshot)
            // 4. last etag wasn't updated

            return true;
        }

        // 1. there is a full backup setup but the next incremental backup wasn't setup
        // 2. there is a full backup setup and the next full backup is before the incremental one
        return nextFullBackup != null &&
               (nextIncrementalBackup == null || nextFullBackup <= nextIncrementalBackup);
    }

    internal static void CheckServerHealthBeforeBackup(ServerStore serverStore, string name)
    {
        if (IgnoreHealthChecksBeforeBackup)
            return;

        if (serverStore.Server.CpuCreditsBalance.BackgroundTasksAlertRaised.IsRaised())
        {
            throw new BackupDelayException($"Failed to start Backup Task: '{name}'. The task cannot run because the CPU credits allocated to this machine are nearing exhaustion.")
            {
                DelayPeriod = serverStore.Configuration.Server.CpuCreditsExhaustionBackupDelay.AsTimeSpan
            };
        }

        if (LowMemoryNotification.Instance.LowMemoryState)
        {
            throw new BackupDelayException($"Failed to start Backup Task: '{name}'. The task cannot run because the server is in low memory state.")
            {
                DelayPeriod = serverStore.Configuration.Backup.LowMemoryBackupDelay.AsTimeSpan
            };
        }

        if (LowMemoryNotification.Instance.DirtyMemoryState.IsHighDirty)
        {
            throw new BackupDelayException($"Failed to start Backup Task: '{name}'. The task cannot run because the server is in high dirty memory state.")
            {
                DelayPeriod = serverStore.Configuration.Backup.LowMemoryBackupDelay.AsTimeSpan
            };
        }
    }

    public static bool CanServerRunBackup(ServerStore serverStore)
    {
        try
        {
            CheckServerHealthBeforeBackup(serverStore, name: null);
            return true;
        }
        catch (BackupDelayException)
        {
            return false;
        }
    }

    public static PathSetting GetBackupTempPath(RavenConfiguration configuration, string dir, out PathSetting basePath)
    {
        basePath = configuration.Backup.TempPath ?? configuration.Storage.TempPath ?? configuration.Core.DataDirectory;
        return basePath.Combine(dir);
    }

    public static IdleDatabaseActivity GetEarliestIdleDatabaseActivity(EarliestIdleDatabaseActivityParameters parameters)
    {
        IdleDatabaseActivity earliestAction = null;
        using (parameters.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        {
            var rawDatabaseRecord = parameters.ServerStore.Cluster.ReadRawDatabaseRecord(context, parameters.DatabaseName);

            foreach (var backup in rawDatabaseRecord.PeriodicBackups)
            {
                var nextAction = GetNextIdleDatabaseActivity(new NextIdleDatabaseActivityParameters(parameters, backup, context));

                if (nextAction == null)
                    continue;

                earliestAction =
                    earliestAction == null || nextAction.DateTime < earliestAction.DateTime
                        ? nextAction
                        : earliestAction;
            }
        }
        Console.WriteLine($"{DateTime.Now:O}: GetEarliestIdleDatabaseActivity for {parameters.DatabaseName} -> nextAction: {earliestAction?.Type}, {earliestAction?.DateTime:O}");
        return earliestAction;
    }

    private static IdleDatabaseActivity GetNextIdleDatabaseActivity(NextIdleDatabaseActivityParameters parameters)
    {
        // General rules:
        // We want to return `IdleDatabaseActivityType.WakeUpDatabase` if:
        // 1. Next backup is Full
        // 2. Next backup is Incremental and there are changes since the last backup
        // 3. Next backup is Incremental and there are no changes since the last backup, but the next Full backup is overdue
        // 4. Database is not idle, and we don't have a backup yet

        // We want to return `IdleDatabaseActivityType.UpdateBackupStatusOnly` if:
        // 1. Next backup is Incremental and there are no changes since the last backup, and the next Full backup is not overdue

        // We want to return `null` if:
        // 1. Periodic Backup is disabled
        // 2. Both FullBackupFrequency and IncrementalBackupFrequency are null
        // 3. There are no destinations in the configuration
        // 4. Cluster is down
        // 5. Responsible node is not the current node
        // 6. Backup task have no next backup. Should not happen and likely a bug
        Console.WriteLine();

        if (parameters.Configuration.Disabled ||
            parameters.Configuration.IncrementalBackupFrequency == null && parameters.Configuration.FullBackupFrequency == null ||
            parameters.Configuration.HasBackup() == false)
            return null;

        var backupStatus = GetBackupStatusFromCluster(parameters.ServerStore, parameters.Context, parameters.DatabaseName, parameters.Configuration.TaskId);

        if (backupStatus == null && parameters.IsIdle == false)
        {
            // we might reach here from periodic backup runner that check whether due time is far enough in the future to justify unloading the db
            // if we never backed up the db then we want to do it now. returning the time now will prevent the unloading
            
            if (parameters.Logger.IsOperationsEnabled)
                parameters.Logger.Operations($"Backup Task '{parameters.Configuration.TaskId}' of database '{parameters.DatabaseName}' is never backed up yet.");

            return new IdleDatabaseActivity(IdleDatabaseActivityType.WakeUpDatabase, DateTime.UtcNow);
        }

        var responsibleNodeTag = GetResponsibleNodeTag(parameters.ServerStore, parameters.DatabaseName, parameters.Configuration.TaskId);
        if (responsibleNodeTag == null)
        {
            // cluster is down
            if (parameters.Logger.IsOperationsEnabled)
                parameters.Logger.Operations($"Could not find the responsible node for backup task '{parameters.Configuration.TaskId}' of database '{parameters.DatabaseName}'.");

            return null;
        }

        if (responsibleNodeTag != parameters.ServerStore.NodeTag)
        {
            // not responsible for this backup task
            if (parameters.Logger.IsOperationsEnabled)
                parameters.Logger.Operations($"Current server '{parameters.ServerStore.NodeTag}' is not responsible node for backup task '{parameters.Configuration.TaskId}' of database '{parameters.DatabaseName}'. Backup Task responsible node is '{responsibleNodeTag}'.");

            return null;
        }

        var nextBackup = GetNextBackupDetails(new NextBackupDetailsParameters
        {
            OnParsingError = parameters.OnParsingError,
            Configuration = parameters.Configuration,
            BackupStatus = backupStatus,
            ResponsibleNodeTag = responsibleNodeTag,
            DatabaseWakeUpTimeUtc = parameters.DatabaseWakeUpTimeUtc,
            NodeTag = parameters.ServerStore.NodeTag,
            OnMissingNextBackupInfo = parameters.OnMissingNextBackupInfo,
            IsIdle = parameters.IsIdle
        });

        if (nextBackup == null)
        {
            if (parameters.Logger.IsOperationsEnabled)
                parameters.Logger.Operations($"Backup Task '{parameters.Configuration.TaskId}' of database '{parameters.DatabaseName}' doesn't have next backup. Should not happen and likely a bug.");
            return null;
        }

        if (backupStatus == null)
        {
            // we want to wait for the backup occurrence
            if (parameters.Logger.IsOperationsEnabled)
                parameters.Logger.Operations($"Backup Task '{parameters.Configuration.TaskId}' of database '{parameters.DatabaseName}' is never backed up yet.");

            Console.WriteLine($"{DateTime.Now:O}: Backup Task '{parameters.Configuration.TaskId}' of database '{parameters.DatabaseName}' is never backed up yet.");

            return new IdleDatabaseActivity(IdleDatabaseActivityType.WakeUpDatabase, nextBackup.DateTime);
        }

        var nowUtc = DateTime.UtcNow;
        if (nextBackup.DateTime < nowUtc)
        {
            Console.WriteLine($"{DateTime.Now:O}: {parameters.DatabaseName} nextBackup.DateTime: {nextBackup.DateTime:O}");
            // Next backup is full and it's already due
            if (parameters.Logger.IsOperationsEnabled)
                parameters.Logger.Operations($"Backup Task '{parameters.Configuration.TaskId}' of database '{parameters.DatabaseName}' is already due.");

            Console.WriteLine($"{DateTime.Now:O}: Backup Task '{parameters.Configuration.TaskId}' of database '{parameters.DatabaseName}' is already due. Next database activity will be WakeUpDatabase now.");
            return new IdleDatabaseActivity(IdleDatabaseActivityType.WakeUpDatabase, DateTime.UtcNow);
        }

        if (backupStatus.LastEtag != parameters.LastEtag)
        {
            // we have changes since last backup
            var type = nextBackup.IsFull ? "full" : "incremental";
            if (parameters.Logger.IsOperationsEnabled)
                parameters.Logger.Operations($"Backup Task '{parameters.Configuration.TaskId}' of database '{parameters.DatabaseName}' have changes since last backup. Wakeup timer will be set to the next {type} backup at '{nextBackup.DateTime}'.");

            Console.WriteLine($"{DateTime.Now:O}: Backup Task '{parameters.Configuration.TaskId}' of database '{parameters.DatabaseName}' have changes since last backup. Wakeup timer will be set to the next {type} backup at '{nextBackup.DateTime}'.");
            return new IdleDatabaseActivity(IdleDatabaseActivityType.WakeUpDatabase, nextBackup.DateTime);
        }

        if (nextBackup.IsFull)
        {
            if (parameters.Logger.IsOperationsEnabled)
                parameters.Logger.Operations($"Backup Task '{parameters.Configuration.TaskId}' of database '{parameters.DatabaseName}' doesn't have changes since last backup. Wakeup timer will be set to the next full backup at '{nextBackup.DateTime}'.");
            // there are no changes since the last backup, and the next backup will be full.

            Console.WriteLine($"{DateTime.Now:O}: Backup Task '{parameters.Configuration.TaskId}' of database '{parameters.DatabaseName}' doesn't have changes since last backup. Wakeup timer will be set to the next full backup at '{nextBackup.DateTime}'.");
            return new IdleDatabaseActivity(IdleDatabaseActivityType.WakeUpDatabase, nextBackup.DateTime);
        }

        // there are no changes since the last backup, and the next backup will be incremental.
        var lastFullBackup = backupStatus.LastFullBackupInternal ?? nowUtc;
        var nextFullBackup = GetNextBackupOccurrence(new NextBackupOccurrenceParameters
        {
            BackupFrequency = parameters.Configuration.FullBackupFrequency,
            LastBackupUtc = lastFullBackup,
            Configuration = parameters.Configuration,
            OnParsingError = parameters.OnParsingError
        });

        if (nextFullBackup < nowUtc)
        {
            if (parameters.Logger.IsOperationsEnabled)
                parameters.Logger.Operations($"Backup Task '{parameters.Configuration.TaskId}' of database '{parameters.DatabaseName}' doesn't have changes since last backup but has already due full backup.");
            // this backup is delayed

            Console.WriteLine($"{DateTime.Now:O}: Backup Task '{parameters.Configuration.TaskId}' of database '{parameters.DatabaseName}' doesn't have changes since last backup but has already due full backup.");
            return new IdleDatabaseActivity(IdleDatabaseActivityType.WakeUpDatabase, nowUtc);
        }

        if (parameters.Logger.IsOperationsEnabled)
            parameters.Logger.Operations($"Backup Task '{parameters.Configuration.TaskId}' of database '{parameters.DatabaseName}' doesn't have changes since last backup. Wakeup timer set to next incremental backup at '{nextBackup.DateTime}' to update the backup status only.");

        Console.WriteLine($"{DateTime.Now:O}: Backup Task '{parameters.Configuration.TaskId}' of database '{parameters.DatabaseName}' doesn't have changes since last backup. Wakeup timer set to next incremental backup at '{nextBackup.DateTime}' to update the backup status only.");
        return new IdleDatabaseActivity(IdleDatabaseActivityType.UpdateBackupStatusOnly, nextBackup.DateTime, parameters.Configuration.TaskId, parameters.LastEtag);
    }

    public static void SaveBackupStatus(PeriodicBackupStatus status, string databaseName, ServerStore serverStore, Logger logger,
        BackupResult backupResult = default, Action<IOperationProgress> onProgress = default, OperationCancelToken operationCancelToken = default)
    {
        try
        {
            var raftId = RaftIdGenerator.NewId();

            Console.WriteLine($"{DateTime.Now:O}: SaveBackupStatus -> db: {databaseName}, LastFullBackupInternal: {status.LastFullBackupInternal:O}, LastIncrementalBackupInternal: {status.LastIncrementalBackupInternal:O}");

            AsyncHelpers.RunSync(async () =>
            {
                var index = await BackupHelper.RunWithRetriesAsync(maxRetries: 10, async () =>
                    {
                        var command = new UpdatePeriodicBackupStatusCommand(databaseName, raftId) { PeriodicBackupStatus = status };
                        var result = await serverStore.SendToLeaderAsync(command);
                        return result.Index;
                    },
                    infoMessage: "Saving the backup status in the cluster",
                    errorMessage: "Failed to save the backup status in the cluster",
                    backupResult, onProgress, operationCancelToken);

                await BackupHelper.RunWithRetriesAsync(maxRetries: 10, async () =>
                    {
                        await serverStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, index);
                        return default;
                    },
                    infoMessage: "Verifying that the backup status was successfully saved in the cluster",
                    errorMessage: "Failed to verify that the backup status was successfully saved in the cluster",
                    backupResult, onProgress, operationCancelToken);
            });

            if (logger.IsInfoEnabled)
                logger.Info($"Periodic backup status with task id {status.TaskId} was updated");
        }
        catch (Exception e)
        {
            const string message = "Error saving the periodic backup status";

            if (logger.IsOperationsEnabled)
                logger.Operations(message, e);

            backupResult?.AddError($"{message}{Environment.NewLine}{e}");
        }
    }

    public sealed class NextBackupOccurrenceParameters
    {
        public string BackupFrequency { get; set; }

        public DateTime LastBackupUtc { get; set; }

        public PeriodicBackupConfiguration Configuration { get; set; }

        public Action<OnParsingErrorParameters> OnParsingError { get; set; }
    }

    public sealed class OnParsingErrorParameters
    {
        public Exception Exception { get; set; }

        public string BackupFrequency { get; set; }

        public PeriodicBackupConfiguration Configuration { get; set; }
    }

    public sealed class NextBackupDetailsParameters
    {
        public PeriodicBackupConfiguration Configuration { get; set; }

        public PeriodicBackupStatus BackupStatus { get; set; }

        public string ResponsibleNodeTag { get; set; }

        public DateTime? DatabaseWakeUpTimeUtc { get; set; }

        public string NodeTag { get; set; }

        public DateTime? BaseLine { get; set; }

        public Action<OnParsingErrorParameters> OnParsingError { get; set; }

        public Action<PeriodicBackupConfiguration> OnMissingNextBackupInfo { get; set; }

        public bool IsIdle { get; set; }
    }

    public sealed class BackupInfoParameters
    {
        public ClusterOperationContext Context { get; set; }
        public ServerStore ServerStore { get; set; }
        public List<PeriodicBackup> PeriodicBackups { get; set; }
        public string DatabaseName { get; set; }
    }

    public class EarliestIdleDatabaseActivityParameters
    {
        public string DatabaseName { get; set; }

        public bool IsIdle { get; set; }

        public DateTime? DatabaseWakeUpTimeUtc { get; set; }

        public long LastEtag { get; set; }

        public Logger Logger { get; set; }

        public AbstractNotificationCenter NotificationCenter { get; set; }

        public Action<OnParsingErrorParameters> OnParsingError { get; set; }

        public Action<PeriodicBackupConfiguration> OnMissingNextBackupInfo { get; set; }

        public ServerStore ServerStore { get; set; }
    }

    public sealed class NextIdleDatabaseActivityParameters : EarliestIdleDatabaseActivityParameters
    {
        public PeriodicBackupConfiguration Configuration { get; set; }

        public ClusterOperationContext Context { get; set; }

        public NextIdleDatabaseActivityParameters(EarliestIdleDatabaseActivityParameters parameters, PeriodicBackupConfiguration configuration, ClusterOperationContext context)
        {
            Context = context;
            Configuration = configuration;
            DatabaseName = parameters.DatabaseName;
            DatabaseWakeUpTimeUtc = parameters.DatabaseWakeUpTimeUtc;
            LastEtag = parameters.LastEtag;
            Logger = parameters.Logger;
            NotificationCenter = parameters.NotificationCenter;
            OnParsingError = parameters.OnParsingError;
            OnMissingNextBackupInfo = parameters.OnMissingNextBackupInfo;
            ServerStore = parameters.ServerStore;
            IsIdle = parameters.IsIdle;
        }
    }
}
