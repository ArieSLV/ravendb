using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NuGet.Protocol;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Http;
using Raven.Client.ServerWide.Commands;
using Raven.Server.Documents;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.BackupHistory;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using static Raven.Server.Documents.Handlers.Debugging.NodeDebugHandler;

namespace Raven.Server.Web.System
{
    public sealed class BackupDatabaseHandler : RequestHandler
    {
        [RavenAction("/periodic-backup", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetPeriodicBackup()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            if (await CanAccessDatabaseAsync(name, requireAdmin: false, requireWrite: false) == false)
                return;

            var taskId = GetLongQueryString("taskId", required: true).Value;
            if (taskId == 0)
                throw new ArgumentException("Task ID cannot be 0");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            using (var rawRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, name))
            {
                var periodicBackup = rawRecord.GetPeriodicBackupConfiguration(taskId);
                if (periodicBackup == null)
                    throw new InvalidOperationException($"Periodic backup task ID: {taskId} doesn't exist");

                context.Write(writer, periodicBackup.ToJson());
            }
        }

        [RavenAction("/periodic-backup/status", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetPeriodicBackupStatus()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            if (await CanAccessDatabaseAsync(name, requireAdmin: false, requireWrite: false) == false)
                return;

            var taskId = GetLongQueryString("taskId", required: true);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            using (var statusBlittable = ServerStore.Cluster.Read(context, PeriodicBackupStatus.GenerateItemName(name, taskId.Value)))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(GetPeriodicBackupStatusOperationResult.Status));
                writer.WriteObject(statusBlittable);
                writer.WriteEndObject();
            }
        }

        [RavenAction("/admin/debug/periodic-backup/timers", "GET", AuthorizationStatus.Operator)]
        public async Task GetAllPeriodicBackupsTimers()
        {
            var first = true;
            var count = 0;

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                WriteStartOfTimers(writer);

                foreach ((var name, Task<DocumentDatabase> task) in ServerStore.DatabasesLandlord.DatabasesCache)
                {
                    if (task.Status != TaskStatus.RanToCompletion)
                        continue;

                    var database = await task;
                    if (database.PeriodicBackupRunner.PeriodicBackups.Count == 0)
                        continue;

                    if (first == false)
                        writer.WriteComma();

                    first = false;
                    WritePeriodicBackups(database, writer, context, out int c);
                    count += c;
                }

                WriteEndOfTimers(writer, count);
            }
        }

        [RavenAction("/admin/backup-history", "GET", AuthorizationStatus.Operator)]
        public async Task GetBackupHistory()
        {
            // var backupHistoryEntries = new List<BackupHistoryTableValue>();
            var dest = GetStringQueryString("url", false) ?? GetStringQueryString("node", false);

            var topology = ServerStore.GetClusterTopology();
            var tasks = new List<Task<AsyncBlittableJsonTextWriter>();
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                if (string.IsNullOrEmpty(dest))
                {
                    foreach (var node in topology.AllNodes)
                    {
                        tasks.Add(GetBackupHistoryFromNode(node.Value, writer));
                    }
                }
                else
                {
                    var url = topology.GetUrlFromTag(dest);
                    tasks.Add(GetBackupHistoryFromNode(url ?? dest, writer));
                }


                writer.WriteStartObject();
                writer.WritePropertyName("Result");

                writer.WriteStartArray();
                while (tasks.Count > 0)
                {
                    var task = await Task.WhenAny(tasks);
                    tasks.Remove(task);
                    foreach (var obj in task.Result)
                    {

                    }

                    task.Result
                    context.Write(writer,);
                    if (tasks.Count > 0)
                    {
                        writer.WriteComma();
                    }

                    await writer.MaybeFlushAsync();
                }

                writer.WriteEndArray();
                writer.WriteEndObject();
            }


            async Task GetBackupHistoryFromNode(string url, AsyncBlittableJsonTextWriter writer)
            {

                if (url.Equals(ServerStore.GetNodeTcpServerUrl(), StringComparison.OrdinalIgnoreCase))
                {
                    var first = true;

                    writer.WriteStartObject();
                    writer.WritePropertyName(url);
                    writer.WriteStartArray();

                    using (ServerStore.BackupHistoryStorage.ReadEntriesOrderedByCreationDate(out var entries))
                        foreach (var entry in entries)
                        {
                            if (first == false)
                                writer.WriteComma();

                            first = false;
                            writer.WriteObject(entry.Json);
                        }


                    writer.WriteEndObject();
                    writer.WriteEndArray();
                }
                else
                {
                    using (var requestExecutor =
                           ClusterRequestExecutor.CreateForSingleNode(url, ServerStore.Engine.ClusterCertificate, DocumentConventions.DefaultForServer))
                    using (requestExecutor.ContextPool.AllocateOperationContext(out var context))
                    {
                        var command = new GetBackupHistoryCommand(url);
                        entriesToReturn.Add(command.Result);
                    }
                }

                return entriesToReturn;


                
                    
                


            }

            internal static void WriteEndOfTimers(AbstractBlittableJsonTextWriter writer, int count)
            {
                writer.WriteEndArray();
                writer.WriteComma();
                writer.WritePropertyName("TimersCount");
                writer.WriteInteger(count);
                writer.WriteEndObject();
            }

            internal static void WriteStartOfTimers(AbstractBlittableJsonTextWriter writer)
            {
                writer.WriteStartObject();
                writer.WritePropertyName("TimersList");
                writer.WriteStartArray();
            }

            internal static void WritePeriodicBackups(DocumentDatabase db, AbstractBlittableJsonTextWriter writer, JsonOperationContext context, out int count)
            {
                count = 0;
                var first = true;
                foreach (var periodicBackup in db.PeriodicBackupRunner.PeriodicBackups)
                {
                    if (first == false)
                        writer.WriteComma();

                    first = false;
                    writer.WriteStartObject();
                    writer.WritePropertyName("DatabaseName");
                    writer.WriteString(db.Name);
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(periodicBackup.Configuration.TaskId));
                    writer.WriteInteger(periodicBackup.Configuration.TaskId);
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(periodicBackup.Configuration.Name));
                    writer.WriteString(periodicBackup.Configuration.Name);
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(periodicBackup.Configuration.FullBackupFrequency));
                    writer.WriteString(periodicBackup.Configuration.FullBackupFrequency);
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(periodicBackup.Configuration.IncrementalBackupFrequency));
                    writer.WriteString(periodicBackup.Configuration.IncrementalBackupFrequency);
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(NextBackup));
                    using (var nextBackup = context.ReadObject(periodicBackup.GetNextBackup().ToJson(), "nextBackup"))
                        writer.WriteObject(nextBackup);
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(PeriodicBackup.BackupTimer.CreatedAt));
                    var createdAt = periodicBackup.GetCreatedAt();
                    if (createdAt.HasValue == false)
                        writer.WriteNull();
                    else
                        writer.WriteDateTime(createdAt.Value, isUtc: true);
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(PeriodicBackup.Disposed));
                    writer.WriteBool(periodicBackup.Disposed);
                    writer.WriteEndObject();

                    count++;
                }
            }
        }
    }
