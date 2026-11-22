﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Maintenance;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Cluster
{
    public class ClusterObserverTests : ReplicationTestBase
    {
        public ClusterObserverTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.ClientApi)]
        public async Task Move_To_Rehab_Grace_Time_Keeps_The_Priority_Order()
        {
            const int clusterSize = 3;
            var settings = new Dictionary<string, string>()
            {
                [RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] = 300.ToString(),
                [RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "60",
                [RavenConfiguration.GetKey(x => x.Cluster.RotatePreferredNodeGraceTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Replication.ReplicationMinimalHeartbeat)] = "15",
            };

            var cluster = await CreateRaftCluster(clusterSize, false, 0, watcherCluster: true, customSettings: settings);
            var order = new List<string> { "A", "B", "C" };

            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = clusterSize,
                ModifyDatabaseRecord = x => x.Topology = new DatabaseTopology
                {
                    Members = order,
                    ReplicationFactor = 3,
                    PriorityOrder = order
                }
            }))
            {
                var tcs = new TaskCompletionSource<DocumentDatabase>(TaskCreationOptions.RunContinuationsAsynchronously);

                var databaseName = store.Database;
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/1");
                    session.SaveChanges();

                    Assert.True(await WaitForDocumentInClusterAsync<User>(cluster.Nodes, databaseName, "users/1", _ => true, TimeSpan.FromSeconds(5)));
                }

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var preferred = Servers.Single(s => s.ServerStore.NodeTag == record.Topology.Members[0]);

                int val;
                using (new DisposableAction(() =>
                {
                    if (preferred.ServerStore.DatabasesLandlord.DatabasesCache.TryRemove(databaseName, tcs.Task))
                        tcs.TrySetCanceled();
                }))
                {
                    var database = await preferred.ServerStore.DatabasesLandlord.DatabasesCache.ForTestingPurposesOnly().Replace(databaseName, tcs.Task);
                    database.Dispose();

                    Assert.True(await WaitForValueAsync(async () =>
                    {
                        record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                        return record.Topology.Members[0] != preferred.ServerStore.NodeTag;
                    }, true));

                    val = await WaitForValueAsync(async () => await GetRehabCount(store, databaseName), 0);
                    Assert.Equal(0, val);
                    val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), clusterSize);
                    Assert.Equal(clusterSize, val);
                }

                val = await WaitForValueAsync(async () => await GetRehabCount(store, databaseName), 0);
                Assert.Equal(0, val);
                val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), clusterSize);
                Assert.Equal(clusterSize, val);

                await WaitForValueAsync(async () =>
                {
                    var r = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    return r.Topology.Members.SequenceEqual(order);
                }, true);

                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(order, record.Topology.Members);
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.ClusterTransactions)]
        public async Task Cluster_Transaction_Counter_Mismatch_Raises_Alert_On_Leader()
        {
            var (nodes, leader) = await CreateRaftCluster(3, leaderIndex: 0);

            using (var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 2 }))
            {
                var database = store.Database;
                await WriteClusterWide(store, "warmup", count: 5, documentsPerTransaction: 2);

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(database));
                var members = record.Topology.Members;
                Assert.Equal(2, members.Count);

                Assert.True(await WaitForValueAsync(() => CountersReported(leader, database, members), true));
                Assert.True(await WaitForValueAsync(() => HasNoStoredCommands(leader, database), true, timeout: 30_000));
                await Task.Delay(TimeSpan.FromSeconds(3));
                Assert.False(HasCounterAlertOnAnyNode(nodes, database));

                var raised = nodes.Single(n => n.ServerStore.NodeTag == members.First(m => m != leader.ServerStore.NodeTag));
                RaiseClusterTransactionCounter(raised, database, 2);

                await WriteClusterWide(store, "after", count: 1, documentsPerTransaction: 2);

                Assert.True(await WaitForValueAsync(() => HasCounterAlertOnAnyNode(nodes, database), true, timeout: 15_000), Diagnose(nodes, database));

                var message = ReadCounterAlertMessageFromAnyNode(nodes, database);
                Output.WriteLine(message);
                Assert.Contains($"'{raised.ServerStore.NodeTag}'", message);
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.ClusterTransactions)]
        public async Task Cluster_Transaction_Counter_Mismatch_Is_Detected_With_A_Large_Backlog()
        {
            const int writersCount = 8;
            const long backlogToReach = 15_000;

            var (nodes, leader) = await CreateRaftCluster(3, leaderIndex: 0);

            var dbNodes = nodes.Where(n => n != leader).Select(n => n.ServerStore.NodeTag).ToList();

            using (var store = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = 2,
                ModifyDatabaseRecord = r => r.Topology = new DatabaseTopology { Members = dbNodes }
            }))
            {
                var database = store.Database;
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(database));
                var members = record.Topology.Members;
                Assert.DoesNotContain(leader.ServerStore.NodeTag, members);

                var executing = nodes.Single(n => n.ServerStore.NodeTag == members[0]);
                var stalled = nodes.Single(n => n.ServerStore.NodeTag == members[1]);
                using var release = new ManualResetEventSlim(false);
                stalled.ServerStore.ForTestingPurposesOnly().BeforeExecuteClusterTransactionBatch = (name, _) =>
                {
                    if (string.Equals(name, database, StringComparison.OrdinalIgnoreCase))
                        release.Wait(TimeSpan.FromMinutes(3));
                };

                using var stop = new CancellationTokenSource(TimeSpan.FromMinutes(3));
                var transactions = 0;
                var failures = 0;

                var writers = Enumerable.Range(0, writersCount).Select(writer => Task.Run(async () =>
                {
                    for (var i = 0; stop.IsCancellationRequested == false; i++)
                    {
                        try
                        {
                            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                            {
                                await session.StoreAsync(new User { Name = $"{writer}/{i}" }, $"backlog/{writer}/{i}");
                                await session.SaveChangesAsync();
                                Interlocked.Increment(ref transactions);
                            }
                        }
                        catch
                        {
                            Interlocked.Increment(ref failures);
                        }
                    }
                })).ToArray();

                try
                {
                    var backlogReached = await WaitForValueAsync(() => StoredCommandsBacklog(leader, database) >= backlogToReach, true, timeout: 150_000, interval: 500);
                    var backlog = StoredCommandsBacklog(leader, database);
                    Output.WriteLine($"backlog on the leader: {backlog:N0} stored commands after {transactions:N0} transactions");
                    Assert.True(backlogReached);
                    Assert.False(HasCounterAlertOnAnyNode(nodes, database), HasCounterAlertOnAnyNode(nodes, database) ? ReadCounterAlertMessageFromAnyNode(nodes, database) : null);

                    var sw = System.Diagnostics.Stopwatch.StartNew();
                    RaiseClusterTransactionCounter(executing, database, 2);

                    var detected = await WaitForValueAsync(() => HasCounterAlertOnAnyNode(nodes, database), true, timeout: 30_000, interval: 50);
                    Output.WriteLine($"detected: {detected} after {sw.Elapsed.TotalMilliseconds:N0} ms; load still running: {stop.IsCancellationRequested == false}; " +
                                     $"backlog now: {StoredCommandsBacklog(leader, database):N0}; failed: {failures:N0}");
                    if (detected)
                        Output.WriteLine(ReadCounterAlertMessageFromAnyNode(nodes, database));

                    Assert.True(detected, Diagnose(nodes, database));
                    Assert.Contains($"'{executing.ServerStore.NodeTag}'", ReadCounterAlertMessageFromAnyNode(nodes, database));
                }
                finally
                {
                    stop.Cancel();
                    release.Set();
                    await Task.WhenAll(writers);
                }
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.ClusterTransactions)]
        public async Task Cluster_Transaction_Counter_Check_Raises_No_Alert_Under_Load_Leader_Change_And_Reload()
        {
            var (nodes, leader) = await CreateRaftCluster(3, leaderIndex: 0);

            using (var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 3 }))
            {
                var database = store.Database;
                var transactions = await RunMixedLoad(store, "first", TimeSpan.FromSeconds(15));
                Output.WriteLine($"cluster-wide transactions before the leader change: {transactions:N0}");

                leader.ServerStore.Engine.CurrentLeader?.StepDown();
                Assert.True(await WaitForValueAsync(() => nodes.Any(n => n != leader && n.ServerStore.IsLeader()), true, timeout: 30_000));
                var newLeader = nodes.Single(n => n.ServerStore.IsLeader());
                Output.WriteLine($"leader changed from {leader.ServerStore.NodeTag} to {newLeader.ServerStore.NodeTag}");

                var reloaded = nodes.First(n => n != newLeader);
                reloaded.ServerStore.DatabasesLandlord.UnloadDirectly(database);
                await GetDocumentDatabaseInstanceForAsync(database, reloaded);

                transactions += await RunMixedLoad(store, "second", TimeSpan.FromSeconds(10));
                Output.WriteLine($"cluster-wide transactions in total: {transactions:N0}");

                Assert.Equal(1, await WaitForValueAsync(() => nodes.Select(n => ReadClusterTransactionCounter(n, database)).Distinct().Count(), 1));
                Assert.True(await WaitForValueAsync(() => CountersReported(newLeader, database, nodes.Select(n => n.ServerStore.NodeTag)), true));
                await Task.Delay(TimeSpan.FromSeconds(3));

                foreach (var node in nodes)
                    Assert.False(HasCounterAlert(node, database), HasCounterAlert(node, database) ? ReadCounterAlertMessage(node, database) : null);
                Assert.DoesNotContain(newLeader.ServerStore.Observer.ReadDecisionsForDatabase().List, e => e.Message.Contains("Failed to check the cluster transaction counters"));
            }
        }

        [RavenTheory(RavenTestCategory.Cluster | RavenTestCategory.ClusterTransactions)]
        [InlineData(1)]
        [InlineData(3)]
        public async Task Cluster_Transaction_Counter_Mismatch_Is_Detected_While_Load_Continues(int documentsPerTransaction)
        {
            const int writersCount = 8;
            var (nodes, leader) = await CreateRaftCluster(3, leaderIndex: 0);

            using (var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 2 }))
            {
                var database = store.Database;
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(database));
                var members = record.Topology.Members;

                using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(90));
                var transactions = 0;
                var failures = 0;

                var writers = Enumerable.Range(0, writersCount).Select(writer => Task.Run(async () =>
                {
                    for (var i = 0; stop.IsCancellationRequested == false; i++)
                    {
                        try
                        {
                            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                            {
                                for (var d = 0; d < documentsPerTransaction; d++)
                                    await session.StoreAsync(new User { Name = $"{writer}/{i}" }, $"load/{writer}/{i}/{d}");
                                await session.SaveChangesAsync();
                                Interlocked.Increment(ref transactions);
                            }
                        }
                        catch
                        {
                            Interlocked.Increment(ref failures);
                        }
                    }
                })).ToArray();

                await Task.Delay(TimeSpan.FromSeconds(10));
                Assert.True(CountersReported(leader, database, members));
                Assert.False(HasCounterAlertOnAnyNode(nodes, database));

                var raised = nodes.Single(n => n.ServerStore.NodeTag == members[1]);
                var transactionsAtOffset = Volatile.Read(ref transactions);
                var sw = System.Diagnostics.Stopwatch.StartNew();
                RaiseClusterTransactionCounter(raised, database, 2);

                var detected = await WaitForValueAsync(() => HasCounterAlertOnAnyNode(nodes, database), true, timeout: 60_000, interval: 50);
                var detectedAfter = sw.Elapsed;
                var transactionsWhileWaiting = Volatile.Read(ref transactions) - transactionsAtOffset;
                var stillLoaded = stop.IsCancellationRequested == false;

                stop.Cancel();
                await Task.WhenAll(writers);

                Output.WriteLine($"detected: {detected} after {detectedAfter.TotalMilliseconds:N0} ms; load still running: {stillLoaded}; " +
                                 $"cluster-wide transactions while waiting: {transactionsWhileWaiting:N0}; total: {transactions:N0}; failed: {failures:N0}");
                if (detected)
                    Output.WriteLine(ReadCounterAlertMessageFromAnyNode(nodes, database));

                Assert.True(detected, Diagnose(nodes, database));
                Assert.True(stillLoaded);
                Assert.Contains($"'{raised.ServerStore.NodeTag}'", ReadCounterAlertMessageFromAnyNode(nodes, database));
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.ClusterTransactions)]
        public async Task Cluster_Transaction_Counter_Mismatch_After_An_Apply_Exception_On_One_Node_Raises_One_Alert()
        {
            const string id = "users/expiring";
            var (nodes, leader) = await CreateRaftCluster(3, leaderIndex: 0);

            using (var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 3 }))
            {
                var database = store.Database;
                var guardKey = ClusterWideTransactionHelper.GetAtomicGuardKey(id);
                var faulted = nodes.First(n => n != leader);
                var healthy = nodes.Single(n => n != leader && n != faulted);
                using var leaderStore = GetStoreForNode(leader, database);

                using (var session = leaderStore.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var user = new User { Name = "v0" };
                    await session.StoreAsync(user, id);
                    session.Advanced.GetMetadataFor(user)[Constants.Documents.Metadata.Expires] = DateTime.UtcNow.AddDays(30);
                    await session.SaveChangesAsync();
                }
                await WriteClusterWide(leaderStore, "before", count: 3);
                Assert.True(await WaitForValueAsync(() => CountersReported(leader, database, nodes.Select(n => n.ServerStore.NodeTag)), true));
                await Task.Delay(TimeSpan.FromSeconds(2));
                Assert.False(HasCounterAlertOnAnyNode(nodes, database));

                var fired = 0;
                faulted.ServerStore.Engine.ForTestingPurposesOnly().BeforeGetCompareExchangeExpires = key =>
                {
                    if (string.Equals(key, guardKey, StringComparison.OrdinalIgnoreCase) && Interlocked.Exchange(ref fired, 1) == 0)
                        throw new InvalidOperationException("injected node-local fault in TryGetExpires");
                };
                try
                {
                    using (var session = leaderStore.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                    {
                        var user = await session.LoadAsync<User>(id);
                        user.Name = "v1";
                        await session.SaveChangesAsync();
                    }
                }
                finally
                {
                    faulted.ServerStore.Engine.ForTestingPurposesOnly().BeforeGetCompareExchangeExpires = null;
                }
                Assert.Equal(1, fired);
                Output.WriteLine($"counters after the fault: {string.Join(", ", nodes.Select(n => $"{n.ServerStore.NodeTag}={ReadClusterTransactionCounter(n, database)}"))}");

                await WriteClusterWide(leaderStore, "after", count: 5);
                Assert.True(await WaitForValueAsync(() => HasCounterAlertOnAnyNode(nodes, database), true, timeout: 15_000), Diagnose(nodes, database));

                var message = ReadCounterAlertMessageFromAnyNode(nodes, database);
                Output.WriteLine(message);
                Assert.Contains($"Node '{faulted.ServerStore.NodeTag}'", message);
                Assert.DoesNotContain($"Node '{healthy.ServerStore.NodeTag}'", message);

                await WriteClusterWide(leaderStore, "more", count: 20);
                await Task.Delay(TimeSpan.FromSeconds(3));
                var entries = leader.ServerStore.Observer.ReadDecisionsForDatabase().List
                    .Count(e => e.Database == database && e.Message.Contains("acknowledged to the client without being written"));
                Assert.Equal(1, entries);
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.ClusterTransactions)]
        public async Task Cluster_Transaction_Counter_Mismatch_On_The_Leader_Names_The_Leader()
        {
            var (nodes, leader) = await CreateRaftCluster(3, leaderIndex: 0);

            using (var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 3 }))
            {
                var database = store.Database;
                await WriteClusterWide(store, "warmup", count: 3);
                Assert.True(await WaitForValueAsync(() => CountersReported(leader, database, nodes.Select(n => n.ServerStore.NodeTag)), true));

                RaiseClusterTransactionCounter(leader, database, 2);
                await WriteClusterWide(store, "after", count: 5);

                var hint = $"so node '{leader.ServerStore.NodeTag}' is the likely odd one";
                Assert.True(await WaitForValueAsync(() => HasCounterAlertOnAnyNode(nodes, database) && ReadCounterAlertMessageFromAnyNode(nodes, database).Contains(hint), true, timeout: 15_000), Diagnose(nodes, database));
                Output.WriteLine(ReadCounterAlertMessageFromAnyNode(nodes, database));
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.ClusterTransactions)]
        public async Task Cluster_Transaction_Counter_Mismatch_On_A_Leader_That_Hosts_The_Database_Is_Detected_After_Cleanup()
        {
            var (nodes, leader) = await CreateRaftCluster(3, leaderIndex: 0);
            var peer = nodes.First(n => n != leader);

            using (var store = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = 2,
                ModifyDatabaseRecord = r => r.Topology = new DatabaseTopology { Members = new List<string> { leader.ServerStore.NodeTag, peer.ServerStore.NodeTag } }
            }))
            {
                var database = store.Database;
                await WriteClusterWide(store, "warmup", count: 3, documentsPerTransaction: 2);
                Assert.True(await WaitForValueAsync(() => CountersReported(leader, database, new[] { peer.ServerStore.NodeTag }), true));
                Assert.True(await WaitForValueAsync(() => HasNoStoredCommands(leader, database), true, timeout: 30_000), Diagnose(nodes, database));
                Assert.False(HasCounterAlertOnAnyNode(nodes, database));

                RaiseClusterTransactionCounter(leader, database, 2);
                await WriteClusterWide(store, "after", count: 1, documentsPerTransaction: 2);

                Assert.True(await WaitForValueAsync(() => HasCounterAlertOnAnyNode(nodes, database), true, timeout: 15_000), Diagnose(nodes, database));
                var message = ReadCounterAlertMessageFromAnyNode(nodes, database);
                Output.WriteLine(message);
                Assert.Contains("that transaction is stored at a later position", message);
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.ClusterTransactions)]
        public async Task Cluster_Transaction_Counter_Mismatch_On_A_Leader_That_Does_Not_Host_The_Database_Says_So()
        {
            var (nodes, leader) = await CreateRaftCluster(3, leaderIndex: 0);
            var dbNodes = nodes.Where(n => n != leader).Select(n => n.ServerStore.NodeTag).ToList();

            using (var store = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = 2,
                ModifyDatabaseRecord = r => r.Topology = new DatabaseTopology { Members = dbNodes }
            }))
            {
                var database = store.Database;
                await WriteClusterWide(store, "warmup", count: 3);
                Assert.True(await WaitForValueAsync(() => CountersReported(leader, database, dbNodes), true));

                RaiseClusterTransactionCounter(leader, database, 2);
                await WriteClusterWide(store, "after", count: 5);

                var hint = $"Node '{leader.ServerStore.NodeTag}' does not host this database";
                Assert.True(await WaitForValueAsync(() => HasCounterAlert(leader, database) && ReadCounterAlertMessage(leader, database).Contains(hint), true, timeout: 15_000),
                    Diagnose(nodes, database));
                Output.WriteLine(ReadCounterAlertMessage(leader, database));
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.ClusterTransactions)]
        public async Task Cluster_Transaction_Counter_Mismatch_On_The_Leader_Shows_When_The_Other_Node_Executes_Again()
        {
            var (nodes, leader) = await CreateRaftCluster(3, leaderIndex: 0);
            var peer = nodes.First(n => n != leader);

            using (var store = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = 2,
                ModifyDatabaseRecord = r => r.Topology = new DatabaseTopology { Members = new List<string> { leader.ServerStore.NodeTag, peer.ServerStore.NodeTag } }
            }))
            {
                var database = store.Database;
                using var leaderStore = GetStoreForNode(leader, database);
                await WriteClusterWide(leaderStore, "warmup", count: 3);
                Assert.True(await WaitForValueAsync(() => CountersReported(leader, database, new[] { leader.ServerStore.NodeTag, peer.ServerStore.NodeTag }), true));

                using var release = new ManualResetEventSlim(false);
                peer.ServerStore.ForTestingPurposesOnly().BeforeExecuteClusterTransactionBatch = (name, _) =>
                {
                    if (string.Equals(name, database, StringComparison.OrdinalIgnoreCase))
                        release.Wait(TimeSpan.FromMinutes(2));
                };

                try
                {
                    RaiseClusterTransactionCounter(leader, database, 2);
                    await WriteClusterWide(leaderStore, "while-stalled", count: 5);

                    await Task.Delay(TimeSpan.FromSeconds(3));
                    Assert.False(HasCounterAlertOnAnyNode(nodes, database));

                    var sw = System.Diagnostics.Stopwatch.StartNew();
                    release.Set();
                    Assert.True(await WaitForValueAsync(() => HasCounterAlertOnAnyNode(nodes, database), true, timeout: 15_000), Diagnose(nodes, database));
                    Output.WriteLine($"detected {sw.Elapsed.TotalMilliseconds:N0} ms after the other node resumed: {ReadCounterAlertMessageFromAnyNode(nodes, database)}");
                    Assert.Contains($"'{peer.ServerStore.NodeTag}'", ReadCounterAlertMessageFromAnyNode(nodes, database));
                }
                finally
                {
                    release.Set();
                    peer.ServerStore.ForTestingPurposesOnly().BeforeExecuteClusterTransactionBatch = null;
                }
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.ClusterTransactions)]
        public async Task Cluster_Transaction_Counter_Mismatch_Is_Reported_By_A_New_Leader_After_Cleanup()
        {
            var (nodes, leader) = await CreateRaftCluster(3, leaderIndex: 0);

            using (var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 3 }))
            {
                var database = store.Database;
                await WriteClusterWide(store, "warmup", count: 3);
                Assert.True(await WaitForValueAsync(() => CountersReported(leader, database, nodes.Select(n => n.ServerStore.NodeTag)), true));

                var raised = nodes.First(n => n != leader);
                RaiseClusterTransactionCounter(raised, database, 2);
                await WriteClusterWide(store, "after", count: 1, documentsPerTransaction: 2);
                Assert.True(await WaitForValueAsync(() => HasCounterAlertOnAnyNode(nodes, database), true, timeout: 15_000), Diagnose(nodes, database));
                Output.WriteLine($"old leader {leader.ServerStore.NodeTag}: {ReadCounterAlertMessageFromAnyNode(nodes, database)}");

                var candidates = nodes.Where(n => n != raised).ToList();
                Assert.True(await WaitForValueAsync(() => candidates.All(n => HasNoStoredCommands(n, database)), true, timeout: 30_000));

                var third = nodes.Single(n => n != leader && n != raised);
                for (var attempt = 0; attempt < 15 && third.ServerStore.IsLeader() == false; attempt++)
                {
                    var current = nodes.FirstOrDefault(n => n.ServerStore.IsLeader());
                    if (current == null)
                    {
                        await WaitForValueAsync(() => nodes.Any(n => n.ServerStore.IsLeader()), true, timeout: 15_000);
                        continue;
                    }

                    current.ServerStore.Engine.CurrentLeader?.StepDown();
                    await WaitForValueAsync(() => nodes.Any(n => n != current && n.ServerStore.IsLeader()), true, timeout: 15_000);
                }
                Assert.True(third.ServerStore.IsLeader(), "Setup: could not move leadership to the third node");

                Assert.True(await WaitForValueAsync(() => HasCounterAlert(third, database), true, timeout: 30_000));
                var message = ReadCounterAlertMessage(third, database);
                Output.WriteLine($"new leader {third.ServerStore.NodeTag}: {message}");
                Assert.Contains($"Node '{raised.ServerStore.NodeTag}' executed every cluster transaction up to position", message);
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.ClusterTransactions)]
        public async Task Cluster_Transaction_Counter_Check_Ignores_A_Report_About_Another_Database_With_The_Same_Name()
        {
            var (nodes, leader) = await CreateRaftCluster(3, leaderIndex: 0);

            using (var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 3 }))
            {
                var database = store.Database;
                await WriteClusterWide(store, "warmup", count: 3);
                Assert.True(await WaitForValueAsync(() => CountersReported(leader, database, nodes.Select(n => n.ServerStore.NodeTag)), true));
                Assert.False(HasCounterAlertOnAnyNode(nodes, database));

                var node = nodes.First(n => n != leader);
                var real = leader.ServerStore.ClusterMaintenanceSupervisor.GetStats()[node.ServerStore.NodeTag].Report[database];
                var mismatching = new DatabaseStatusReport(real)
                {
                    ExecutedClusterTransactionRaftIndex = real.ExecutedClusterTransactionRaftIndex + 1_000,
                    ExecutedClusterTransactionNextCount = real.ExecutedClusterTransactionNextCount + 1_000
                };

                mismatching.DatabaseGroupId = "a-deleted-database-with-the-same-name";
                CheckClusterTransactionCounters(leader, database, node, mismatching);
                Assert.False(HasCounterAlert(leader, database));

                mismatching.DatabaseGroupId = real.DatabaseGroupId;
                CheckClusterTransactionCounters(leader, database, node, mismatching);
                Assert.True(HasCounterAlert(leader, database));
                Output.WriteLine(ReadCounterAlertMessage(leader, database));
            }
        }

        private static void CheckClusterTransactionCounters(RavenServer leader, string database, RavenServer node, DatabaseStatusReport report)
        {
            using (leader.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            using (var rawRecord = leader.ServerStore.Cluster.ReadRawDatabaseRecord(context, database))
            {
                var current = new Dictionary<string, ClusterNodeStatusReport>
                {
                    [node.ServerStore.NodeTag] = new ClusterNodeStatusReport(new ServerReport(), new Dictionary<string, DatabaseStatusReport> { [database] = report },
                        ClusterNodeStatusReport.ReportStatus.Ok, null, DateTime.UtcNow, null)
                };
                var state = new ClusterObserver.DatabaseObservationState(database, rawRecord, rawRecord.Topology, leader.ServerStore.GetClusterTopology(context),
                    current, new Dictionary<string, ClusterNodeStatusReport>(), lastIndexModification: 0, observerIteration: 0);

                leader.ServerStore.Observer.CheckClusterTransactionCounters(context, state);
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.ClusterTransactions)]
        public async Task Cluster_Transaction_Counter_Check_Raises_No_Alert_For_A_Recreated_Database()
        {
            var (nodes, leader) = await CreateRaftCluster(3, leaderIndex: 0);

            using (var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 3 }))
            {
                var database = store.Database;
                var step = TimeSpan.FromSeconds(60);
                await WriteClusterWide(store, "old", count: 20).WaitAsync(step);
                Assert.True(await WaitForValueAsync(() => CountersReported(leader, database, nodes.Select(n => n.ServerStore.NodeTag)), true));
                Output.WriteLine("old database written");

                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(database, hardDelete: true, timeToWaitForConfirmation: TimeSpan.FromSeconds(30))).WaitAsync(step);
                Output.WriteLine("delete sent");
                Assert.True(await WaitForValueAsync(() => nodes.All(n => n.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(database, out _) == false), true, timeout: 30_000));
                Output.WriteLine("database unloaded on every node");

                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(database), replicationFactor: 3)).WaitAsync(step);
                Output.WriteLine("database created again");
                foreach (var node in nodes)
                    await GetDocumentDatabaseInstanceForAsync(database, node).AsTask().WaitAsync(step);
                Output.WriteLine("database loaded on every node");

                using (var newStore = GetStoreForNode(leader, database))
                    await WriteClusterWide(newStore, "new", count: 5).WaitAsync(step);
                Output.WriteLine("new database written");
                Assert.True(await WaitForValueAsync(() => CountersReported(leader, database, nodes.Select(n => n.ServerStore.NodeTag)), true));
                await Task.Delay(TimeSpan.FromSeconds(3));

                Assert.False(HasCounterAlertOnAnyNode(nodes, database), HasCounterAlertOnAnyNode(nodes, database) ? ReadCounterAlertMessageFromAnyNode(nodes, database) : null);
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.ClusterTransactions | RavenTestCategory.Sharding)]
        public async Task Cluster_Transaction_Counter_Check_Skips_Sharded_Databases()
        {
            var (nodes, leader) = await CreateRaftCluster(3, leaderIndex: 0);
            var options = Sharding.GetOptionsForCluster(leader, shards: 2, shardReplicationFactor: 2, orchestratorReplicationFactor: 2);

            using (var store = GetDocumentStore(options))
            {
                var database = store.Database;
                await WriteClusterWide(store, "sharded", count: 20);
                await Task.Delay(TimeSpan.FromSeconds(3));

                foreach (var node in nodes)
                    Assert.False(HasCounterAlert(node, database));
                Assert.DoesNotContain(leader.ServerStore.Observer.ReadDecisionsForDatabase().List, e => e.Message.Contains("Failed to check the cluster transaction counters"));
            }
        }

        [RavenFact(RavenTestCategory.Cluster)]
        public void Database_Status_Report_Is_Read_Across_Versions()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var older = new DynamicJsonValue
                {
                    [nameof(DatabaseStatusReport.Name)] = "db",
                    [nameof(DatabaseStatusReport.NodeName)] = "A",
                    [nameof(DatabaseStatusReport.LastCompletedClusterTransaction)] = 42,
                    [nameof(DatabaseStatusReport.LastClusterWideTransactionRaftIndex)] = 100
                };
                var fromOlder = JsonDeserializationServer.DatabaseStatusReport(context.ReadObject(older, "older"));
                Assert.Equal(42, fromOlder.LastCompletedClusterTransaction);
                Assert.Equal(0, fromOlder.ExecutedClusterTransactionRaftIndex);
                Assert.Equal(0, fromOlder.ExecutedClusterTransactionPreviousCount);
                Assert.Equal(0, fromOlder.ExecutedClusterTransactionNextCount);
                Assert.Null(fromOlder.DatabaseGroupId);

                var newer = new DatabaseStatusReport
                {
                    Name = "db",
                    NodeName = "B",
                    ExecutedClusterTransactionRaftIndex = 100,
                    ExecutedClusterTransactionPreviousCount = 41,
                    ExecutedClusterTransactionNextCount = 42,
                    DatabaseGroupId = "group"
                }.ToJson();
                newer["SomeFieldFromALaterVersion"] = 7;
                var fromNewer = JsonDeserializationServer.DatabaseStatusReport(context.ReadObject(newer, "newer"));
                Assert.Equal(100, fromNewer.ExecutedClusterTransactionRaftIndex);
                Assert.Equal(41, fromNewer.ExecutedClusterTransactionPreviousCount);
                Assert.Equal(42, fromNewer.ExecutedClusterTransactionNextCount);
                Assert.Equal("group", fromNewer.DatabaseGroupId);
            }
        }

        private static async Task<int> RunMixedLoad(IDocumentStore store, string prefix, TimeSpan duration)
        {
            var stopAt = DateTime.UtcNow + duration;
            var transactions = 0;

            var writers = Enumerable.Range(0, 4).Select(writer => Task.Run(async () =>
            {
                for (var i = 0; DateTime.UtcNow < stopAt; i++)
                {
                    var options = new SessionOptions
                    {
                        TransactionMode = TransactionMode.ClusterWide,
                        DisableAtomicDocumentWritesInClusterWideTransaction = writer == 3 ? true : null
                    };
                    using (var session = store.OpenAsyncSession(options))
                    {
                        var id = $"{prefix}/{writer}/{i}";
                        switch (writer)
                        {
                            case 0: // one document
                            case 3: // one document, no atomic guard
                                await session.StoreAsync(new User { Name = id }, id);
                                break;
                            case 1: // three documents
                                for (var d = 0; d < 3; d++)
                                    await session.StoreAsync(new User { Name = id }, $"{id}/{d}");
                                break;
                            case 2: // new document, update and delete earlier ones
                                await session.StoreAsync(new User { Name = id }, id);
                                if (i >= 5)
                                {
                                    var earlier = await session.LoadAsync<User>($"{prefix}/{writer}/{i - 5}");
                                    if (earlier != null)
                                        earlier.Name += "/updated";
                                    session.Delete($"{prefix}/{writer}/{i - 4}");
                                }
                                break;
                        }

                        await session.SaveChangesAsync();
                        Interlocked.Increment(ref transactions);
                    }
                }
            })).ToArray();

            await Task.WhenAll(writers);
            return transactions;
        }

        private IDocumentStore GetStoreForNode(RavenServer node, string database)
        {
            return new DocumentStore
            {
                Urls = new[] { node.WebUrl },
                Database = database,
                Conventions = new DocumentConventions { DisableTopologyUpdates = true }
            }.Initialize();
        }

        private static bool HasNoStoredCommands(RavenServer node, string database)
        {
            using (node.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
                return ClusterTransactionCommand.ReadFirstClusterTransactionPreviousCount(context, database) == null;
        }

        private static async Task WriteClusterWide(IDocumentStore store, string prefix, int count, int documentsPerTransaction = 1)
        {
            for (var i = 0; i < count; i++)
            {
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    for (var d = 0; d < documentsPerTransaction; d++)
                        await session.StoreAsync(new User { Name = prefix }, $"{prefix}/{i}/{d}");
                    await session.SaveChangesAsync();
                }
            }
        }

        private static bool HasCounterAlertOnAnyNode(IEnumerable<RavenServer> nodes, string database) => nodes.Any(n => HasCounterAlert(n, database));

        private static string ReadCounterAlertMessageFromAnyNode(IEnumerable<RavenServer> nodes, string database)
        {
            var node = nodes.First(n => HasCounterAlert(n, database));
            return $"[on {node.ServerStore.NodeTag}] {ReadCounterAlertMessage(node, database)}";
        }

        private static string Diagnose(IEnumerable<RavenServer> nodes, string database)
        {
            var sb = new System.Text.StringBuilder();
            var leader = nodes.FirstOrDefault(n => n.ServerStore.IsLeader());
            sb.AppendLine($"leader: {leader?.ServerStore.NodeTag ?? "none"}");
            if (leader == null)
                return sb.ToString();

            var stats = leader.ServerStore.ClusterMaintenanceSupervisor.GetStats();
            using (leader.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                var first = ClusterTransactionCommand.ReadFirstClusterTransactionPreviousCount(context, database);
                var tree = context.Transaction.InnerTransaction.ReadTree(ClusterStateMachine.TransactionCommandsCountPerDatabase);
                var count = tree == null ? 0 : ClusterTransactionCommand.GetPrevCount(context, tree, database);
                sb.AppendLine($"leader stored commands: first position {first?.ToString() ?? "none"}, counter {count}");

                foreach (var (tag, nodeReport) in stats)
                {
                    if (nodeReport.Report.TryGetValue(database, out var r) == false)
                    {
                        sb.AppendLine($"{tag}: no report for the database");
                        continue;
                    }

                    var found = ClusterTransactionCommand.TryReadRaftIndexByPreviousCount(context, database, r.ExecutedClusterTransactionPreviousCount, out var atIndex);
                    var at = found ? atIndex : (long?)null;
                    var firstPosition = ClusterTransactionCommand.ReadFirstClusterTransactionPreviousCount(context, database, out var firstRaftIndex);
                    sb.AppendLine($"{tag}: status {r.Status}, executed index {r.ExecutedClusterTransactionRaftIndex}, position {r.ExecutedClusterTransactionPreviousCount}, " +
                                  $"next {r.ExecutedClusterTransactionNextCount}, group {r.DatabaseGroupId}; leader at that position: {at?.ToString() ?? "none"}, " +
                                  $"first stored: {firstPosition?.ToString() ?? "none"} (Raft index {firstRaftIndex})");
                }
            }

            foreach (var e in leader.ServerStore.Observer.ReadDecisionsForDatabase().List.Where(e => e.Database == database).TakeLast(5))
                sb.AppendLine($"decision: {e.Message}");

            return sb.ToString();
        }

        private static bool CountersReported(RavenServer leader, string database, IEnumerable<string> nodeTags)
        {
            var stats = leader.ServerStore.ClusterMaintenanceSupervisor.GetStats();
            foreach (var tag in nodeTags)
            {
                if (stats.TryGetValue(tag, out var nodeReport) == false ||
                    nodeReport.Report.TryGetValue(database, out var report) == false ||
                    report.ExecutedClusterTransactionRaftIndex == 0)
                    return false;
            }

            return true;
        }

        private static long StoredCommandsBacklog(RavenServer node, string database)
        {
            using (node.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                var first = ClusterTransactionCommand.ReadFirstClusterTransactionPreviousCount(context, database);
                if (first == null)
                    return 0;

                var tree = context.Transaction.InnerTransaction.ReadTree(ClusterStateMachine.TransactionCommandsCountPerDatabase);
                return ClusterTransactionCommand.GetPrevCount(context, tree, database) - first.Value;
            }
        }

        private static string CounterAlertId(string database) => AlertRaised.GetKey(AlertType.ClusterTransactionCounterMismatch, database);

        private static bool HasCounterAlert(RavenServer leader, string database) => leader.ServerStore.NotificationCenter.Storage.Exists(CounterAlertId(database));

        private static string ReadCounterAlertMessage(RavenServer leader, string database)
        {
            using (leader.ServerStore.NotificationCenter.Storage.Read(CounterAlertId(database), out var value))
            {
                value.Json.TryGet(nameof(AlertRaised.Message), out string message);
                return message;
            }
        }

        private static void RaiseClusterTransactionCounter(RavenServer node, string database, long delta)
        {
            using (node.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                var tree = tx.InnerTransaction.CreateTree(ClusterStateMachine.TransactionCommandsCountPerDatabase);
                using (ClusterTransactionCommand.GetPrefix(context, database, out Slice key))
                    tree.Increment(key, delta);
                tx.Commit();
            }
        }

        private static long ReadClusterTransactionCounter(RavenServer node, string database)
        {
            using (node.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                var tree = context.Transaction.InnerTransaction.ReadTree(ClusterStateMachine.TransactionCommandsCountPerDatabase);
                return tree == null ? 0 : ClusterTransactionCommand.GetPrevCount(context, tree, database);
            }
        }
    }
}
