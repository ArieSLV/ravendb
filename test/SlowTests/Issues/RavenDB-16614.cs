﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents.Replication;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16614 : ReplicationTestBase
    {
        public RavenDB_16614(ITestOutputHelper output) : base(output)
        {
        }

        protected override RavenServer GetNewServer(ServerCreationOptions options = null, [CallerMemberName] string caller = null)
        {
            if (options == null)
            {
                options = new ServerCreationOptions();
            }

            if (options.CustomSettings == null)
                options.CustomSettings = new Dictionary<string, string>();

            options.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.OperationTimeout)] = "60";
            options.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "10";
            options.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.TcpConnectionTimeout)] = "30000";

            return base.GetNewServer(options, caller);
        }
        private class User
        {
            public string Name;
        }

        [RavenTheory(RavenTestCategory.BackupExportImport | RavenTestCategory.Cluster)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task WillDeleteOrphanedAtomicGuards_AfterRestoreFromBackup(Options options)
        {
            var (_, leader) = await CreateRaftCluster(1);
            using var store = GetDocumentStore(new Options(options) {Server = leader, ReplicationFactor = 3});
            
            using (var session = store.OpenAsyncSession(new SessionOptions {TransactionMode = TransactionMode.ClusterWide}))
            {
                // this forces us to create an orphan!
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<AtomicGuard>(ClusterWideTransactionHelper.GetAtomicGuardKey("users/phoebe"),
                    new AtomicGuard {Id = "users/phoebe"}, 0));
                await session.StoreAsync(new User {Name = "arava"}, "users/arava");
                await session.SaveChangesAsync();
            }
            
            WaitForDocument(store, "users/arava");
            
            string tempFileName = GetTempFileName();
            try
            {
                var op = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), tempFileName, CancellationToken.None);
                await op.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                
                // we are simulating a scenario where we took a backup midway through removing an atomic guard

                using var store2 = GetDocumentStore(options, caller: store.Database + "_Restored");

                op = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), tempFileName, CancellationToken.None);
                await op.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                using (var session = store2.OpenAsyncSession(new SessionOptions {TransactionMode = TransactionMode.ClusterWide}))
                {
                    var val = await store2.Operations.SendAsync(
                        new GetCompareExchangeValueOperation<AtomicGuard>(ClusterWideTransactionHelper.GetAtomicGuardKey("users/phoebe")));
                    Assert.Null(val);

                    val = await store2.Operations.SendAsync(
                        new GetCompareExchangeValueOperation<AtomicGuard>(ClusterWideTransactionHelper.GetAtomicGuardKey("users/arava")));

                    var arava = await session.LoadAsync<User>("users/arava");
                    var cv = session.Advanced.GetChangeVectorFor(arava);
                    var cti = cv.ToChangeVectorList().Single(x => x.NodeTag == ChangeVectorParser.TrxnInt);
                    var record = await store2.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store2.Database));
                    Assert.Equal(val.Index, cti.Etag);
                }
            }
            finally
            {
                if (File.Exists(tempFileName))
                    File.Delete(tempFileName);
            }
        }

        private class AtomicGuard
        {
            public string Id;
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Cluster)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanHandleConflictsWithClusterTransactionIndex(Options options)
        {
            var (_, leader) = await CreateRaftCluster(1);
            using var store = GetDocumentStore(new Options(options) { Server = leader, ReplicationFactor = 3 });
            using var store2 = GetDocumentStore(new Options(options) { Server = leader, ReplicationFactor = 3 });

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new User { Name = "arava" }, "users/arava");
                await session.SaveChangesAsync();
            }

            using (var session2 = store2.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session2.StoreAsync(new User { Name = "Arava" }, "users/arava");
                await session2.SaveChangesAsync();
            }

            await SetupReplicationAsync(store, store2);
            await EnsureReplicatingAsync(store, store2);

            using (var session2 = store2.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var arava = await session2.LoadAsync<User>("users/arava");
                var cv = session2.Advanced.GetChangeVectorFor(arava);
                var cti = cv.ToChangeVectorList().Where(x => x.NodeTag == ChangeVectorParser.TrxnInt).ToList();
                Assert.Equal(2, cti.Count);
                Assert.Equal("Arava", arava.Name);
            }
        }

        [RavenTheory(RavenTestCategory.Cluster)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task WillMarkClusterWideDocumentsWithTransactionId(Options options)
        {
            var (_, leader) = await CreateRaftCluster(1);
            using var store = GetDocumentStore(new Options(options) { Server = leader, ReplicationFactor = 3 });

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new User { Name = "arava" }, "users/arava");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var arava = await session.LoadAsync<User>("users/arava");
                var metadata = session.Advanced.GetMetadataFor(arava);
                var cv = session.Advanced.GetChangeVectorFor(arava);
                var cti = cv.ToChangeVectorList().Single(x => x.NodeTag == ChangeVectorParser.TrxnInt);
                var guard = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<AtomicGuard>(ClusterWideTransactionHelper.GetAtomicGuardKey("users/arava")));
                Assert.Equal(cti.Etag, guard.Index);
            }
        }

        [RavenTheory(RavenTestCategory.Cluster)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task WillGetGoodErrorOnMismatchClusterTxId(Options options)
        {
            var (_, leader) = await CreateRaftCluster(1);
            using var store = GetDocumentStore(new Options(options) { Server = leader, ReplicationFactor = 3 });

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new User { Name = "arava" }, "users/arava");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var arava = await session.LoadAsync<User>("users/arava");

                using (var nested = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var arava2 = await nested.LoadAsync<User>("users/arava");
                    arava2.Name += "nested";
                    await nested.SaveChangesAsync();
                }

                arava.Name += "-modified";
                var err = await Assert.ThrowsAsync<ClusterTransactionConcurrencyException>(() => session.SaveChangesAsync());
                Assert.Contains("Failed to execute cluster transaction due to the following issues: " +
                    "Guard compare exchange value 'rvn-atomic/users/arava' index does not match ", err.Message);
            }
        }

        [RavenTheory(RavenTestCategory.Cluster)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task WillFailNormalTransactionThatDoesNotMatchAtomicGuardIndex(Options options)
        {
            var (_, leader) = await CreateRaftCluster(1);
            using var store = GetDocumentStore(new Options(options) { Server = leader, ReplicationFactor = 3 });

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new User { Name = "arava" }, "users/arava");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions
            {
                TransactionMode = TransactionMode.SingleNode // important, NOT a cluster wide transaction
            }))
            {
                var arava = await session.LoadAsync<User>("users/arava");

                using (var nested = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var arava2 = await nested.LoadAsync<User>("users/arava");
                    arava2.Name += "nested";
                    await nested.SaveChangesAsync();
                }

                arava.Name += "-modified";
                await Assert.ThrowsAsync<ConcurrencyException>(() => session.SaveChangesAsync());
            }
        }

        [RavenTheory(RavenTestCategory.CompareExchange | RavenTestCategory.Cluster)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanDeleteCmpXchgValue(Options options)
        {
            var (_, leader) = await CreateRaftCluster(1);
            using var store = GetDocumentStore(new Options(options) { Server = leader, ReplicationFactor = 3 });

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new User { Name = "arava" }, "users/arava");
                await session.SaveChangesAsync();
            }
            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                session.Delete("users/arava");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var arava = await session.LoadAsync<User>("users/arava");
                Assert.Null(arava);
                var guard = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<object>("rvn-atomic-guard/users/arava");
                Assert.Null(guard);
            }
        }

        [RavenTheory(RavenTestCategory.Cluster)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanModifyDocumentAfterFirstTime(Options options)
        {
            var (_, leader) = await CreateRaftCluster(1);
            using var store = GetDocumentStore(new Options(options) { Server = leader, ReplicationFactor = 3 });

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new User { Name = "arava" }, "users/arava");
                await session.StoreAsync(new User { Name = "phoebe" }, "users/phoebe");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var arava = await session.LoadAsync<User>("users/arava");
                arava.Name = "Arava Eini";
                var phoebe = await session.LoadAsync<User>("users/phoebe");
                phoebe.Name = "Phoebe Eini";
                await session.SaveChangesAsync();
            }
        }

        [RavenTheory(RavenTestCategory.Cluster)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ModificationInAnotherTransactionWillFail(Options options)
        {
            var (_, leader) = await CreateRaftCluster(1);
            using var store = GetDocumentStore(new Options(options) { Server = leader, ReplicationFactor = 3 });

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new User { Name = "arava" }, "users/arava");
                await session.StoreAsync(new User { Name = "phoebe" }, "users/phoebe");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var user = await session.LoadAsync<User>("users/arava");
                user.Name = "Arava Eini";
                var user2 = await session.LoadAsync<User>("users/phoebe");
                user2.Name = "Phoebe Eini";

                using (var conflictedSession = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var conflictedArava = await conflictedSession.LoadAsync<User>("users/arava");
                    conflictedArava.Name = "Arava!";
                    await conflictedSession.SaveChangesAsync();
                }

                await Assert.ThrowsAsync<ClusterTransactionConcurrencyException>(() => session.SaveChangesAsync());
            }
        }

        [RavenTheory(RavenTestCategory.Cluster)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ModificationInAnotherTransactionWillFailWithDelete(Options options)
        {
            var (_, leader) = await CreateRaftCluster(1);
            using var store = GetDocumentStore(new Options(options) { Server = leader, ReplicationFactor = 3 });

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new User { Name = "arava" }, "users/arava");
                await session.StoreAsync(new User { Name = "phoebe" }, "users/phoebe");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var user = await session.LoadAsync<User>("users/arava");
                session.Delete(user);
                var user2 = await session.LoadAsync<User>("users/phoebe");
                user2.Name = "Phoebe Eini";

                using (var conflictedSession = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var conflictedArava = await conflictedSession.LoadAsync<User>("users/arava");
                    conflictedArava.Name = "Arava!";
                    await conflictedSession.SaveChangesAsync();
                }

                await Assert.ThrowsAsync<ClusterTransactionConcurrencyException>(() => session.SaveChangesAsync());
            }
        }
    }
}
