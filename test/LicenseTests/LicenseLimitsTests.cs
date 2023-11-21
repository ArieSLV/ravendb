using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using LicenseTests.Fixtures;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes.Analysis;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.ServerWide.Commands.Cluster;
using Raven.Client.ServerWide.Operations.Analyzers;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.ServerWide.Operations.OngoingTasks;
using Raven.Client.ServerWide.Operations.Sorters;
using Raven.Server.Config;
using Raven.Server.ServerWide.Context;
using SlowTests.Issues;
using Sparrow.Json;
using Sparrow.Utils;
using Xunit;
using Xunit.Abstractions;

namespace LicenseTests
{
    public class LicenseLimitsTests : LicenseLimitsTestsBase
    {

        static LicenseLimitsTests()
        {
            IgnoreProcessorAffinityChanges(ignore: false);
        }

        public LicenseLimitsTests(ITestOutputHelper output) : base(output)
        {

        }

        [Fact]
        public async Task WillUtilizeAllAvailableCores()
        {
            var server = GetNewServer(new ServerCreationOptions
            {
                RunInMemory = false
            });

            await server.ServerStore.EnsureNotPassiveAsync();

            await server.ServerStore.LicenseManager.ChangeLicenseLimits(server.ServerStore.NodeTag, 1, Guid.NewGuid().ToString());
            var licenseLimits = server.ServerStore.LoadLicenseLimits();
            Assert.True(licenseLimits.NodeLicenseDetails.TryGetValue(server.ServerStore.NodeTag, out var detailsPerNode));
            Assert.True(detailsPerNode.UtilizedCores == 1, "detailsPerNode.UtilizedCores == 1");

            // Taking down server
            var result = await DisposeServerAndWaitForFinishOfDisposalAsync(server);
            var settings = new Dictionary<string, string>
            {
                {RavenConfiguration.GetKey(x => x.Core.ServerUrls), result.Url}
            };

            // Bring server up
            server = GetNewServer(
                new ServerCreationOptions { RunInMemory = false, DeletePrevious = false, DataDirectory = result.DataDirectory, CustomSettings = settings });

            licenseLimits = server.ServerStore.LoadLicenseLimits();
            Assert.True(licenseLimits.NodeLicenseDetails.TryGetValue(server.ServerStore.NodeTag, out detailsPerNode));
            Assert.True(detailsPerNode.UtilizedCores == 1, "detailsPerNode.UtilizedCores == 1");

            await server.ServerStore.LicenseManager.ChangeLicenseLimits(server.ServerStore.NodeTag, null, Guid.NewGuid().ToString());
            licenseLimits = server.ServerStore.LoadLicenseLimits();
            Assert.True(licenseLimits.NodeLicenseDetails.TryGetValue(server.ServerStore.NodeTag, out detailsPerNode));
            Assert.True(detailsPerNode.UtilizedCores == ProcessorInfo.ProcessorCount, $"detailsPerNode.UtilizedCores == {ProcessorInfo.ProcessorCount}");
        }

        [Fact]
        public async Task WillUtilizeAllAvailableCoresInACluster()
        {
            DoNotReuseServer();

            var (servers, leader) = await CreateRaftCluster(5);
            await leader.ServerStore.EnsureNotPassiveAsync();

            foreach (var server in servers)
            {
                await server.ServerStore.LicenseManager.ChangeLicenseLimits(server.ServerStore.NodeTag, 1, Guid.NewGuid().ToString());

                var licenseLimits = server.ServerStore.LoadLicenseLimits();
                Assert.True(licenseLimits.NodeLicenseDetails.TryGetValue(server.ServerStore.NodeTag, out var detailsPerNode),
                    "license.NodeLicenseDetails.TryGetValue(tag, out var detailsPerNode)");
                Assert.Equal(1, detailsPerNode.UtilizedCores);
            }

            var seenNodeTags = new HashSet<string>();
            foreach (var server in servers)
            {
                await server.ServerStore.LicenseManager.ChangeLicenseLimits(server.ServerStore.NodeTag, null, Guid.NewGuid().ToString());
                seenNodeTags.Add(server.ServerStore.NodeTag);

                var licenseLimits = server.ServerStore.LoadLicenseLimits();
                Assert.True(licenseLimits.NodeLicenseDetails.TryGetValue(server.ServerStore.NodeTag, out var detailsPerNode),
                    "license.NodeLicenseDetails.TryGetValue(tag, out var detailsPerNode)");
                Assert.Equal(ProcessorInfo.ProcessorCount, detailsPerNode.UtilizedCores);

                var notChangedServers = servers.Select(x => x.ServerStore).Where(x => seenNodeTags.Contains(x.NodeTag) == false);
                foreach (var notChangedServer in notChangedServers)
                {
                    licenseLimits = notChangedServer.LoadLicenseLimits();
                    Assert.True(licenseLimits.NodeLicenseDetails.TryGetValue(notChangedServer.NodeTag, out detailsPerNode),
                        "license.NodeLicenseDetails.TryGetValue(tag, out var detailsPerNode)");
                    Assert.Equal(1, detailsPerNode.UtilizedCores);
                }
            }
        }

        [Fact]
        public async Task UtilizedCoresShouldNotChangeAfterRestart()
        {
            var server = GetNewServer(new ServerCreationOptions
            {
                RunInMemory = false
            });

            using (GetDocumentStore(new Options
            {
                Server = server,
                Path = Path.Combine(server.Configuration.Core.DataDirectory.FullPath, "UtilizedCoresShouldNotChangeAfterRestart")
            }))
            {
                await server.ServerStore.LicenseManager.ChangeLicenseLimits(server.ServerStore.NodeTag, 1, Guid.NewGuid().ToString());
                var license = server.ServerStore.LoadLicenseLimits();
                Assert.True(license.NodeLicenseDetails.TryGetValue(server.ServerStore.NodeTag, out var detailsPerNode));
                Assert.True(detailsPerNode.UtilizedCores == 1, "detailsPerNode.UtilizedCores == 1");

                // Taking down server
                var result = await DisposeServerAndWaitForFinishOfDisposalAsync(server);
                var settings = new Dictionary<string, string>
                {
                    { RavenConfiguration.GetKey(x => x.Core.ServerUrls), result.Url }
                };

                // Bring server up
                server = GetNewServer(new ServerCreationOptions { RunInMemory = false, DeletePrevious = false, DataDirectory = result.DataDirectory, CustomSettings = settings });

                license = server.ServerStore.LoadLicenseLimits();
                Assert.True(license.NodeLicenseDetails.TryGetValue(server.ServerStore.NodeTag, out detailsPerNode));
                Assert.True(detailsPerNode.UtilizedCores == 1, "detailsPerNode.UtilizedCores == 1");
            }
        }

        [Fact]
        public async Task DemotePromoteShouldNotChangeTheUtilizedCores()
        {
            DoNotReuseServer();

            var reasonableTime = Debugger.IsAttached ? 5000 : 3000;
            var (servers, leader) = await CreateRaftCluster(3);

            using (var store = GetDocumentStore(new Options
            {
                CreateDatabase = true,
                ReplicationFactor = 3,
                Server = leader
            }))
            {
                foreach (var server in servers)
                {
                    await server.ServerStore.LicenseManager.ChangeLicenseLimits(server.ServerStore.NodeTag, 1, Guid.NewGuid().ToString());

                    var license = server.ServerStore.LoadLicenseLimits();
                    Assert.True(license.NodeLicenseDetails.TryGetValue(server.ServerStore.NodeTag, out var detailsPerNode), $"license.NodeLicenseDetails.TryGetValue(tag:{server.ServerStore.NodeTag}, out var detailsPerNode:{detailsPerNode})");
                    Assert.True(detailsPerNode.UtilizedCores == 1, $"detailsPerNode.UtilizedCores:{detailsPerNode.UtilizedCores} == 1");
                }

                foreach (var tag in servers.Select(x => x.ServerStore.NodeTag).Where(x => x != leader.ServerStore.NodeTag))
                {
                    var re = store.GetRequestExecutor(store.Database);
                    using (re.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                    {
                        await re.ExecuteAsync(new DemoteClusterNodeCommand(tag), context);
                        await Task.Delay(reasonableTime);

                        using (leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                        {
                            ctx.OpenReadTransaction();
                            var topology = leader.ServerStore.GetClusterTopology(ctx);
                            Assert.True(topology.Watchers.ContainsKey(tag), $"topology.Watchers.ContainsKey(tag:{tag})");
                        }

                        var license = leader.ServerStore.LoadLicenseLimits();
                        Assert.True(license.NodeLicenseDetails.TryGetValue(tag, out var detailsPerNode), $"license.NodeLicenseDetails.TryGetValue(tag:{tag}, out var detailsPerNode:{detailsPerNode})");
                        Assert.True(detailsPerNode.UtilizedCores == 1, $"detailsPerNode.UtilizedCores:{detailsPerNode.UtilizedCores} == 1");

                        await re.ExecuteAsync(new PromoteClusterNodeCommand(tag), context);
                        await Task.Delay(reasonableTime);

                        using (leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                        {
                            ctx.OpenReadTransaction();
                            var topology = leader.ServerStore.GetClusterTopology(ctx);
                            Assert.True(topology.Watchers.ContainsKey(tag) == false, $"topology.Watchers.ContainsKey(tag:{tag}) == false");
                        }

                        license = leader.ServerStore.LoadLicenseLimits();
                        Assert.True(license.NodeLicenseDetails.TryGetValue(tag, out detailsPerNode), $"license.NodeLicenseDetails.TryGetValue(tag:{tag}, out detailsPerNode:{detailsPerNode})");
                        Assert.True(detailsPerNode.UtilizedCores == 1, $"detailsPerNode.UtilizedCores:{detailsPerNode.UtilizedCores} == 1");
                    }
                }
            }
        }














        private readonly PutServerWideBackupConfigurationOperation _putServerWideBackupConfigurationOperation =
            new(new ServerWideBackupConfiguration { FullBackupFrequency = "* * * * *", Disabled = true });

        [LicenseWithAllFeaturesDisabledRequiredFact]
        public async Task ServerWideBackups_Can_SwitchToLicenseWithRestriction()
        {
            var fixture = LicenseLimitsOperationsTestFixtureBuilder<PutServerWideBackupConfigurationOperation, PutServerWideBackupConfigurationResponse>.Init(this)
                .WithPutOperation(() => _putServerWideBackupConfigurationOperation)
                .Build();

            await Assert_Success_ExecutePut_ServerOperation(fixture);
            await Assert_Success_SwitchToLicenseWithRestrictions(fixture);

            await Assert_Equal(fixture,
                expectedValue: 1,
                actualValue: databaseRecord => databaseRecord.PeriodicBackups.Count);
        }

        [Fact]
        public async Task ServerWideBackups_Fail_IfLicenseOptionDisabled()
        {
            var fixture = LicenseLimitsOperationsTestFixtureBuilder<PutServerWideBackupConfigurationOperation, PutServerWideBackupConfigurationResponse>.Init(this, licenseWithFeatureEnabled: false)
                .WithPutOperation(() => _putServerWideBackupConfigurationOperation)
                .Build();

            await Assert_Throw_ExecutePut_ServerOperation(fixture);

            await Assert_Equal(fixture,
                expectedValue: 0,
                actualValue: databaseRecord => databaseRecord.PeriodicBackups.Count);
        }

        private static PutServerWideExternalReplicationOperation GetPutServerWideExternalReplicationOperation(IDocumentStore store) =>
            new(new ServerWideExternalReplication { Disabled = true, TopologyDiscoveryUrls = new[] { store.Urls.First() } });

        [Fact]
        public async Task ServerWideExternalReplication_ThrowOnSwitchToLicenseWithFeatureDisabled()
        {
            var fixture = LicenseLimitsOperationsTestFixtureBuilder<PutServerWideExternalReplicationOperation, ServerWideExternalReplicationResponse>.Init(this)
                .WithPutOperation(GetPutServerWideExternalReplicationOperation)
                .Build();

            await Assert_Success_ExecutePut_ServerOperation(fixture);
            await Assert_Fail_SwitchToLicenseWithRestrictions(fixture);

            await Assert_Equal(fixture,
                expectedValue: 1,
                actualValue: databaseRecord => databaseRecord.ExternalReplications.Count);
        }

        [Fact]
        public async Task ServerWideExternalReplication_Fail_IfLicenseOptionDisabled()
        {
            var fixture = LicenseLimitsOperationsTestFixtureBuilder<PutServerWideExternalReplicationOperation, ServerWideExternalReplicationResponse>.Init(this)
                .WithPutOperation(GetPutServerWideExternalReplicationOperation)
                .WithLicenseFeatureDisabled()
                .Build();
            
            await Assert_Throw_ExecutePut_ServerOperation(fixture);

            await Assert_Equal(fixture,
                expectedValue: 0,
                actualValue: databaseRecord => databaseRecord.ExternalReplications.Count);
        }
        
        private readonly PutServerWideSortersOperation _putServerWideSortersOperation =
            new(new SorterDefinition { Name = "sorterName", Code = GetResource("MySorter.cs", "MySorter", "sorterName") });
        
        [Fact]
        public async Task ServerWideCustomSorters_Can_SwitchToLicenseWithRestriction()
        {
            var fixture = LicenseLimitsOperationsTestFixtureBuilder<PutServerWideSortersOperation>.Init(this)
                .WithPutOperation(() => _putServerWideSortersOperation)
                .Build();

            await Assert_Success_ExecutePut_ServerOperation(fixture);
            await Assert_Success_SwitchToLicenseWithRestrictions(fixture);

            Assert_Equal(fixture,
                expectedValue: 1,
                actualValue: GetServerWideCustomSortersCount);
        }
        
        [Fact]
        public async Task ServerWideCustomSorters_Fail_IfLicenseOptionDisabled()
        {
            var fixture = LicenseLimitsOperationsTestFixtureBuilder<PutServerWideSortersOperation>.Init(this)
                .WithPutOperation(() => _putServerWideSortersOperation)
                .WithLicenseFeatureDisabled()
                .Build();

            await Assert_Throw_ExecutePut_ServerOperation(fixture);

            Assert_Equal(fixture,
                expectedValue: 0,
                actualValue: GetServerWideCustomSortersCount);
        }

        private readonly PutServerWideAnalyzersOperation _putServerWideAnalyzersOperation =
            new(new AnalyzerDefinition { Name = "analyzerName", Code = GetResource("MyAnalyzer.cs", "MyAnalyzer", "analyzerName") });

        [Fact]
        public async Task ServerWideAnalyzers_Can_SwitchToLicenseWithRestriction()
        {
            var fixture = LicenseLimitsOperationsTestFixtureBuilder<PutServerWideAnalyzersOperation>.Init(this)
                .WithPutOperation(() => _putServerWideAnalyzersOperation)
                .Build();

            await Assert_Success_ExecutePut_ServerOperation(fixture);
            await Assert_Success_SwitchToLicenseWithRestrictions(fixture);

            Assert_Equal(fixture,
                expectedValue: 1,
                actualValue: GetServerWideAnalyzerCount);
        }

        [Fact]
        public async Task ServerWideAnalyzers_Fail_IfLicenseOptionDisabled()
        {
            var fixture = LicenseLimitsOperationsTestFixtureBuilder<PutServerWideAnalyzersOperation>.Init(this)
                .WithPutOperation(() => _putServerWideAnalyzersOperation)
                .WithLicenseFeatureDisabled()
                .Build();

            await Assert_Throw_ExecutePut_ServerOperation(fixture);

            Assert_Equal(fixture,
                expectedValue: 0,
                actualValue: GetServerWideAnalyzerCount);
        }

        private readonly UpdatePeriodicBackupOperation _updatePeriodicBackupOperation =
            new(new PeriodicBackupConfiguration { BackupType = BackupType.Backup, FullBackupFrequency = "0 0 1 1 *", Disabled = true });

        [Fact]
        public async Task PeriodicBackup_Can_SwitchToLicenseWithRestriction()
        {
            var fixture = LicenseLimitsOperationsTestFixtureBuilder<UpdatePeriodicBackupOperation, UpdatePeriodicBackupOperationResult>.Init(this)
                .WithPutOperation(() => _updatePeriodicBackupOperation)
                .Build();

            await Assert_Success_ExecutePut_MaintenanceOperation(fixture);
            await Assert_Success_SwitchToLicenseWithRestrictions(fixture);

            await Assert_Equal(fixture,
                expectedValue: 1,
                actualValue: databaseRecord => databaseRecord.PeriodicBackups.Count);
        }

        [Fact]
        public async Task PeriodicBackup_Fail_IfLicenseOptionDisabled()
        {
            var fixture = LicenseLimitsOperationsTestFixtureBuilder<UpdatePeriodicBackupOperation, UpdatePeriodicBackupOperationResult>.Init(this)
                .WithPutOperation(() => _updatePeriodicBackupOperation)
                .WithLicenseFeatureDisabled()
                .Build();

            await Assert_Throw_ExecutePut_MaintenanceOperation(fixture);

            await Assert_Equal(fixture,
                expectedValue: 0,
                actualValue: databaseRecord => databaseRecord.PeriodicBackups.Count);
        }

        private const int ExpectedMaxNumberOfRequestsPerSession = 101;
        private readonly PutClientConfigurationOperation _putClientConfigurationOperation =
            new(new ClientConfiguration { ReadBalanceBehavior = ReadBalanceBehavior.RoundRobin, MaxNumberOfRequestsPerSession = ExpectedMaxNumberOfRequestsPerSession, Disabled = false });

        [Fact]
        public async Task ClientConfiguration_Can_SwitchToLicenseWithRestriction()
        {
            var fixture = LicenseLimitsOperationsTestFixtureBuilder<PutClientConfigurationOperation>.Init(this)
                .WithPutOperation(() => _putClientConfigurationOperation)
                .Build();

            await Assert_Success_ExecutePut_MaintenanceOperation(fixture);
            await Assert_Success_SwitchToLicenseWithRestrictions(fixture);

            Assert_Equal(fixture,
                expectedValue: ExpectedMaxNumberOfRequestsPerSession,
                actualValue: store =>
                {
                    using var session = store.OpenSession();
                    session.Load<dynamic>("users/1");
                    return store.GetRequestExecutor().Conventions.MaxNumberOfRequestsPerSession;
                });
        }

        [Fact]
        public async Task ClientConfiguration_Fail_IfLicenseOptionDisabled()
        {
            var fixture = LicenseLimitsOperationsTestFixtureBuilder<PutClientConfigurationOperation>.Init(this)
                .WithPutOperation(() => _putClientConfigurationOperation)
                .WithLicenseFeatureDisabled()
                .Build();

            await Assert_Throw_ExecutePut_MaintenanceOperation(fixture);

            Assert_Equal(fixture,
                expectedValue: DocumentConventions.Default.MaxNumberOfRequestsPerSession,
                actualValue: store =>
                {
                    using var session = store.OpenSession();
                    session.Load<dynamic>("users/1");
                    return store.GetRequestExecutor().Conventions.MaxNumberOfRequestsPerSession;
                });
        }

        private readonly RavenDB_10546.PutServerWideStudioConfigurationOperation _putServerWideStudioConfigurationOperation =
            new(new ServerWideStudioConfiguration { ReplicationFactor = 2 });

        [Fact]
        public async Task ServerWideStudioConfiguration_Can_SwitchToLicenseWithRestriction()
        {
            var fixture = LicenseLimitsOperationsTestFixtureBuilder<RavenDB_10546.PutServerWideStudioConfigurationOperation>.Init(this)
                .WithPutOperation(() => _putServerWideStudioConfigurationOperation)
                .Build();

            await Assert_Success_ExecutePut_ServerOperation(fixture);
            await Assert_Success_SwitchToLicenseWithRestrictions(fixture);

            Assert_Equal(fixture,
                expectedValue: 2,
                actualValue: store =>
            {
                var serverWideStudioConfiguration = store.Maintenance.Server.Send(new RavenDB_10546.GetServerWideStudioConfigurationOperation());
                return serverWideStudioConfiguration.ReplicationFactor ?? -1;
            });
        }

        [Fact]
        public async Task ServerWideStudioConfiguration_Fail_IfLicenseOptionDisabled()
        {
            var fixture = LicenseLimitsOperationsTestFixtureBuilder<RavenDB_10546.PutServerWideStudioConfigurationOperation>.Init(this)
                .WithPutOperation(() => _putServerWideStudioConfigurationOperation)
                .WithLicenseFeatureDisabled()
                .Build();

            await Assert_Throw_ExecutePut_ServerOperation(fixture);

            Assert_Equal(fixture,
                expectedValue: -1,
                actualValue: store =>
            {
                var serverWideStudioConfiguration = store.Maintenance.Server.Send(new RavenDB_10546.GetServerWideStudioConfigurationOperation());
                return serverWideStudioConfiguration?.ReplicationFactor ?? -1;
            });
        }

        private readonly RavenDB_10546.PutStudioConfigurationOperation _putStudioConfigurationOperation =
            new(new StudioConfiguration { Environment = StudioConfiguration.StudioEnvironment.Production });

        [Fact]
        public async Task StudioConfiguration_Can_SwitchToLicenseWithRestriction()
        {
            var fixture = LicenseLimitsOperationsTestFixtureBuilder<RavenDB_10546.PutStudioConfigurationOperation>.Init(this)
                .WithPutOperation(() => _putStudioConfigurationOperation)
                .Build();

            await Assert_Success_ExecutePut_MaintenanceOperation(fixture);
            await Assert_Success_SwitchToLicenseWithRestrictions(fixture);

            Assert_Equal(fixture,
                expectedValue: (int)StudioConfiguration.StudioEnvironment.Production,
                actualValue: store =>
                {
                    var studioConfiguration = store.Maintenance.Send(new GetStudioConfigurationOperation());;
                    return studioConfiguration != null ? (int)studioConfiguration.Environment : -1;
                });
        }

        [Fact]
        public async Task StudioConfiguration_Fail_IfLicenseOptionDisabled()
        {
            var fixture = LicenseLimitsOperationsTestFixtureBuilder<RavenDB_10546.PutStudioConfigurationOperation>.Init(this)
                .WithPutOperation(() => _putStudioConfigurationOperation)
                .WithLicenseFeatureDisabled()
                .Build();

            await Assert_Throw_ExecutePut_MaintenanceOperation(fixture);

            Assert_Equal(fixture,
                expectedValue: -1,
                actualValue: store =>
                {
                    var studioConfiguration = store.Maintenance.Send(new GetStudioConfigurationOperation());;
                    return studioConfiguration != null ? (int)studioConfiguration.Environment : -1;
                });
        }

        [Fact]
        public async Task QueueSink_Kafka_Can_Put_Than_SwitchToLicenseWithRestriction()
        {
            var fixture = LicenseLimitsOperationsTestFixtureBuilder<AddQueueSinkOperation<QueueConnectionString>, AddQueueSinkOperationResult>.Init(this)
                .WithPutOperation(store => GetAddQueueSinkOperation(store, QueueBrokerType.Kafka))
                .Build();

            await Assert_Success_ExecutePut_MaintenanceOperation(fixture);
            await Assert_Success_SwitchToLicenseWithRestrictions(fixture);

            await Assert_Equal(fixture,
                expectedValue: 1,
                actualValue: databaseRecord => databaseRecord.QueueSinks.Count);
        }

        [Fact]
        public async Task QueueSink_Kafka_FailPut_IfLicenseOptionDisabled()
        {
            var fixture = LicenseLimitsOperationsTestFixtureBuilder<AddQueueSinkOperation<QueueConnectionString>, AddQueueSinkOperationResult>.Init(this)
                .WithPutOperation(store => GetAddQueueSinkOperation(store, QueueBrokerType.Kafka))
                .WithLicenseFeatureDisabled()
                .Build();

            await Assert_Throw_ExecutePut_MaintenanceOperation(fixture);

            await Assert_Equal(fixture,
                expectedValue: 0,
                actualValue: databaseRecord => databaseRecord.QueueSinks.Count);
        }

        [Fact]
        public async Task QueueSink_RabbitMq_Can_Put_Than_SwitchToLicenseWithRestriction()
        {
            var fixture = LicenseLimitsOperationsTestFixtureBuilder<AddQueueSinkOperation<QueueConnectionString>, AddQueueSinkOperationResult>.Init(this)
                .WithPutOperation(store => GetAddQueueSinkOperation(store, QueueBrokerType.RabbitMq))
                .Build();

            await Assert_Success_ExecutePut_MaintenanceOperation(fixture);
            await Assert_Success_SwitchToLicenseWithRestrictions(fixture);

            await Assert_Equal(fixture,
                expectedValue: 1,
                actualValue: databaseRecord => databaseRecord.QueueSinks.Count);
        }

        [Fact]
        public async Task QueueSink_RabbitMq_FailPut_IfLicenseOptionDisabled()
        {
            var fixture = LicenseLimitsOperationsTestFixtureBuilder<AddQueueSinkOperation<QueueConnectionString>, AddQueueSinkOperationResult>.Init(this)
                .WithPutOperation(store => GetAddQueueSinkOperation(store, QueueBrokerType.RabbitMq))
                .WithLicenseFeatureDisabled()
                .Build();

            await Assert_Throw_ExecutePut_MaintenanceOperation(fixture);

            await Assert_Equal(fixture,
                expectedValue: 0,
                actualValue: databaseRecord => databaseRecord.QueueSinks.Count);
        }

        [Fact]
        public async Task QueueSink_RabbitMq_CanUpdate_IfLicenseOptionEnabled()
        {
            var fixture = LicenseLimitsOperationsTestFixtureBuilder<AddQueueSinkOperation<QueueConnectionString>, AddQueueSinkOperationResult>.Init(this)
                .WithPutOperation(store => GetAddQueueSinkOperation(store, QueueBrokerType.RabbitMq))
                .Build();

            await Assert_Success_ExecutePut_MaintenanceOperation(fixture);
            var updateOperation = GetUpdateQueueSinkOperation(fixture.Store, fixture.PutResult.TaskId, QueueBrokerType.RabbitMq);
            await Assert_Success_ExecuteUpdate_MaintenanceOperation<UpdateQueueSinkOperation<QueueConnectionString>, UpdateQueueSinkOperationResult>(fixture.Store, updateOperation);

            await Assert_Equal(fixture,
                expectedValue: 1,
                actualValue: databaseRecord => databaseRecord.QueueSinks.Count);
        }

        [Fact]
        public async Task QueueSink_RabbitMq_FailUpdate_IfLicenseOptionDisabled()
        {
            var fixture = LicenseLimitsOperationsTestFixtureBuilder<AddQueueSinkOperation<QueueConnectionString>, AddQueueSinkOperationResult>.Init(this)
                .WithPutOperation(store => GetAddQueueSinkOperation(store, QueueBrokerType.RabbitMq))
                .Build();

            await Assert_Success_ExecutePut_MaintenanceOperation(fixture);
            await Assert_Success_SwitchToLicenseWithRestrictions(fixture);

            var updateOperation = GetUpdateQueueSinkOperation(fixture.Store, fixture.PutResult.TaskId, QueueBrokerType.RabbitMq);
            await Assert_Throw_ExecuteUpdate_MaintenanceOperation<UpdateQueueSinkOperation<QueueConnectionString>, UpdateQueueSinkOperationResult>(fixture.Store, updateOperation);

            await Assert_Equal(fixture,
                expectedValue: 1,
                actualValue: databaseRecord => databaseRecord.QueueSinks.Count);
        }

        [Fact]
        public async Task DataArchival_Can_Put_Than_SwitchToLicenseWithRestriction()
        {
            var fixture = LicenseLimitsOperationsTestFixtureBuilder<ConfigureDataArchivalOperation, ConfigureDataArchivalOperationResult>.Init(this)
                .WithPutOperation(GetConfigureDataArchivalOperation)
                .Build();

            await Assert_Success_ExecutePut_MaintenanceOperation(fixture);
            await Assert_Success_SwitchToLicenseWithRestrictions(fixture);

            await Assert_Equal(fixture,
                expectedValue: false,
                actualValue: databaseRecord => databaseRecord.DataArchival.Disabled);
        }

        [Fact]
        public async Task DataArchival_FailPut_IfLicenseOptionDisabled()
        {
            var fixture = LicenseLimitsOperationsTestFixtureBuilder<ConfigureDataArchivalOperation, ConfigureDataArchivalOperationResult>.Init(this)
                .WithPutOperation(GetConfigureDataArchivalOperation)
                .WithLicenseFeatureDisabled()
                .Build();

            await Assert_Throw_ExecutePut_MaintenanceOperation(fixture);

            await Assert_Equal(fixture,
                expectedValue: true,
                actualValue: databaseRecord => databaseRecord.DataArchival == null);
        }

        private static readonly SubscriptionCreationOptions RevisionsInSubscriptionCreationOptions = new() { Query = $"from People {DocumentSubscriptions.IncludeRevisionsRQL}" };

        [Fact]
        public async Task RevisionsInSubscriptions_Can_Put_Than_SwitchToLicenseWithRestriction()
        {
            var fixture = LicenseLimitsSubscriptionsTestFixtureBuilder.Init(this)
                .WithCreateOptions(() => RevisionsInSubscriptionCreationOptions)
                .Build();

            await Assert_Success_CreateSubscription(fixture);
            await Assert_Success_SwitchToLicenseWithRestrictions(fixture);

            await Assert_Equal(fixture,
                expectedValue: 1,
                actualValue: async store => (await store.Subscriptions.GetSubscriptionsAsync(0, int.MaxValue)).Count);
        }

        [Fact]
        public async Task RevisionsInSubscriptions_FailPut_IfLicenseOptionDisabled()
        {
            var fixture = LicenseLimitsSubscriptionsTestFixtureBuilder.Init(this)
                .WithCreateOptions(() => RevisionsInSubscriptionCreationOptions)
                .WithLicenseFeatureDisabled()
                .Build();

            await Assert_Throw_CreateSubscription(fixture);

            await Assert_Equal(fixture,
                expectedValue: 0,
                actualValue: async store => (await store.Subscriptions.GetSubscriptionsAsync(0, int.MaxValue)).Count);
        }

        private static readonly SubscriptionUpdateOptions RevisionInSubscriptionUpdateOptions = new() { Query = RevisionsInSubscriptionCreationOptions.Query };



        [Fact]
        public async Task RevisionsInSubscriptions_Fail_UpdateSubscription_WithRevisions()
        {
            var fixture = LicenseLimitsSubscriptionsTestFixtureBuilder.Init(this)
                .WithCreateOptions(() => RevisionsInSubscriptionCreationOptions)
                .WithUpdatedOptions(() => RevisionInSubscriptionUpdateOptions)
                .WithLicenseFeatureDisabled()
                .Build();

            await Assert_Throw_CreateSubscription(fixture); // With revisions in Subscription

            fixture.SubscriptionCreationOptions = new SubscriptionCreationOptions { Query = "from People" };
            Assert.False(fixture.SubscriptionCreationOptions.Query.Contains(DocumentSubscriptions.IncludeRevisionsRQL));

            await Assert_Success_CreateSubscription(fixture); // Without revision in Subscription
            await Assert_Fail_UpdateSubscription(fixture); // With revisions in Subscription

            await Assert_Equal(fixture,
                expectedValue: false,
                actualValue: async store =>
                {
                    var subscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, int.MaxValue);
                    Assert.Single(subscriptions);
                    return subscriptions[0].Query.Contains(DocumentSubscriptions.IncludeRevisionsRQL);
                });
        }

        [Fact]
        public async Task RevisionsInSubscriptions_Can_UpdateSubscription_WithoutRevisions()
        {
            var fixture = LicenseLimitsSubscriptionsTestFixtureBuilder.Init(this)
                .WithCreateOptions(() => RevisionsInSubscriptionCreationOptions)
                .WithUpdatedOptions(() => new SubscriptionUpdateOptions { Query = "from Orders" })
                .Build();

            await Assert_Success_CreateSubscription(fixture); // With revision in Subscription
            await Assert_Success_SwitchToLicenseWithRestrictions(fixture);

            await Assert_Success_UpdateSubscription(fixture); // Without revisions in Subscription

            await Assert_Equal(fixture,
                expectedValue: true,
                actualValue: async store =>
                {
                    var subscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, int.MaxValue);
                    Assert.Single(subscriptions);
                    return
                        subscriptions[0].Query.Contains("from Orders") &&
                        subscriptions[0].Query.Contains(DocumentSubscriptions.IncludeRevisionsRQL) == false;
                });
        }

        [Fact]
        public async Task MultiNodeSharding_Can_Put_Than_SwitchToLicenseWithRestriction()
        {
            using (var server = GetNewServer())
            {
                var options = ShardingTestBase.ShardingOptionsBuilder.Init()
                    .WithLeader(server)
                    .WithNumberOfShards(2)
                    .WithShardReplicationFactor(1)
                    .WithOrchestratorReplicationFactor(1)
                    .WithRunInMemory(false)
                    .WithoutDocumentsCompression()
                    .Build();

                var fixture = LicenseLimitsTestFixtureBuilder.InitSharded(this, options)
                    .Build();

                await Assert_Success_SwitchToLicenseWithRestrictions(fixture);

                await Assert_Equal(fixture,
                    expectedValue: 2,
                    actualValue: databaseRecord => databaseRecord.Sharding.Shards.Count);
            }
        }
    }
}
