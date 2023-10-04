using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Commercial;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.ServerWide.Commands.Analyzers;
using Raven.Server.ServerWide.Commands.Sorters;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace LicenseTests;

public class LicenseLimitsTestsBase(ITestOutputHelper output) : ReplicationTestBase(output)
{
    protected static Task<T> Assert_Success_ExecutePutServerOperation<TOperation, T>(LicenseTestsFixtureBuilder<TOperation>.Fixture<TOperation> fixture) where TOperation : IServerOperation<T>
    {
        return ExecuteServerOperation<TOperation, T>(fixture);
    }

    protected static Task Assert_Success_ExecutePutServerOperation<TOperation>(LicenseTestsFixtureBuilder<TOperation>.Fixture<TOperation> fixture) where TOperation : IServerOperation
    {
        return ExecuteServerOperation(fixture);
    }

    protected static async Task Assert_Throw_ExecutePutServerOperation<TOperation, T>(LicenseTestsFixtureBuilder<TOperation>.Fixture<TOperation> fixture)
        where TOperation : IServerOperation<T>
    {
        await Assert.ThrowsAsync<LicenseLimitException>(() => ExecuteServerOperation<TOperation, T>(fixture));
    }

    protected static async Task Assert_Throw_ExecutePutServerOperation<TOperation>(LicenseTestsFixtureBuilder<TOperation>.Fixture<TOperation> fixture)
        where TOperation : IServerOperation
    {
        await Assert.ThrowsAsync<LicenseLimitException>(() => ExecuteServerOperation(fixture));
    }

    private static Task<T> ExecuteServerOperation<TOperation, T>(LicenseTestsFixtureBuilder<TOperation>.Fixture<TOperation> fixture) where TOperation : IServerOperation<T>
    {
        return fixture.Store.Maintenance.Server.SendAsync(fixture.PutOperation);
    }

    private static Task ExecuteServerOperation<TOperation>(LicenseTestsFixtureBuilder<TOperation>.Fixture<TOperation> fixture) where TOperation : IServerOperation
    {
        return fixture.Store.Maintenance.Server.SendAsync(fixture.PutOperation);
    }






    protected static Task<T> Assert_Success_ExecutePutMaintenanceOperation<TOperation, T>(LicenseTestsFixtureBuilder<TOperation>.Fixture<TOperation> fixture) where TOperation : IMaintenanceOperation<T>
    {
        var result = ExecuteMaintenanceOperation<TOperation, T>(fixture);
        return result;
    }

    protected static Task Assert_Success_ExecutePutMaintenanceOperation<TOperation>(LicenseTestsFixtureBuilder<TOperation>.Fixture<TOperation> fixture) where TOperation : IMaintenanceOperation
    {
        return ExecuteMaintenanceOperation(fixture);
    }

    protected static async Task Assert_Throw_ExecutePutMaintenanceOperation<TOperation, T>(LicenseTestsFixtureBuilder<TOperation>.Fixture<TOperation> fixture)
        where TOperation : IMaintenanceOperation<T>
    {
        await Assert.ThrowsAsync<LicenseLimitException>(() => ExecuteMaintenanceOperation<TOperation, T>(fixture));
    }

    protected static async Task Assert_Throw_ExecutePutMaintenanceOperation<TOperation>(LicenseTestsFixtureBuilder<TOperation>.Fixture<TOperation> fixture)
        where TOperation : IMaintenanceOperation
    {
        await Assert.ThrowsAsync<LicenseLimitException>(() => ExecuteMaintenanceOperation(fixture));
    }

    protected static Task Assert_Success_ExecuteUpdateMaintenanceOperation<TOperation, T>(DocumentStore store, TOperation operation) where TOperation : IMaintenanceOperation<T>
    {
        return ExecuteMaintenanceOperation<TOperation, T>(store, operation);
    }

    protected static async Task Assert_Throw_ExecuteUpdateMaintenanceOperation<TOperation, T>(DocumentStore store, TOperation operation) where TOperation : IMaintenanceOperation<T>
    {
        await Assert.ThrowsAsync<LicenseLimitException>(() => ExecuteMaintenanceOperation<TOperation, T>(store, operation));
    }

    private static Task<T> ExecuteMaintenanceOperation<TOperation, T>(LicenseTestsFixtureBuilder<TOperation>.Fixture<TOperation> fixture) where TOperation : IMaintenanceOperation<T>
    {
        return fixture.Store.Maintenance.SendAsync(fixture.PutOperation);
    }

    private static Task ExecuteMaintenanceOperation<TOperation, T>(DocumentStore store, TOperation operation) where TOperation : IMaintenanceOperation<T>
    {
        return store.Maintenance.SendAsync(operation);
    }

    private static Task ExecuteMaintenanceOperation<TOperation>(LicenseTestsFixtureBuilder<TOperation>.Fixture<TOperation> fixture) where TOperation : IMaintenanceOperation
    {
        return fixture.Store.Maintenance.SendAsync(fixture.PutOperation);
    }












    protected Task Assert_Success_SwitchToLicenseWithRestrictions<TOperation>(LicenseTestsFixtureBuilder<TOperation>.Fixture<TOperation> fixture)
    {
        return SwitchToCommunityLicense(fixture);
    }

    protected async Task Assert_Fail_SwitchToLicenseWithRestrictions<TOperation>(LicenseTestsFixtureBuilder<TOperation>.Fixture<TOperation> fixture)
    {
        await Assert.ThrowsAsync<LicenseLimitException>(() => SwitchToCommunityLicense(fixture));
    }

    protected static async Task Assert_Equal<TOperation>(LicenseTestsFixtureBuilder<TOperation>.Fixture<TOperation> fixture, int expectedValue, Func<DatabaseRecordWithEtag, int> actualValue)
    {
        Assert.Equal(expectedValue, actualValue(await GetDatabaseRecord(fixture)));
    }

    protected static async Task Assert_Equal<TOperation>(LicenseTestsFixtureBuilder<TOperation>.Fixture<TOperation> fixture, bool expectedValue, Func<DatabaseRecordWithEtag, bool> actualValue)
    {
        Assert.Equal(expectedValue, actualValue(await GetDatabaseRecord(fixture)));
    }

    protected static async Task Assert_Equal<TOperation>(LicenseTestsFixtureBuilder<TOperation>.Fixture<TOperation> fixture, int expectedValue, Func<RavenServer, int> actualValue)
    {
        Assert.Equal(expectedValue, actualValue(fixture.Server));
    }

    protected static async Task Assert_Equal<TOperation>(LicenseTestsFixtureBuilder<TOperation>.Fixture<TOperation> fixture, int expectedValue, Func<DocumentStore, int> actualValue)
    {
        Assert.Equal(expectedValue, actualValue(fixture.Store));
    }

    private static Task<DatabaseRecordWithEtag> GetDatabaseRecord<TOperation>(LicenseTestsFixtureBuilder<TOperation>.Fixture<TOperation> fixture) =>
        fixture.Store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(fixture.Store.Database));

    public static async Task SwitchToCommunityLicense<TOperation>(LicenseTestsFixtureBuilder<TOperation>.Fixture<TOperation> fixture)
    {
        await SwitchToCommunityLicense(fixture.Server, fixture.CommunityLicenseString);

        WaitForValue(() => fixture.Server.ServerStore.LicenseManager.LicenseStatus.Type, LicenseType.Community);
        Assert.Equal(LicenseType.Community, fixture.Server.ServerStore.LicenseManager.LicenseStatus.Type);
    }

    public static async Task SwitchToCommunityLicense(RavenServer server, string communityLicenseString)
    {
        if (server == null || communityLicenseString == null)
            throw new InvalidOperationException("Call Init method first to initialize server and store.");

        Assert.False(server.ServerStore.LicenseManager.LicenseStatus.Type == LicenseType.Community);

        using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            byte[] byteArray = Encoding.UTF8.GetBytes(communityLicenseString);
            await using Stream stream = new MemoryStream(byteArray);

            var json = await context.ReadForMemoryAsync(stream, "license activation");
            License license = JsonDeserializationServer.License(json);

            await server.ServerStore.EnsureNotPassiveAsync(skipLicenseActivation: true);
            await server.ServerStore.LicenseManager.ActivateAsync(license, RaftIdGenerator.NewId());
        }
    }





    internal static string GetResource(string resourceName, string originalSorterName, string sorterName)
    {
        using (var stream = GetDump(resourceName))
        using (var reader = new StreamReader(stream))
        {
            var analyzerCode = reader.ReadToEnd();
            analyzerCode = analyzerCode.Replace(originalSorterName, sorterName);

            return analyzerCode;
        }
    }

    private static Stream GetDump(string name)
    {
        var assembly = typeof(LicenseLimitsTests).Assembly;
        return assembly.GetManifestResourceStream("LicenseTests." + name);
    }

    internal static int GetServerWideCustomSortersCount(RavenServer server)
    {
        using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
            return server.ServerStore.Cluster.ItemsStartingWith(context, PutServerWideSorterCommand.Prefix, 0, long.MaxValue).Count();
    }

    internal static int GetServerWideAnalyzerCount(RavenServer server)
    {
        using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
            return server.ServerStore.Cluster.ItemsStartingWith(context, PutServerWideAnalyzerCommand.Prefix, 0, long.MaxValue).Count();
    }

    private static AddQueueSinkOperation<T> GetAddQueueSinkOperation<T>(IDocumentStore store, QueueSinkConfiguration configuration, T connectionString) where T : ConnectionString
    {
        var putResult = store.Maintenance.Send(new PutConnectionStringOperation<T>(connectionString));
        Assert.NotNull(putResult.RaftCommandIndex);

        return new AddQueueSinkOperation<T>(configuration);
    }

    private static UpdateQueueSinkOperation<T> GetUpdateQueueSinkOperation<T>(IDocumentStore store, long taskId, QueueSinkConfiguration configuration, T connectionString) where T : ConnectionString
    {
        var putResult = store.Maintenance.Send(new PutConnectionStringOperation<T>(connectionString));
        Assert.NotNull(putResult.RaftCommandIndex);

        return new UpdateQueueSinkOperation<T>(taskId, configuration);
    }

    internal AddQueueSinkOperation<QueueConnectionString> GetAddQueueSinkOperation(DocumentStore store, QueueBrokerType brokerType)
    {
        var parameters = PrepareQueueSinkConfiguration(brokerType);

        return GetAddQueueSinkOperation(store, parameters.Configuration, parameters.ConnectionString);
    }

    internal UpdateQueueSinkOperation<QueueConnectionString> GetUpdateQueueSinkOperation(DocumentStore store, long taskId, QueueBrokerType brokerType)
    {
        var parameters = PrepareQueueSinkConfiguration(brokerType, $"{GetDatabaseName()} to {nameof(brokerType)} (Updated)");
        return GetUpdateQueueSinkOperation(store, taskId, parameters.Configuration, parameters.ConnectionString);
    }

    private (QueueSinkConfiguration Configuration, QueueConnectionString ConnectionString) PrepareQueueSinkConfiguration(QueueBrokerType brokerType, string connectionStringName = null)
    {
        connectionStringName ??= $"{GetDatabaseName()} to {nameof(brokerType)}";

        var configuration = new QueueSinkConfiguration
        {
            Name = connectionStringName,
            ConnectionStringName = connectionStringName,
            Scripts = { new QueueSinkScript
            {
                Name = $"Queue Sink : {connectionStringName}",
                Queues = new List<string>(),
                Script = "put(this.Id, this)",
            } },
            BrokerType = brokerType
        };

        var connectionString = new QueueConnectionString
        {
            Name = configuration.ConnectionStringName,
            BrokerType = configuration.BrokerType
        };

        switch (brokerType)
        {
            case QueueBrokerType.Kafka:
                connectionString.KafkaConnectionSettings = new KafkaConnectionSettings { BootstrapServers = "http://localhost:1234", };
                break;
            case QueueBrokerType.RabbitMq:
                connectionString.RabbitMqConnectionSettings = new RabbitMqConnectionSettings { ConnectionString = "http://localhost:1234", };
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }

        return (configuration, connectionString);
    }

    internal ConfigureDataArchivalOperation GetConfigureDataArchivalOperation(IDocumentStore store, RavenServer server)
    {
        var config = new DataArchivalConfiguration { Disabled = false, ArchiveFrequencyInSec = 100 };

        if (store == null)
            throw new ArgumentNullException(nameof(store));
        if (server?.ServerStore == null)
            throw new ArgumentNullException(nameof(server.ServerStore));

        return new ConfigureDataArchivalOperation(config);
    }
}
