using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using LicenseTests.Fixtures;
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
using Raven.Server.Json;
using Raven.Server.ServerWide.Commands.Analyzers;
using Raven.Server.ServerWide.Commands.Sorters;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace LicenseTests;

public class LicenseLimitsTestsBase : ReplicationTestBase
{
    internal const string EnvironmentVariableForLicenceKeyWithAllFeaturesEnabled = "RAVEN_LICENSE";
    internal const string EnvironmentVariableForLicenceKeyWithAllFeaturesDisabled = "RAVEN_LICENSE_ALL_FEATURES_DISABLED";

    private static readonly string LicenseKeyWithAllFeaturesEnabled = Environment.GetEnvironmentVariable(EnvironmentVariableForLicenceKeyWithAllFeaturesEnabled);
    private static readonly string LicenseKeyWithAllFeaturesDisabled = Environment.GetEnvironmentVariable(EnvironmentVariableForLicenceKeyWithAllFeaturesDisabled);

    protected LicenseLimitsTestsBase(ITestOutputHelper output) : base(output)
    {
    }

    public static async Task SwitchToLicenseWithFeatureDisabled(RavenServer server)
    {
        if (server == null)
            throw new InvalidOperationException("Call Init method first to initialize server.");

        Assert.False(server.ServerStore.LicenseManager.LicenseStatus.Type == LicenseType.Community);

        using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            byte[] byteArray = Encoding.UTF8.GetBytes(LicenseKeyWithAllFeaturesDisabled);
            await using Stream stream = new MemoryStream(byteArray);

            var json = await context.ReadForMemoryAsync(stream, "license activation");
            License license = JsonDeserializationServer.License(json);

            await server.ServerStore.EnsureNotPassiveAsync(skipLicenseActivation: true);
            await server.ServerStore.LicenseManager.ActivateAsync(license, RaftIdGenerator.NewId());
        }

        WaitForValue(() => server.ServerStore.LicenseManager.LicenseStatus.Type, LicenseType.Community);
        Assert.Equal(LicenseType.Community, server.ServerStore.LicenseManager.LicenseStatus.Type);
    }

    protected static async Task Assert_Fail_SwitchToLicenseWithRestrictions(ILicenseLimitsTestsFixture fixture)
    {
        await Assert.ThrowsAsync<LicenseLimitException>(() => SwitchToLicenseWithFeatureDisabled(fixture.Server));
    }

    protected static Task Assert_Success_SwitchToLicenseWithRestrictions(ILicenseLimitsTestsFixture fixture)
    {
        return SwitchToLicenseWithFeatureDisabled(fixture.Server);
    }


    private static Task ExecuteServerOperation<TOperation>(ILicenseLimitsTestsFixture<TOperation> fixture) where TOperation : IServerOperation =>
        fixture.Store.Maintenance.Server.SendAsync(fixture.PutOperation);

    private static Task<TResult> ExecuteServerOperation<TOperation, TResult>(ILicenseLimitsTestsFixture<TOperation, TResult> fixture) where TOperation : IServerOperation<TResult> =>
        fixture.Store.Maintenance.Server.SendAsync(fixture.PutOperation);

    protected static Task Assert_Success_ExecutePut_ServerOperation<TOperation>(ILicenseLimitsTestsFixture<TOperation> fixture) where TOperation : IServerOperation =>
        ExecuteServerOperation(fixture);

    protected static Task<TResult> Assert_Success_ExecutePut_ServerOperation<TOperation, TResult>(ILicenseLimitsTestsFixture<TOperation, TResult> fixture) where TOperation : IServerOperation<TResult> =>
        ExecuteServerOperation(fixture);

    protected static Task Assert_Throw_ExecutePut_ServerOperation<TOperation>(ILicenseLimitsTestsFixture<TOperation> fixture) where TOperation : IServerOperation =>
        Assert.ThrowsAsync<LicenseLimitException>(() => ExecuteServerOperation(fixture));

    protected static Task Assert_Throw_ExecutePut_ServerOperation<TOperation, TResult>(ILicenseLimitsTestsFixture<TOperation, TResult> fixture) where TOperation : IServerOperation<TResult> =>
        Assert.ThrowsAsync<LicenseLimitException>(() => ExecuteServerOperation(fixture));


    private static Task<TResult> ExecuteMaintenanceOperation<TOperation, TResult>(IDocumentStore store, TOperation operation) where TOperation : IMaintenanceOperation<TResult> =>
        store.Maintenance.SendAsync(operation);

    protected static async Task Assert_Success_ExecutePut_MaintenanceOperation<TOperation, TResult>(ILicenseLimitsTestsFixture<TOperation, TResult> fixture) where TOperation : IMaintenanceOperation<TResult>
    {
        fixture.PutResult = await ExecuteMaintenanceOperation<TOperation, TResult>(fixture.Store, fixture.PutOperation);
    }

    protected static Task Assert_Throw_ExecutePut_MaintenanceOperation<TOperation, TResult>(ILicenseLimitsTestsFixture<TOperation, TResult> fixture) where TOperation : IMaintenanceOperation<TResult> =>
        Assert.ThrowsAsync<LicenseLimitException>(() => ExecuteMaintenanceOperation<TOperation, TResult>(fixture.Store, fixture.PutOperation));

    protected static Task Assert_Success_ExecuteUpdate_MaintenanceOperation<TOperation, TResult>(IDocumentStore store, TOperation operation) where TOperation : IMaintenanceOperation<TResult> =>
        ExecuteMaintenanceOperation<TOperation, TResult>(store, operation);

    protected static Task Assert_Throw_ExecuteUpdate_MaintenanceOperation<TOperation, TResult>(IDocumentStore store, TOperation operation) where TOperation : IMaintenanceOperation<TResult> =>
        Assert.ThrowsAsync<LicenseLimitException>(() => ExecuteMaintenanceOperation<TOperation, TResult>(store, operation));


    private static Task ExecuteMaintenanceOperation<TOperation>(ILicenseLimitsTestsFixture<TOperation> fixture) where TOperation : IMaintenanceOperation =>
        fixture.Store.Maintenance.SendAsync(fixture.PutOperation);

    protected static Task Assert_Success_ExecutePut_MaintenanceOperation<TOperation>(ILicenseLimitsTestsFixture<TOperation> fixture) where TOperation : IMaintenanceOperation =>
        ExecuteMaintenanceOperation(fixture);

    protected static Task Assert_Throw_ExecutePut_MaintenanceOperation<TOperation>(ILicenseLimitsTestsFixture<TOperation> fixture) where TOperation : IMaintenanceOperation =>
        Assert.ThrowsAsync<LicenseLimitException>(() => ExecuteMaintenanceOperation(fixture));


    private static async Task CreateSubscription(LicenseLimitsSubscriptionsTestFixtureBuilder.Fixture fixture)
    {
        await fixture.Store.Subscriptions.CreateAsync(fixture.SubscriptionCreationOptions);
        var subscriptionsConfig = await fixture.Store.Subscriptions.GetSubscriptionsAsync(start: 0, take: int.MaxValue);

        Assert.NotNull(subscriptionsConfig);
        fixture.SubscriptionId = subscriptionsConfig[0].SubscriptionId;

        if (fixture.SubscriptionUpdateOption != null)
            fixture.SubscriptionUpdateOption.Id = subscriptionsConfig[0].SubscriptionId;
    }

    protected static Task Assert_Success_CreateSubscription(LicenseLimitsSubscriptionsTestFixtureBuilder.Fixture fixture) =>
        CreateSubscription(fixture);
    protected static Task Assert_Throw_CreateSubscription(LicenseLimitsSubscriptionsTestFixtureBuilder.Fixture fixture) =>
        Assert.ThrowsAsync<LicenseLimitException>(() => CreateSubscription(fixture));


    private static async Task UpdateSubscription(LicenseLimitsSubscriptionsTestFixtureBuilder.Fixture fixture)
    {
        var subscriptionsConfig = await fixture.Store.Subscriptions.GetSubscriptionsAsync(0, int.MaxValue);
        Assert.NotNull(subscriptionsConfig);
        Assert.Single(subscriptionsConfig);

        Assert.NotNull(fixture.SubscriptionUpdateOption);
        await fixture.Store.Subscriptions.UpdateAsync(fixture.SubscriptionUpdateOption);
    }

    protected static Task Assert_Success_UpdateSubscription(LicenseLimitsSubscriptionsTestFixtureBuilder.Fixture fixture) =>
        UpdateSubscription(fixture);

    protected static Task Assert_Fail_UpdateSubscription(LicenseLimitsSubscriptionsTestFixtureBuilder.Fixture fixture) =>
        Assert.ThrowsAsync<LicenseLimitException>(() =>  UpdateSubscription(fixture));


    protected static async Task Assert_Equal<T>(ILicenseLimitsTestsFixture fixture, T expectedValue, Func<DocumentStore, Task<T>> actualValue)
    {
        Assert.Equal(expectedValue, actualValue(fixture.Store).Result);
    }

    protected static async Task Assert_Equal<T>(ILicenseLimitsTestsFixture fixture, T expectedValue, Func<DatabaseRecordWithEtag, T> actualValue)
    {
        Assert.Equal(expectedValue, actualValue(await GetDatabaseRecord(fixture.Store)));
    }

    protected static void Assert_Equal<TOperation, T>(ILicenseLimitsTestsFixture<TOperation> fixture, T expectedValue, Func<DocumentStore, T> actualValue)
    {
        Assert.Equal(expectedValue, actualValue(fixture.Store));
    }

    protected static void Assert_Equal<TOperation, T>(ILicenseLimitsTestsFixture<TOperation> fixture, T expectedValue, Func<RavenServer, T> actualValue)
    {
        Assert.Equal(expectedValue, actualValue(fixture.Server));
    }

    protected static async Task Assert_Equal<TOperation, TResult, T>(ILicenseLimitsTestsFixture<TOperation, TResult> fixture, T expectedValue, Func<DatabaseRecordWithEtag, T> actualValue)
    {
        Assert.Equal(expectedValue, actualValue(await GetDatabaseRecord(fixture.Store)));
    }

    private static Task<DatabaseRecordWithEtag> GetDatabaseRecord(IDocumentStore store) =>
        store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));


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

public class LicenseWithAllFeaturesDisabledRequiredFactAttribute : FactAttribute
{
    private static readonly bool HasAllFeaturesEnabledLicense;
    private static readonly bool HasAllFeaturesDisabledLicense;

    static LicenseWithAllFeaturesDisabledRequiredFactAttribute()
    {
        HasAllFeaturesEnabledLicense = string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(LicenseLimitsTestsBase.EnvironmentVariableForLicenceKeyWithAllFeaturesEnabled)) == false;
        HasAllFeaturesDisabledLicense = string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(LicenseLimitsTestsBase.EnvironmentVariableForLicenceKeyWithAllFeaturesEnabled)) == false;
    }

    public override string Skip => ShouldSkip(out string skipMessage) ? skipMessage : null;

    private static bool ShouldSkip(out string skipMessage)
    {
        if (HasAllFeaturesEnabledLicense == false && HasAllFeaturesDisabledLicense == false)
        {
            skipMessage = $"Test execution requires both license keys '{LicenseLimitsTestsBase.EnvironmentVariableForLicenceKeyWithAllFeaturesEnabled}' and " +
                          $"'{LicenseLimitsTestsBase.EnvironmentVariableForLicenceKeyWithAllFeaturesDisabled}' to be set in environment variables. " +
                          $"Test will be skipped as both of these keys are missing.";

            return true;
        }

        if (HasAllFeaturesEnabledLicense == false)
        {
            skipMessage = $"Test execution requires 'All Features Enabled' license key with '{LicenseLimitsTestsBase.EnvironmentVariableForLicenceKeyWithAllFeaturesEnabled}' " +
                          $"to be set in environment variables. Test will be skipped as this key is missing.";

            return true;
        }

        if (HasAllFeaturesDisabledLicense == false)
        {
            skipMessage = $"Test execution requires 'All Features Disabled' license key with '{LicenseLimitsTestsBase.EnvironmentVariableForLicenceKeyWithAllFeaturesDisabled}' " +
                          $"to be set in environment variables. Test will be skipped as this key is missing.";

            return true;
        }
        
        skipMessage = null;
        return false;
    }
}
