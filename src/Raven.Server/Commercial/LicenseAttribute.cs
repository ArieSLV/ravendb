namespace Raven.Server.Commercial;

public enum LicenseAttribute
{
    Type,
    Version,
    Expiration,
    Memory,
    Cores,
    Redist,
    Encryption,
    Snmp,
    DistributedCluster,
    MaxClusterSize,
    SnapshotBackup,
    CloudBackup,
    DynamicNodesDistribution,
    ExternalReplication, 
    DelayedExternalReplication,
    RavenEtl,
    SqlEtl,
    HighlyAvailableTasks,
    PullReplicationHub,
    PullReplicationSink,
    EncryptedBackup,
    LetsEncryptAutoRenewal,
    Cloud,
    DocumentsCompression,
    TimeSeriesRollupsAndRetention,
    AdditionalAssembliesNuget,
    MonitoringEndpoints,
    OlapEtl,
    ReadOnlyCertificates,
    TcpDataCompression,
    ConcurrentSubscriptions,
    ElasticSearchEtl,
    PowerBI,
    PostgreSqlIntegration,
    CanBeActivatedUntil,
    QueueEtl,
    ServerWideBackups, // TODO: only PUT done, check if other commands exist
    ServerWideExternalReplications, // TODO: only PUT done, check if other commands exist
    ServerWideCustomSorters, // TODO: only PUT done, check if other commands exist
    ServerWideAnalyzers, // TODO: only PUT done, check if other commands exist
    IndexCleanup, // TODO: only PUT done, check if other commands exist
    PeriodicBackup, // TODO: only PUT done
    ClientConfiguration, // TODO: only PUT done, check if other commands exist
    StudioConfiguration, // TODO: only PUT done, check if other commands exist
    QueueSink, // done
    DataArchival, // done
    RevisionsInSubscriptions,
    MultiNodeSharding,
    SetupDefaultRevisionsConfiguration,
    MaxCoresPerNode,
    MaxNumberOfRevisionsToKeep,
    MaxNumberOfRevisionAgeToKeepInDays,
    MinPeriodForExpirationInHours,
    MinPeriodForRefreshInHours,
    MaxReplicationFactorForSharding,
    MaxNumberOfStaticIndexesPerDatabase,
    MaxNumberOfStaticIndexesPerCluster,
    MaxNumberOfAutoIndexesPerDatabase,
    MaxNumberOfAutoIndexesPerCluster,
    MaxNumberOfSubscriptionsPerDatabase,
    MaxNumberOfSubscriptionsPerCluster,
    MaxNumberOfCustomSortersPerDatabase,
    MaxNumberOfCustomSortersPerCluster,
    MaxNumberOfCustomAnalyzersPerDatabase,
    MaxNumberOfCustomAnalyzersPerCluster,
}
