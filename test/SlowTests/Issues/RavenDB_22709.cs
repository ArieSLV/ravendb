﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_22709 : ReplicationTestBase
    {
        public RavenDB_22709(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Replication)]
        public async Task PullReplicationWithSinksWithSameDatabaseNameShouldWork()
        {
            var hubCluster = await CreateRaftClusterWithSsl(3);
            var hubClusterCert = RegisterClientCertificate(hubCluster);

            var sinkCluster = await CreateRaftClusterWithSsl(1);
            var sinkClusterCert = RegisterClientCertificate(sinkCluster);

            var sinkCluster2 = await CreateRaftClusterWithSsl(1);
            var sinkClusterCert2 = RegisterClientCertificate(sinkCluster2);

            using var hubStore = GetDocumentStore(new Options
            {
                Server = hubCluster.Leader,
                ReplicationFactor = 3,
                AdminCertificate = hubClusterCert,
                ClientCertificate = hubClusterCert,
                ModifyDatabaseName = s => "HubDB"
            });
            using var sinkStore1 = GetDocumentStore(new Options
            {
                Server = sinkCluster.Leader,
                AdminCertificate = sinkClusterCert,
                ClientCertificate = sinkClusterCert,
                ModifyDatabaseName = s => "SinkDB"
            });

            using var sinkStore2 = GetDocumentStore(new Options
            {
                Server = sinkCluster2.Leader,
                AdminCertificate = sinkClusterCert2,
                ClientCertificate = sinkClusterCert2,
                ModifyDatabaseName = s => "SinkDB"
            });

            var pullCert1 = new X509Certificate2(await File.ReadAllBytesAsync(hubCluster.Certificates.ClientCertificate2Path), (string)null, X509KeyStorageFlags.Exportable);
            var pullCert2 = new X509Certificate2(await File.ReadAllBytesAsync(hubCluster.Certificates.ClientCertificate3Path), (string)null, X509KeyStorageFlags.Exportable);

            await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true,
                PinToMentorNode = true,
                MentorNode = "A"
            }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", new ReplicationHubAccess
            {
                Name = "Sink1",
                AllowedSinkToHubPaths = new[] { "*" },
                AllowedHubToSinkPaths = new[] { "*" },
                CertificateBase64 = Convert.ToBase64String(pullCert1.Export(X509ContentType.Cert)),
            }));

            await SetupSink(sinkStore1, "Sink1", pullCert1);

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", new ReplicationHubAccess
            {
                Name = "Sink2",
                AllowedSinkToHubPaths = new[] { "*" },
                AllowedHubToSinkPaths = new[] { "*" },
                CertificateBase64 = Convert.ToBase64String(pullCert2.Export(X509ContentType.Cert)),
            }));

            await SetupSink(sinkStore2, "Sink2", pullCert2);

            Assert.Equal(4, await WaitForValueAsync(async () =>
            {
                // 2 outgoing internal replication connections 
                // 2 outgoing pull replication connections
                var stats = await hubStore.Maintenance.ForNode("A").SendAsync(new GetReplicationPerformanceStatisticsOperation());
                return stats.Outgoing.Length;
            }, 4));

            Assert.Equal(4, await WaitForValueAsync(async () =>
            {
                // 2 incoming internal replication connections 
                // 2 incoming pull replication connections
                var stats = await hubStore.Maintenance.ForNode("A").SendAsync(new GetReplicationPerformanceStatisticsOperation());
                return stats.Incoming.Length;
            }, 4));

            X509Certificate2 RegisterClientCertificate((List<RavenServer> Nodes, RavenServer Leader, TestCertificatesHolder Certificates) cluster)
            {
                return Certificates.RegisterClientCertificate(cluster.Certificates.ServerCertificate.Value, cluster.Certificates
                    .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: cluster.Leader);
            }

            async Task SetupSink(DocumentStore sinkStore, string accessName, X509Certificate2 pullCert)
            {
                await sinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Database = hubStore.Database,
                    Name = hubStore.Database + "ConStr",
                    TopologyDiscoveryUrls = hubStore.Urls
                }));
                await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
                {
                    ConnectionStringName = hubStore.Database + "ConStr",
                    Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                    CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                    HubName = "both",
                    AccessName = accessName,
                    AllowedHubToSinkPaths = new[] { "*" },
                    AllowedSinkToHubPaths = new[] { "*" }
                }));
            }
        }
    }
}
