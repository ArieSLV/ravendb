using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class FilteredReplicationTests : ReplicationTestBase
    {
        public FilteredReplicationTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task Seasame_st()
        {
            var certificates = Certificates.SetupServerAuthentication();
            using var hooper = GetDocumentStore(new Options
            {
                ClientCertificate = certificates.ServerCertificateForCommunication.Value,
                AdminCertificate = certificates.ServerCertificateForCommunication.Value
            });
            using var bert = GetDocumentStore(new Options
            {
                ClientCertificate = certificates.ServerCertificateForCommunication.Value,
                AdminCertificate = certificates.ServerCertificateForCommunication.Value
            });

            using (var s = hooper.OpenAsyncSession())
            {
                await s.StoreAsync(new { Type = "Eggs" }, "menus/breakfast");
                await s.StoreAsync(new { Name = "Bird Seed Milkshake" }, "recipes/bird-seed-milkshake");
                await s.StoreAsync(new { Name = "3 USD" }, "prices/eastus/2");
                await s.StoreAsync(new { Name = "3 EUR" }, "prices/eu/1");
                await s.SaveChangesAsync();
            }

            using (var s = bert.OpenAsyncSession())
            {
                await s.StoreAsync(new { Name = "Candy" }, "orders/bert/3");
                await s.SaveChangesAsync();
            }

            await hooper.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "Franchises",
                Mode = PullReplicationMode.HubToSink | PullReplicationMode.SinkToHub,
                WithFiltering = true,
            }));

            await hooper.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("Franchises",
                new ReplicationHubAccess
                {
                    Name = "Franchises",
                    CertificateBase64 = Convert.ToBase64String(certificates.ClientCertificate1.Value.Export(X509ContentType.Cert)),
                    AllowedSinkToHubPaths = new[] { "orders/bert/*" },
                    AllowedHubToSinkPaths = new[] { "menus/*", "prices/eastus/*", "recipes/*" }
                }));


            await bert.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = hooper.Database,
                Name = "HopperConStr",
                TopologyDiscoveryUrls = hooper.Urls
            }));
            await bert.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                ConnectionStringName = "HopperConStr",
                CertificateWithPrivateKey = Convert.ToBase64String(certificates.ClientCertificate1.Value.Export(X509ContentType.Pfx)),
                HubName = "Franchises",
                Mode = PullReplicationMode.HubToSink | PullReplicationMode.SinkToHub
            }));

            Assert.True(WaitForDocument(bert, "menus/breakfast"));
            Assert.True(WaitForDocument(bert, "recipes/bird-seed-milkshake"));
            Assert.True(WaitForDocument(bert, "prices/eastus/2"));
            Assert.True(WaitForDocument(hooper, "orders/bert/3"));

            using (var s = bert.OpenAsyncSession())
            {
                Assert.Null(await s.LoadAsync<object>("prices/eu/1"));
            }

        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Revisions | RavenTestCategory.Counters | RavenTestCategory.TimeSeries)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Counters_and_force_revisions(Options options)
        {
            using var storeA = GetDocumentStore(options);
            using var storeB = GetDocumentStore(options);

            using (var s = storeA.OpenAsyncSession())
            {
                await s.StoreAsync(new { Breed = "German Shepherd" }, "users/ayende/dogs/arava");
                await s.StoreAsync(new { Color = "Gray/White" }, "users/pheobe");
                await s.StoreAsync(new { Name = "Oren" }, "users/ayende");
                s.CountersFor("users/ayende").Increment("test");
                s.CountersFor("users/pheobe").Increment("test");
                s.TimeSeriesFor<HeartRateMeasure>("users/pheobe").Append(RavenTestHelper.UtcToday, new HeartRateMeasure
                {
                    HeartRate = 34
                }, "test/things/out");
                s.TimeSeriesFor<HeartRateMeasure>("users/ayende").Append(RavenTestHelper.UtcToday, new HeartRateMeasure
                {
                    HeartRate = 55
                }, "test/things/out");
                s.Advanced.Attachments.Store("users/ayende", "test.bin", new MemoryStream(Encoding.UTF8.GetBytes("hello")));
                s.Advanced.Attachments.Store("users/pheobe", "test.bin", new MemoryStream(Encoding.UTF8.GetBytes("hello")));
                s.Advanced.Revisions.ForceRevisionCreationFor("users/ayende", ForceRevisionStrategy.None);
                s.Advanced.Revisions.ForceRevisionCreationFor("users/pheobe", ForceRevisionStrategy.None);
                await s.SaveChangesAsync();
            }

            using (var s = storeA.OpenAsyncSession())
            {
                await s.StoreAsync(new { Color = "Gray/White 2" }, "users/pheobe");
                await s.StoreAsync(new { Name = "Oren 2" }, "users/ayende");

                s.Advanced.Revisions.ForceRevisionCreationFor("users/ayende");
                s.Advanced.Revisions.ForceRevisionCreationFor("users/pheobe");
                await s.SaveChangesAsync();
            }

            await storeA.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = storeB.Database,
                Name = storeB.Database + "ConStr",
                TopologyDiscoveryUrls = storeA.Urls
            }));
            await storeA.Maintenance.SendAsync(new UpdateExternalReplicationOperation(new ExternalReplication
            {
                ConnectionStringName = storeB.Database + "ConStr",
                Name = "erpl"
            }));

            Assert.True(WaitForDocument(storeB, "users/ayende"));
            Assert.True(WaitForDocument(storeB, "users/pheobe"));
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task Can_Setup_Filtered_Replication()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbNameA = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var storeA = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbNameA
            });

            var pullCert = certificates.ClientCertificate2.Value;
            await storeA.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "pull",
                WithFiltering = true
            }));
            await storeA.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("pull",
                new ReplicationHubAccess
                {
                    Name = "Arava",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                    AllowedHubToSinkPaths = new[] { "users/ayende", "users/ayende/*" }
                }));
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task Cannot_setup_partial_filtered_replication()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbNameA = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var storeA = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbNameA
            });

            var pullCertA = certificates.ClientCertificate2.Value;
            var pullCertB = certificates.ClientCertificate3.Value;

            await storeA.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition { Name = "pull" }));

            await storeA.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("pull", new ReplicationHubAccess
            {
                Name = "pull1",
                CertificateBase64 = Convert.ToBase64String(pullCertA.Export(X509ContentType.Cert))
            }));

            await storeA.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("pull", new ReplicationHubAccess
            {
                Name = "pull2",
                CertificateBase64 = Convert.ToBase64String(pullCertB.Export(X509ContentType.Cert))
            }));

            await Assert.ThrowsAsync<RavenException>(async () => await storeA.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("pull",
                new ReplicationHubAccess
                {
                    Name = "Arava",
                    CertificateBase64 = Convert.ToBase64String(pullCertA.Export(X509ContentType.Cert)),
                    AllowedHubToSinkPaths = new[] { "users/ayende", "users/ayende/*" }
                })));
        }

        public class HeartRateMeasure
        {
            [TimeSeriesValue(0)] public double HeartRate;
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task WhenDeletingHubReplicationWillRemoveAllAccess()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbNameA = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var storeA = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbNameA
            });
            long[] ids = new long[3];
            var pullCert = new X509Certificate2(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
            for (int i = 0; i < 3; i++)
            {
                var op = await storeA.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
                {
                    Name = "pull" + i,
                    Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                    WithFiltering = true
                }));

                ids[i] = op.TaskId;

                await storeA.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("pull" + i, new ReplicationHubAccess
                {
                    Name = "Arava",
                    AllowedHubToSinkPaths = new[]
                    {
                        "users/ayende",
                        "users/ayende/*"
                    },
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                }));
            }

            await storeA.Maintenance.SendAsync(new DeleteOngoingTaskOperation(ids[1], OngoingTaskType.PullReplicationAsHub));

            await storeA.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "pull1",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            var accesses = await storeA.Maintenance.SendAsync(new GetReplicationHubAccessOperation("pull1"));
            Assert.Empty(accesses);

            accesses = await storeA.Maintenance.SendAsync(new GetReplicationHubAccessOperation("pull0"));
            Assert.NotEmpty(accesses);
            accesses = await storeA.Maintenance.SendAsync(new GetReplicationHubAccessOperation("pull2"));
            Assert.NotEmpty(accesses);
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task Can_pull_via_filtered_replication()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbNameA = GetDatabaseName();
            var dbNameB = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var storeA = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbNameA
            });
            using var storeB = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbNameB
            });

            using (var s = storeA.OpenAsyncSession())
            {
                await s.StoreAsync(new { Breed = "German Shepherd" }, "users/ayende/dogs/arava");
                await s.StoreAsync(new { Color = "Gray/White" }, "users/pheobe");
                await s.StoreAsync(new { Name = "Oren" }, "users/ayende");
                s.CountersFor("users/ayende").Increment("test");
                s.CountersFor("users/pheobe").Increment("test");
                s.TimeSeriesFor<HeartRateMeasure>("users/pheobe").Append(RavenTestHelper.UtcToday, new HeartRateMeasure
                {
                    HeartRate = 34
                }, "test/things/out");
                s.TimeSeriesFor<HeartRateMeasure>("users/ayende").Append(RavenTestHelper.UtcToday, new HeartRateMeasure
                {
                    HeartRate = 55
                }, "test/things/out");
                s.Advanced.Attachments.Store("users/ayende", "test.bin", new MemoryStream(Encoding.UTF8.GetBytes("hello")));
                s.Advanced.Attachments.Store("users/pheobe", "test.bin", new MemoryStream(Encoding.UTF8.GetBytes("hello")));
                s.Advanced.Revisions.ForceRevisionCreationFor("users/ayende", ForceRevisionStrategy.None);
                s.Advanced.Revisions.ForceRevisionCreationFor("users/pheobe", ForceRevisionStrategy.None);
                await s.SaveChangesAsync();
            }

            using (var s = storeA.OpenAsyncSession())
            {
                await s.LoadAsync<object>("users/pheobe");
                await s.LoadAsync<object>("users/ayende");
            }

            using (var s = storeA.OpenAsyncSession())
            {
                await s.StoreAsync(new { Color = "Gray/White 2" }, "users/pheobe");
                await s.StoreAsync(new { Name = "Oren 2" }, "users/ayende");

                s.Advanced.Revisions.ForceRevisionCreationFor("users/ayende");
                s.Advanced.Revisions.ForceRevisionCreationFor("users/pheobe");
                await s.SaveChangesAsync();
            }

            using (var s = storeA.OpenAsyncSession())
            {
                await s.LoadAsync<object>("users/pheobe");
                await s.LoadAsync<object>("users/ayende");
            }

            var pullCert = new X509Certificate2(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
            await storeA.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "pull",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            await storeA.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("pull", new ReplicationHubAccess
            {
                Name = "Arava",
                AllowedHubToSinkPaths = new[]
                {
                    "users/ayende",
                    "users/ayende/*"
                },
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
            }));

            await storeB.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = dbNameA,
                Name = dbNameA + "ConStr",
                TopologyDiscoveryUrls = storeA.Urls
            }));
            await storeB.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                ConnectionStringName = dbNameA + "ConStr",
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                HubName = "pull"
            }));

            WaitForDocument(storeB, "users/ayende");

            using (var s = storeB.OpenAsyncSession())
            {
                Assert.Null(await s.LoadAsync<object>("users/pheobe"));
                Assert.Null(await s.Advanced.Revisions.GetAsync<object>("users/pheobe", RavenTestHelper.UtcToday.AddDays(1)));
                Assert.Null(await s.CountersFor("users/pheobe").GetAsync("test"));
                Assert.Null(await s.TimeSeriesFor<HeartRateMeasure>("users/pheobe").GetAsync());
                using (var attachment = await s.Advanced.Attachments.GetAsync("users/pheobe", "test.bin"))
                {
                    Assert.Null(attachment);
                }

                Assert.NotNull(await s.LoadAsync<object>("users/ayende/dogs/arava"));
                Assert.NotNull(await s.LoadAsync<object>("users/ayende"));
                Assert.NotNull(await s.Advanced.Revisions.GetAsync<object>("users/ayende", RavenTestHelper.UtcToday.AddDays(1)));

                Assert.NotNull(await s.Advanced.Revisions.GetAsync<object>("users/ayende", RavenTestHelper.UtcToday.AddDays(1)));
                Assert.NotNull(await s.CountersFor("users/ayende").GetAsync("test"));
                Assert.NotEmpty(await s.TimeSeriesFor<HeartRateMeasure>("users/ayende").GetAsync());
                using (var attachment = await s.Advanced.Attachments.GetAsync("users/ayende", "test.bin"))
                {
                    Assert.NotNull(attachment);
                }
            }

            using (var s = storeA.OpenAsyncSession())
            {
                s.Delete("users/ayende/dogs/arava");
                await s.SaveChangesAsync();
            }

            WaitForDocumentDeletion(storeB, "users/ayende/dogs/arava");

            using (var s = storeB.OpenAsyncSession())
            {
                Assert.Null(await s.LoadAsync<object>("users/pheobe"));
                Assert.Null(await s.LoadAsync<object>("users/ayende/dogs/arava"));

                Assert.NotNull(await s.LoadAsync<object>("users/ayende"));

                Assert.NotNull(await s.Advanced.Revisions.GetAsync<object>("users/ayende", RavenTestHelper.UtcToday.AddDays(1)));
                Assert.NotNull(await s.CountersFor("users/ayende").GetAsync("test"));
                Assert.NotEmpty(await s.TimeSeriesFor<HeartRateMeasure>("users/ayende").GetAsync());
                using (var attachment = await s.Advanced.Attachments.GetAsync("users/ayende", "test.bin"))
                {
                    Assert.NotNull(attachment);
                }
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task Can_push_via_filtered_replication()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbNameA = GetDatabaseName();
            var dbNameB = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var storeA = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbNameA
            });
            using var storeB = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbNameB
            });

            using (var s = storeA.OpenAsyncSession())
            {
                await s.StoreAsync(new { Breed = "German Shepherd" }, "users/ayende/dogs/arava");
                await s.StoreAsync(new { Color = "Gray/White" }, "users/pheobe");
                await s.StoreAsync(new { Name = "Oren" }, "users/ayende");
                s.CountersFor("users/ayende").Increment("test");
                s.CountersFor("users/pheobe").Increment("test");
                s.TimeSeriesFor<HeartRateMeasure>("users/pheobe").Append(RavenTestHelper.UtcToday, new HeartRateMeasure
                {
                    HeartRate = 34
                }, "test/things/out");
                s.TimeSeriesFor<HeartRateMeasure>("users/ayende").Append(RavenTestHelper.UtcToday, new HeartRateMeasure
                {
                    HeartRate = 55
                }, "test/things/out");
                s.Advanced.Attachments.Store("users/ayende", "test.bin", new MemoryStream(Encoding.UTF8.GetBytes("hello")));
                s.Advanced.Attachments.Store("users/pheobe", "test.bin", new MemoryStream(Encoding.UTF8.GetBytes("hello")));
                s.Advanced.Revisions.ForceRevisionCreationFor("users/ayende", ForceRevisionStrategy.None);
                s.Advanced.Revisions.ForceRevisionCreationFor("users/pheobe", ForceRevisionStrategy.None);
                await s.SaveChangesAsync();
            }

            using (var s = storeA.OpenAsyncSession())
            {
                await s.LoadAsync<object>("users/pheobe");
                await s.LoadAsync<object>("users/ayende");
            }

            using (var s = storeA.OpenAsyncSession())
            {
                await s.StoreAsync(new { Color = "Gray/White 2" }, "users/pheobe");
                await s.StoreAsync(new { Name = "Oren 2" }, "users/ayende");

                s.Advanced.Revisions.ForceRevisionCreationFor("users/ayende");
                s.Advanced.Revisions.ForceRevisionCreationFor("users/pheobe");
                await s.SaveChangesAsync();
            }

            using (var s = storeA.OpenAsyncSession())
            {
                await s.LoadAsync<object>("users/pheobe");
                await s.LoadAsync<object>("users/ayende");
            }

            var pullCert = new X509Certificate2(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await storeB.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "push",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            await storeB.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("push", new ReplicationHubAccess
            {
                Name = "Arava",
                AllowedSinkToHubPaths = new[]
                {
                    "users/ayende",
                    "users/ayende/*"
                },
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
            }));

            await storeA.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = dbNameB,
                Name = dbNameB + "ConStr",
                TopologyDiscoveryUrls = storeA.Urls
            }));
            await storeA.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                ConnectionStringName = dbNameB + "ConStr",
                Mode = PullReplicationMode.SinkToHub,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                HubName = "push"
            }));

            Assert.True(WaitForDocument(storeB, "users/ayende"));
            using (var s = storeB.OpenAsyncSession())
            {
                Assert.Null(await s.LoadAsync<object>("users/pheobe"));
                Assert.Null(await s.Advanced.Revisions.GetAsync<object>("users/pheobe", RavenTestHelper.UtcToday.AddDays(1)));
                Assert.Null(await s.CountersFor("users/pheobe").GetAsync("test"));
                Assert.Null(await s.TimeSeriesFor<HeartRateMeasure>("users/pheobe").GetAsync());
                using (var attachment = await s.Advanced.Attachments.GetAsync("users/pheobe", "test.bin"))
                {
                    Assert.Null(attachment);
                }

                Assert.NotNull(await s.LoadAsync<object>("users/ayende/dogs/arava"));
                Assert.NotNull(await s.LoadAsync<object>("users/ayende"));
                Assert.NotNull(await s.Advanced.Revisions.GetAsync<object>("users/ayende", RavenTestHelper.UtcToday.AddDays(1)));

                Assert.NotNull(await s.Advanced.Revisions.GetAsync<object>("users/ayende", RavenTestHelper.UtcToday.AddDays(1)));
                Assert.NotNull(await s.CountersFor("users/ayende").GetAsync("test"));
                Assert.NotEmpty(await s.TimeSeriesFor<HeartRateMeasure>("users/ayende").GetAsync());
                using (var attachment = await s.Advanced.Attachments.GetAsync("users/ayende", "test.bin"))
                {
                    Assert.NotNull(attachment);
                }
            }

            using (var s = storeA.OpenAsyncSession())
            {
                s.Delete("users/ayende/dogs/arava");
                await s.SaveChangesAsync();
            }

            WaitForDocumentDeletion(storeB, "users/ayende/dogs/arava");

            using (var s = storeB.OpenAsyncSession())
            {
                Assert.Null(await s.LoadAsync<object>("users/pheobe"));
                Assert.Null(await s.LoadAsync<object>("users/ayende/dogs/arava"));

                Assert.NotNull(await s.LoadAsync<object>("users/ayende"));

                Assert.NotNull(await s.Advanced.Revisions.GetAsync<object>("users/ayende", RavenTestHelper.UtcToday.AddDays(1)));
                Assert.NotNull(await s.CountersFor("users/ayende").GetAsync("test"));
                Assert.NotEmpty(await s.TimeSeriesFor<HeartRateMeasure>("users/ayende").GetAsync());
                using (var attachment = await s.Advanced.Attachments.GetAsync("users/ayende", "test.bin"))
                {
                    Assert.NotNull(attachment);
                }
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task Can_pull_and_push_and_filter_at_dest_and_source()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbNameA = GetDatabaseName();
            var dbNameB = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var storeA = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbNameA
            });
            using var storeB = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbNameB
            });

            using (var s = storeA.OpenAsyncSession())
            {
                await s.StoreAsync(new { Location = "Hadera" }, "users/ayende/office");
                await s.StoreAsync(new { Breed = "German Shepherd" }, "users/ayende/dogs/arava");
                await s.StoreAsync(new { Color = "Gray/White" }, "users/pheobe");
                await s.StoreAsync(new { Name = "Oren" }, "users/ayende");

                await s.SaveChangesAsync();
            }

            using (var s = storeB.OpenAsyncSession())
            {
                await s.StoreAsync(new { Rolling = true }, "users/ayende/chair");
                await s.StoreAsync(new { Color = "Black" }, "users/oscar");
                await s.StoreAsync(new { Secret = "P@$$w0rD" }, "users/ayende/config");

                await s.SaveChangesAsync();
            }

            var pullCert = new X509Certificate2(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await storeA.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            await storeA.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", new ReplicationHubAccess
            {
                Name = "Arava",
                AllowedHubToSinkPaths = new[]
                {
                    "users/ayende",
                    "users/ayende/*"
                },
                AllowedSinkToHubPaths = new[]
                {
                    "users/ayende/config"
                },
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
            }));

            await storeB.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = dbNameA,
                Name = dbNameA + "ConStr",
                TopologyDiscoveryUrls = storeA.Urls
            }));
            await storeB.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                ConnectionStringName = dbNameA + "ConStr",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                HubName = "both",
                AllowedHubToSinkPaths = new[]
                {
                    "users/ayende",
                    "users/ayende/dogs/*"
                },
                AllowedSinkToHubPaths = new[]
                {
                    "users/ayende/config",
                    "users/ayende/chair"
                }
            }));

            WaitForDocument(storeB, "users/ayende");
            WaitForDocument(storeA, "users/ayende/config");

            using (var s = storeB.OpenAsyncSession())
            {
                Assert.Null(await s.LoadAsync<object>("users/ayende/office"));
                Assert.Null(await s.LoadAsync<object>("users/pheobe"));

                Assert.NotNull(await s.LoadAsync<object>("users/ayende/dogs/arava"));
                Assert.Null(await s.LoadAsync<object>("users/ayende/office"));

                Assert.NotNull(await s.LoadAsync<object>("users/ayende"));
            }

            using (var s = storeA.OpenAsyncSession())
            {
                Assert.Null(await s.LoadAsync<object>("users/ayende/chair"));
                Assert.Null(await s.LoadAsync<object>("users/oscar"));
                Assert.NotNull(await s.LoadAsync<object>("users/ayende/config"));
            }
        }

        public class Propagation
        {
            public bool FromHub;
            public bool FromSink1;
            public bool FromSink2;
            public bool Completed;
            public string Source;
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task PickupConfigurationChangesOnTheFly()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var hubStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });
            using var sinkStore1 = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });

            var pullCert = new X509Certificate2(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            var result = await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", new ReplicationHubAccess
            {
                Name = "Arava",
                AllowedSinkToHubPaths = new[]
                {
                    "*",
                },
                AllowedHubToSinkPaths = new[]
                {
                    "*"
                },
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
            }));

            await sinkStore1.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = hubStore.Database,
                Name = hubStore.Database + "ConStr",
                TopologyDiscoveryUrls = hubStore.Urls
            }));

            var sinkTask = new PullReplicationAsSink
            {
                ConnectionStringName = hubStore.Database + "ConStr",
                Mode = PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                HubName = "both",
                AllowedHubToSinkPaths = new[] { "*", },
                AllowedSinkToHubPaths = new[] { "*" }
            };

            var result2 = await sinkStore1.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(sinkTask));

            EnsureReplicating(hubStore, sinkStore1);

            await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.HubToSink | PullReplicationMode.SinkToHub,
                WithFiltering = true,
                TaskId = result.TaskId
            }));

            sinkTask.Mode = PullReplicationMode.HubToSink | PullReplicationMode.SinkToHub;
            sinkTask.TaskId = result2.TaskId;

            await sinkStore1.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(sinkTask));

            EnsureReplicating(sinkStore1, hubStore);
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task Sinks_should_not_update_hubs_change_vector()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var hubStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });
            using var sinkStore1 = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });
            using var sinkStore2 = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });

            var pullCert = new X509Certificate2(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", new ReplicationHubAccess
            {
                Name = "Arava",
                AllowedSinkToHubPaths = new[]
                {
                    "*",
                },
                AllowedHubToSinkPaths = new[]
                {
                    "*"
                },
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
            }));

            await SetupSink(sinkStore1);
            await SetupSink(sinkStore2);

            EnsureReplicating(hubStore, sinkStore1);
            EnsureReplicating(sinkStore1, hubStore);

            EnsureReplicating(hubStore, sinkStore2);
            EnsureReplicating(sinkStore2, hubStore);

            EnsureReplicating(sinkStore1, sinkStore2);
            EnsureReplicating(sinkStore2, sinkStore1);

            using (var s = hubStore.OpenAsyncSession())
            {
                await s.StoreAsync(new Propagation
                {
                    FromHub = true
                }, "common");
                await s.SaveChangesAsync();
            }

            Assert.True(WaitForDocument(sinkStore1, "common"));
            Assert.True(WaitForDocument(sinkStore2, "common"));

            using (var s = sinkStore1.OpenAsyncSession())
            {
                var common = await s.LoadAsync<Propagation>("common");
                common.FromSink1 = true;
                await s.SaveChangesAsync();
            }

            Assert.True(WaitForDocument<Propagation>(hubStore, "common", x => x.FromSink1 == true));
            Assert.True(WaitForDocument<Propagation>(sinkStore2, "common", x => x.FromSink1 == true));

            using (var s = sinkStore2.OpenAsyncSession())
            {
                var common = await s.LoadAsync<Propagation>("common");
                common.FromSink2 = true;
                await s.SaveChangesAsync();
            }

            Assert.True(WaitForDocument<Propagation>(hubStore, "common", x => x.FromSink2 == true));
            Assert.True(WaitForDocument<Propagation>(sinkStore1, "common", x => x.FromSink2 == true));

            WaitForUserToContinueTheTest(hubStore, clientCert: adminCert);

            using (var s = hubStore.OpenAsyncSession())
            {
                var common = await s.LoadAsync<Propagation>("common");
                common.Completed = true;
                await s.SaveChangesAsync();
            }

            Assert.True(WaitForDocument<Propagation>(sinkStore2, "common", x => x.Completed == true));
            Assert.True(WaitForDocument<Propagation>(sinkStore1, "common", x => x.Completed == true));

            var hubDb = await Databases.GetDocumentDatabaseInstanceFor(hubStore);
            var sink1Db = await Databases.GetDocumentDatabaseInstanceFor(sinkStore1);
            var sink2Db = await Databases.GetDocumentDatabaseInstanceFor(sinkStore2);

            using (hubDb.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var hubGlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx).AsString();
                Assert.Equal(1, hubGlobalCv.ToChangeVector().Length);
            }

            using (sink1Db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var sink1GlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx).AsString();
                Assert.Equal(2, sink1GlobalCv.ToChangeVector().Length);
            }

            using (sink2Db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var sink2GlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx).AsString();
                Assert.Equal(2, sink2GlobalCv.ToChangeVector().Length);
            }

            async Task SetupSink(DocumentStore sinkStore)
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
                    AllowedHubToSinkPaths = new[] { "*", },
                    AllowedSinkToHubPaths = new[] { "*" }
                }));
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task Sinks_should_not_update_hubs_change_vector2()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var hubStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });
            using var sinkStore1 = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });

            var pullCert = new X509Certificate2(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", new ReplicationHubAccess
            {
                Name = "Arava",
                AllowedSinkToHubPaths = new[]
                {
                    "*",
                },
                AllowedHubToSinkPaths = new[]
                {
                    "*"
                },
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
            }));

            await SetupSink(sinkStore1);

            EnsureReplicating(hubStore, sinkStore1);
            EnsureReplicating(sinkStore1, hubStore);

            using (var s = hubStore.OpenAsyncSession())
            {
                await s.StoreAsync(new Propagation
                {
                    FromHub = true
                }, "common");
                await s.SaveChangesAsync();
            }

            Assert.True(WaitForDocument(sinkStore1, "common"));

            using (var s = sinkStore1.OpenAsyncSession())
            {
                var common = await s.LoadAsync<Propagation>("common");
                common.FromSink1 = true;
                await s.SaveChangesAsync();
            }

            Assert.True(WaitForDocument<Propagation>(hubStore, "common", x => x.FromSink1 == true));

            using (var s = hubStore.OpenAsyncSession())
            {
                var common = await s.LoadAsync<Propagation>("common");
                common.Completed = true;
                await s.SaveChangesAsync();
            }

            Assert.True(WaitForDocument<Propagation>(sinkStore1, "common", x => x.Completed == true));

            using (var s = sinkStore1.OpenAsyncSession())
            {
                s.TimeSeriesFor("common", "test").Append(RavenTestHelper.UtcToday, 12);
                await s.SaveChangesAsync();
            }

            using (var s = sinkStore1.OpenAsyncSession())
            {
                s.CountersFor("common").Increment("test");
                await s.SaveChangesAsync();
            }

            using (var s = sinkStore1.OpenAsyncSession())
            {
                await using (var ms = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                {
                    s.Advanced.Attachments.Store("common", "test", ms);
                    await s.SaveChangesAsync();
                }
            }

            using (var s = sinkStore1.OpenAsyncSession())
            {
                var common = await s.LoadAsync<Propagation>("common");
                common.FromSink2 = true;
                await s.SaveChangesAsync();
            }

            Assert.True(WaitForDocument<Propagation>(hubStore, "common", x => x.FromSink2 == true));


            var hubDb = await Databases.GetDocumentDatabaseInstanceFor(hubStore);
            var sink1Db = await Databases.GetDocumentDatabaseInstanceFor(sinkStore1);

            using (hubDb.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var hubGlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx).AsString();
                Assert.Equal(1, hubGlobalCv.ToChangeVector().Length);
            }

            using (sink1Db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var sink1GlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx).AsString();
                Assert.Equal(2, sink1GlobalCv.ToChangeVector().Length);
            }

            using (var s = hubStore.OpenAsyncSession())
            {
                var common = await s.LoadAsync<Propagation>("common");
                var cv = s.Advanced.GetChangeVectorFor(common);
                var r = await s.Advanced.Revisions.GetForAsync<Propagation>("common");

                Assert.Equal(2, cv.ToChangeVectorList().Count);
                Assert.Contains("SINK", cv);
                Assert.Equal(0, r.Count);
            }

            using (var s = sinkStore1.OpenAsyncSession())
            {
                var common = await s.LoadAsync<Propagation>("common");
                var cv = s.Advanced.GetChangeVectorFor(common);
                var r = await s.Advanced.Revisions.GetForAsync<Propagation>("common");
                Assert.Equal(2, cv.ToChangeVectorList().Count);
                Assert.DoesNotContain("SINK", cv);
                Assert.Equal(0, r.Count);
            }

            async Task SetupSink(DocumentStore sinkStore)
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
                    AllowedHubToSinkPaths = new[] { "*", },
                    AllowedSinkToHubPaths = new[] { "*" }
                }));
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task Sinks_should_not_update_hubs_change_vector3()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var hubStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });
            using var sinkStore1 = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });
            using var sinkStore2 = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
            });

            var fooCert = new X509Certificate2(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            var barCert = new X509Certificate2(File.ReadAllBytes(certificates.ClientCertificate3Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            var access1 = new ReplicationHubAccess
            {
                Name = "both",
                AllowedSinkToHubPaths = new[] { "foo" },
                AllowedHubToSinkPaths = new[] { "foo" },
                CertificateBase64 = Convert.ToBase64String(fooCert.Export(X509ContentType.Cert)),
            };

            var access2 = new ReplicationHubAccess
            {
                Name = "both",
                AllowedSinkToHubPaths = new[] { "bar" },
                AllowedHubToSinkPaths = new[] { "bar" },
                CertificateBase64 = Convert.ToBase64String(barCert.Export(X509ContentType.Cert)),
            };

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", access1));
            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", access2));

            await SetupSink(sinkStore1, access1, fooCert);
            await SetupSink(sinkStore2, access2, barCert);

            using (var s = hubStore.OpenAsyncSession())
            {
                await s.StoreAsync(new Propagation
                {
                    FromHub = true
                }, "foo");
                await s.StoreAsync(new Propagation
                {
                    FromHub = true
                }, "bar");
                await s.SaveChangesAsync();
            }

            using (var s = hubStore.OpenAsyncSession())
            {
                var baseline = RavenTestHelper.UtcToday;
                for (int i = 0; i < 150; i++)
                {
                    s.TimeSeriesFor("foo", "test").Append(baseline.AddHours(i), 1);
                    s.TimeSeriesFor("bar", "test").Append(baseline.AddHours(i), 1);
                }
                await s.SaveChangesAsync();
            }

            Assert.True(WaitForDocument(sinkStore1, "foo"));
            Assert.True(WaitForDocument(sinkStore2, "bar"));

            await sinkStore1.TimeSeries.SetPolicyAsync<Propagation>("By3Hours", TimeValue.FromHours(3), TimeValue.FromDays(3));
            await sinkStore2.TimeSeries.SetPolicyAsync<Propagation>("By3Hours", TimeValue.FromHours(3), TimeValue.FromDays(3));

            var hubDb = await Databases.GetDocumentDatabaseInstanceFor(hubStore);
            var sink1Db = await Databases.GetDocumentDatabaseInstanceFor(sinkStore1);
            var sink2Db = await Databases.GetDocumentDatabaseInstanceFor(sinkStore2);

            await DoRollup(sink1Db);
            await DoRollup(sink2Db);

            using (var s = sinkStore1.OpenAsyncSession())
            {
                var common = await s.LoadAsync<Propagation>("foo");
                common.FromSink1 = true;
                await s.SaveChangesAsync();
            }

            using (var s = sinkStore2.OpenAsyncSession())
            {
                var common = await s.LoadAsync<Propagation>("bar");
                common.FromSink2 = true;
                await s.SaveChangesAsync();
            }

            Assert.True(WaitForDocument<Propagation>(hubStore, "foo", x => x.FromSink1 == true));
            Assert.True(WaitForDocument<Propagation>(hubStore, "bar", x => x.FromSink2 == true));

            using (var s = hubStore.OpenAsyncSession())
            {
                var foo = await s.LoadAsync<Propagation>("foo");
                foo.Completed = true;
                var bar = await s.LoadAsync<Propagation>("bar");
                bar.Completed = true;

                s.Advanced.Revisions.ForceRevisionCreationFor("foo");
                s.Advanced.Revisions.ForceRevisionCreationFor("bar");
                await s.SaveChangesAsync();
            }

            Assert.True(WaitForDocument<Propagation>(sinkStore2, "bar", x => x.Completed == true));
            Assert.True(WaitForDocument<Propagation>(sinkStore1, "foo", x => x.Completed == true));

            using (var token = new OperationCancelToken(hubDb.Configuration.Databases.OperationTimeout.AsTimeSpan, hubDb.DatabaseShutdown, CancellationToken.None))
                await hubDb.DocumentsStorage.RevisionsStorage.EnforceConfigurationAsync(_ => { }, token);

            using (var s = hubStore.OpenAsyncSession())
            {
                var foo = await s.LoadAsync<Propagation>("foo");
                foo.Source = "after-enforce-revision";
                var bar = await s.LoadAsync<Propagation>("bar");
                bar.Source = "after-enforce-revision";
                await s.SaveChangesAsync();
            }

            Assert.True(WaitForDocument<Propagation>(sinkStore2, "bar", x => x.Source == "after-enforce-revision"));
            Assert.True(WaitForDocument<Propagation>(sinkStore1, "foo", x => x.Source == "after-enforce-revision"));

            await VerifyNoRevisions(hubStore, "foo");
            await VerifyNoRevisions(hubStore, "bar");

            await VerifyNoRevisions(sinkStore1, "foo");
            await VerifyNoRevisions(sinkStore2, "bar");

            using (hubDb.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var hubGlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx).AsString();
                Assert.Equal(1, hubGlobalCv.ToChangeVector().Length);
            }

            using (sink1Db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var sink1GlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx).AsString();
                Assert.Equal(2, sink1GlobalCv.ToChangeVector().Length);
            }

            using (sink2Db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var sink2GlobalCv = DocumentsStorage.GetDatabaseChangeVector(ctx).AsString();
                Assert.Equal(2, sink2GlobalCv.ToChangeVector().Length);
            }

            using (var s = hubStore.OpenAsyncSession())
            {
                await AssertOnHub(s, "foo");
                await AssertOnHub(s, "bar");
            }

            using (var s = sinkStore1.OpenAsyncSession())
            {
                await AssertOnSink(s, "foo");
            }

            using (var s = sinkStore2.OpenAsyncSession())
            {
                await AssertOnSink(s, "bar");
            }

            async Task SetupSink(DocumentStore sinkStore, ReplicationHubAccess access, X509Certificate2 cert)
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
                    CertificateWithPrivateKey = Convert.ToBase64String(cert.Export(X509ContentType.Pfx)),
                    HubName = access.Name,
                    AllowedHubToSinkPaths = access.AllowedHubToSinkPaths,
                    AllowedSinkToHubPaths = access.AllowedSinkToHubPaths
                }));
            }

            async Task DoRollup(DocumentDatabase database)
            {
                await database.TimeSeriesPolicyRunner.HandleChanges();
                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();
            }
        }

        private static async Task VerifyNoRevisions(DocumentStore hubStore, string id)
        {
            using (var s = hubStore.OpenAsyncSession())
            {
                var rev = await s.Advanced.Revisions.GetForAsync<Propagation>(id);
                Assert.Equal(0, rev?.Count ?? 0);
            }
        }

        private static async Task AssertOnSink(IAsyncDocumentSession s, string id)
        {
            var doc = await s.LoadAsync<Propagation>(id);
            var cv = s.Advanced.GetChangeVectorFor(doc);
            var r = await s.Advanced.Revisions.GetForAsync<Propagation>(id);
            Assert.Equal(2, cv.ToChangeVectorList().Count);
            Assert.DoesNotContain("SINK", cv);
            Assert.Equal(0, r.Count);
        }

        private static async Task AssertOnHub(IAsyncDocumentSession s, string id)
        {
            var doc = await s.LoadAsync<Propagation>(id);
            var cv = s.Advanced.GetChangeVectorFor(doc);
            var r = await s.Advanced.Revisions.GetForAsync<Propagation>(id);

            Assert.Equal(2, cv.ToChangeVectorList().Count);
            Assert.Contains("SINK", cv);
            Assert.Equal(0, r.Count);
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates | RavenTestCategory.BackupExportImport)]
        public async Task Can_import_export_replication_certs()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbNameA = GetDatabaseName();
            var dbNameB = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var storeA = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbNameA
            });
            using var storeB = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbNameB
            });

            using (var s = storeA.OpenAsyncSession())
            {
                await s.StoreAsync(new { Location = "Hadera" }, "users/ayende/office");
                await s.StoreAsync(new { Breed = "German Shepherd" }, "users/ayende/dogs/arava");
                await s.StoreAsync(new { Color = "Gray/White" }, "users/pheobe");
                await s.StoreAsync(new { Name = "Oren" }, "users/ayende");

                await s.SaveChangesAsync();
            }

            var pullCert = new X509Certificate2(File.ReadAllBytes(certificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await storeA.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            await storeA.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", new ReplicationHubAccess
            {
                Name = "Arava",
                AllowedSinkToHubPaths = new[]
                {
                    "users/ayende",
                    "users/ayende/*"
                },
                AllowedHubToSinkPaths = new[]
                {
                    "users/ayende/config"
                },
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
            }));

            var file = GetTempFileName();
            var op = await storeA.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
            await op.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

            var accessResults = await storeB.Maintenance.SendAsync(new GetReplicationHubAccessOperation("both"));
            Assert.Empty(accessResults);

            op = await storeB.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
            await op.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

            accessResults = await storeB.Maintenance.SendAsync(new GetReplicationHubAccessOperation("both"));
            Assert.NotEmpty(accessResults);
            Assert.Equal("Arava", accessResults[0].Name);
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task Cannot_use_access_paths_if_filtering_is_not_set()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbNameA = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var storeA = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbNameA
            });

            var pullCert = certificates.ClientCertificate2.Value;
            await storeA.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "pull",
                WithFiltering = false
            }));

            var ex = await Assert.ThrowsAsync<RavenException>(async () =>
               await storeA.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("pull",
                   new ReplicationHubAccess
                   {
                       Name = "Arava",
                       CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                       AllowedHubToSinkPaths = new[] { "users/ayende", "users/ayende/*" }
                   })
           ));

            Assert.Contains("Filtering replication is not set for this Replication Hub task. AllowedSinkToHubPaths and AllowedHubToSinkPaths cannot have a value.", ex.InnerException.Message);
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task Must_use_access_paths_if_filtering_is_set()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbNameA = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates
                .ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            using var storeA = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbNameA
            });

            var pullCert = certificates.ClientCertificate2.Value;
            await storeA.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "pull",
                WithFiltering = true
            }));

            var ex = await Assert.ThrowsAsync<RavenException>(async () =>
               await storeA.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("pull",
                   new ReplicationHubAccess
                   {
                       Name = "Arava",
                       CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
                   })
               ));

            Assert.Contains("Either AllowedSinkToHubPaths or AllowedHubToSinkPaths must have a value, but both were null or empty", ex.InnerException.Message);
        }

        [RavenFact(RavenTestCategory.Replication)]
        public async Task Can_pull_and_push_with_first_transaction_on_sink()
        {
            var dbNameA = GetDatabaseName();
            var dbNameB = GetDatabaseName();

            var (hubNodes, hubLeader, hubCertificates) = await CreateRaftClusterWithSsl(2, watcherCluster: true);
            using var hub = GetDocumentStore(new Options
            {
                Server = hubLeader,
                ReplicationFactor = 2,
                AdminCertificate = hubCertificates.ServerCertificateForCommunication.Value,
                ClientCertificate = hubCertificates.ServerCertificateForCommunication.Value,
                ModifyDatabaseName = s => dbNameA,
                CreateDatabase = true
            });

            using var sink = GetDocumentStore(new Options
            {
                AdminCertificate = hubCertificates.ServerCertificateForCommunication.Value,
                ClientCertificate = hubCertificates.ServerCertificateForCommunication.Value,
                ModifyDatabaseName = s => dbNameB
            });

            var sinkDatabaseId = (await GetDatabase(Server, sink.Database)).DbBase64Id;
            var hubDatabaseIds = await Task.WhenAll(hubNodes.Select(async node => (await GetDatabase(node, hub.Database)).DbBase64Id));
            
            const string usersDocId1 = "users/1";

            using (var session = sink.OpenAsyncSession())
            {
                var user = new User { Name = "Grisha" };
                await session.StoreAsync(user, usersDocId1);
                await session.SaveChangesAsync();

                var changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.True(changeVector.Contains(sinkDatabaseId));
    }

            var pullCert = new X509Certificate2(File.ReadAllBytes(hubCertificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await hub.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            await hub.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", new ReplicationHubAccess
            {
                Name = "Arava",
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedHubToSinkPaths = new[] { "*" },
                AllowedSinkToHubPaths = new[] { "*" }
            }));

            await sink.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = dbNameA,
                Name = dbNameA + "ConStr",
                TopologyDiscoveryUrls = hub.Urls
            }));
            await sink.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                ConnectionStringName = dbNameA + "ConStr",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                HubName = "both",
                AllowedHubToSinkPaths = new[] { "*" },
                AllowedSinkToHubPaths = new[] { "*" }
            }));

            WaitForDocument(hub, usersDocId1);

            const int age = 38;
            using (var session = hub.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(usersDocId1);
                Assert.NotNull(user);

                var changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.True(changeVector.Contains(ChangeVectorParser.SinkTag));
                Assert.True(changeVector.Contains(sinkDatabaseId));

                var stats = await hub.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.False(stats.DatabaseChangeVector.Contains(ChangeVectorParser.SinkTag));

                user.Age = age;
                await session.SaveChangesAsync();
                changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.True(changeVector.Contains(ChangeVectorParser.SinkTag));
                Assert.True(changeVector.Contains(sinkDatabaseId));
                Assert.True(hubDatabaseIds.Any(id => changeVector.Contains(id)));

                stats = await hub.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.False(stats.DatabaseChangeVector.Contains(ChangeVectorParser.SinkTag));
}

            var ageVal = await WaitForValueAsync(async () =>
            {
                using (var session = sink.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(usersDocId1);
                    return user.Age;
                }
            }, age);
            Assert.Equal(age, ageVal);

            using (var session = sink.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(usersDocId1);

                var changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.False(changeVector.Contains(ChangeVectorParser.SinkTag));
                Assert.True(changeVector.Contains(sinkDatabaseId));
                Assert.True(hubDatabaseIds.Any(id => changeVector.Contains(id)));

                var stats = await sink.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.False(stats.DatabaseChangeVector.Contains(ChangeVectorParser.SinkTag));
            }
        }

        [RavenFact(RavenTestCategory.ClusterTransactions | RavenTestCategory.Replication)]
        public async Task Can_pull_and_push_with_first_cluster_transactions_on_sink()
        {
            var dbNameA = GetDatabaseName();
            var dbNameB = GetDatabaseName();

            var (hubNodes, hubLeader, hubCertificates) = await CreateRaftClusterWithSsl(2);
            using var hub = GetDocumentStore(new Options
            {
                Server = hubLeader,
                ReplicationFactor = 2,
                AdminCertificate = hubCertificates.ServerCertificateForCommunication.Value,
                ClientCertificate = hubCertificates.ServerCertificateForCommunication.Value,
                ModifyDatabaseName = s => dbNameA,
                CreateDatabase = true
            });

            using var sink = GetDocumentStore(new Options
            {
                AdminCertificate = hubCertificates.ServerCertificateForCommunication.Value,
                ClientCertificate = hubCertificates.ServerCertificateForCommunication.Value,
                ModifyDatabaseName = s => dbNameB
            });

            var sinkRecord = await sink.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(sink.Database));
            var sinkClusterId = sinkRecord.Topology.ClusterTransactionIdBase64;
            var hubDatabaseIds = await Task.WhenAll(hubNodes.Select(async node => (await GetDatabase(node, hub.Database)).DbBase64Id));

            const string usersDocId1 = "users/1";

            using (var session = sink.OpenAsyncSession())
            {
                var user = new User { Name = "Grisha" };
                session.Advanced.SetTransactionMode(TransactionMode.ClusterWide);
                await session.StoreAsync(user, usersDocId1);
                await session.SaveChangesAsync();

                var changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.True(changeVector.Contains(ChangeVectorParser.TrxnTag));
                Assert.True(changeVector.Contains(sinkClusterId));
            }

            var pullCert = new X509Certificate2(File.ReadAllBytes(hubCertificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await hub.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            await hub.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", new ReplicationHubAccess
            {
                Name = "Arava",
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedHubToSinkPaths = new[] { "*" },
                AllowedSinkToHubPaths = new[] { "*" }
            }));

            await sink.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = dbNameA,
                Name = dbNameA + "ConStr",
                TopologyDiscoveryUrls = hub.Urls
            }));
            await sink.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                ConnectionStringName = dbNameA + "ConStr",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                HubName = "both",
                AllowedHubToSinkPaths = new[] { "*" },
                AllowedSinkToHubPaths = new[] { "*" }
            }));

            Assert.True(await WaitForDocumentInClusterAsync<User>
                (hubNodes, dbNameA, usersDocId1, u => u.Name == "Grisha", 
                    timeout: TimeSpan.FromSeconds(30), certificate: hubCertificates.ServerCertificateForCommunication.Value));

            const int age = 38;
            using (var session = hub.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(usersDocId1);
                Assert.NotNull(user);

                var changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.True(changeVector.Contains(ChangeVectorParser.SinkTag));
                Assert.True(changeVector.Contains(sinkClusterId));

                await VerifyDatabaseChangeVector(hub);

                user.Age = age;
                await session.SaveChangesAsync();
                changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.True(changeVector.Contains(ChangeVectorParser.SinkTag));
                Assert.True(changeVector.Contains(sinkClusterId));
                Assert.True(hubDatabaseIds.Any(id => changeVector.Contains(id)));

                await VerifyDatabaseChangeVector(hub);
            }

            var ageVal = await WaitForValueAsync(async () =>
            {
                using (var session = sink.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(usersDocId1);
                    return user.Age;
                }
            }, age);
            Assert.Equal(age, ageVal);

            using (var session = sink.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(usersDocId1);

                var changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.True(changeVector.Contains(ChangeVectorParser.TrxnTag));
                Assert.True(changeVector.Contains(ChangeVectorParser.RaftTag));
                Assert.False(changeVector.Contains(ChangeVectorParser.SinkTag));
                Assert.True(changeVector.Contains(sinkClusterId));
                Assert.True(hubDatabaseIds.Any(id => changeVector.Contains(id)));

                await VerifyDatabaseChangeVector(sink);
            }

            using (var session = sink.OpenAsyncSession())
            {
                const string anotherUserId = "users/2";
                var user = new User();
                await session.StoreAsync(user, anotherUserId);
                await session.SaveChangesAsync();

                user.Name = "Grisha";
                await session.SaveChangesAsync();
            }

            async Task VerifyDatabaseChangeVector(DocumentStore store)
            {
                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.False(stats.DatabaseChangeVector.Contains(ChangeVectorParser.TrxnTag));
                Assert.False(stats.DatabaseChangeVector.Contains(ChangeVectorParser.SinkTag));
                Assert.False(stats.DatabaseChangeVector.Contains(sinkClusterId));
            }
        }

        [RavenFact(RavenTestCategory.Replication)]
        public async Task Can_pull_and_push_with_first_transactions_on_hub()
        {
            var dbNameA = GetDatabaseName();
            var dbNameB = GetDatabaseName();

            var (hubNodes, hubLeader, hubCertificates) = await CreateRaftClusterWithSsl(2, watcherCluster: true);
            using var hub = GetDocumentStore(new Options
            {
                Server = hubLeader,
                ReplicationFactor = 2,
                AdminCertificate = hubCertificates.ServerCertificateForCommunication.Value,
                ClientCertificate = hubCertificates.ServerCertificateForCommunication.Value,
                ModifyDatabaseName = s => dbNameA,
                CreateDatabase = true
            });

            using var sink = GetDocumentStore(new Options
            {
                AdminCertificate = hubCertificates.ServerCertificateForCommunication.Value,
                ClientCertificate = hubCertificates.ServerCertificateForCommunication.Value,
                ModifyDatabaseName = s => dbNameB
            });

            var sinkDatabaseId = (await GetDatabase(Server, sink.Database)).DbBase64Id;
            var hubDatabaseIds = await Task.WhenAll(hubNodes.Select(async node => (await GetDatabase(node, hub.Database)).DbBase64Id));

            const string usersDocId1 = "users/1";

            using (var session = hub.OpenAsyncSession())
            {
                var user = new User { Name = "Grisha" };
                await session.StoreAsync(user, usersDocId1);
                await session.SaveChangesAsync();

                var changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.True(hubDatabaseIds.Any(id => changeVector.Contains(id)));
            }

            var pullCert = new X509Certificate2(File.ReadAllBytes(hubCertificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await hub.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            await hub.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", new ReplicationHubAccess
            {
                Name = "Arava",
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedHubToSinkPaths = new[] { "*" },
                AllowedSinkToHubPaths = new[] { "*" }
            }));

            await sink.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = dbNameA,
                Name = dbNameA + "ConStr",
                TopologyDiscoveryUrls = hub.Urls
            }));
            await sink.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                ConnectionStringName = dbNameA + "ConStr",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                HubName = "both",
                AllowedHubToSinkPaths = new[] { "*" },
                AllowedSinkToHubPaths = new[] { "*" }
            }));

            WaitForDocument(sink, usersDocId1);

            const int age = 38;
            using (var session = sink.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(usersDocId1);
                Assert.NotNull(user);

                var changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.False(changeVector.Contains(ChangeVectorParser.SinkTag));
                Assert.True(hubDatabaseIds.Any(id => changeVector.Contains(id)));

                var stats = await hub.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.False(stats.DatabaseChangeVector.Contains(ChangeVectorParser.SinkTag));

                user.Age = age;
                await session.SaveChangesAsync();
                changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.False(changeVector.Contains(ChangeVectorParser.SinkTag));
                Assert.True(hubDatabaseIds.Any(id => changeVector.Contains(id)));
                Assert.True(changeVector.Contains(sinkDatabaseId));

                stats = await hub.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.False(stats.DatabaseChangeVector.Contains(ChangeVectorParser.SinkTag));
            }

            var ageVal = await WaitForValueAsync(async () =>
            {
                using (var session = hub.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(usersDocId1);
                    return user.Age;
                }
            }, age);
            Assert.Equal(age, ageVal);

            using (var session = hub.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(usersDocId1);

                var changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.True(changeVector.Contains(ChangeVectorParser.SinkTag));
                Assert.True(hubDatabaseIds.Any(id => changeVector.Contains(id)));
                Assert.True(changeVector.Contains(sinkDatabaseId));

                var stats = await hub.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.False(stats.DatabaseChangeVector.Contains(ChangeVectorParser.SinkTag));
            }
        }

        [RavenFact(RavenTestCategory.ClusterTransactions | RavenTestCategory.Replication)]
        public async Task Can_pull_and_push_with_first_cluster_transactions_on_hub()
        {
            var dbNameA = GetDatabaseName();
            var dbNameB = GetDatabaseName();

            var (_, hubLeader, hubCertificates) = await CreateRaftClusterWithSsl(2, watcherCluster: true);
            using var hub = GetDocumentStore(new Options
            {
                Server = hubLeader,
                ReplicationFactor = 2,
                AdminCertificate = hubCertificates.ServerCertificateForCommunication.Value,
                ClientCertificate = hubCertificates.ServerCertificateForCommunication.Value,
                ModifyDatabaseName = s => dbNameA,
                CreateDatabase = true
            });

            using var sink = GetDocumentStore(new Options
            {
                AdminCertificate = hubCertificates.ServerCertificateForCommunication.Value,
                ClientCertificate = hubCertificates.ServerCertificateForCommunication.Value,
                ModifyDatabaseName = s => dbNameB
            });

            var hubRecord = await hub.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(hub.Database));
            var hubClusterId = hubRecord.Topology.ClusterTransactionIdBase64;
            var sinkDatabaseId = (await GetDatabase(Server, sink.Database)).DbBase64Id;

            const string usersDocId1 = "users/1";

            using (var session = hub.OpenAsyncSession())
            {
                var user = new User { Name = "Grisha" };
                session.Advanced.SetTransactionMode(TransactionMode.ClusterWide);
                await session.StoreAsync(user, usersDocId1);
                await session.SaveChangesAsync();

                var changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.True(changeVector.Contains(ChangeVectorParser.TrxnTag));
                Assert.True(changeVector.Contains(hubClusterId));
            }

            var pullCert = new X509Certificate2(File.ReadAllBytes(hubCertificates.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);

            await hub.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "both",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            await hub.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("both", new ReplicationHubAccess
            {
                Name = "Arava",
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedHubToSinkPaths = new[] { "*" },
                AllowedSinkToHubPaths = new[] { "*" }
            }));

            await sink.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = dbNameA,
                Name = dbNameA + "ConStr",
                TopologyDiscoveryUrls = hub.Urls
            }));
            await sink.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                ConnectionStringName = dbNameA + "ConStr",
                Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                HubName = "both",
                AllowedHubToSinkPaths = new[] { "*" },
                AllowedSinkToHubPaths = new[] { "*" }
            }));

            WaitForDocument(sink, usersDocId1);

            const int age = 38;
            using (var session = sink.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(usersDocId1);
                Assert.NotNull(user);

                var changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.True(changeVector.Contains(ChangeVectorParser.TrxnTag));
                Assert.True(changeVector.Contains(hubClusterId));

                await VerifyDatabaseChangeVector(sink);

                user.Age = age;
                await session.SaveChangesAsync();
                changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.True(changeVector.Contains(ChangeVectorParser.TrxnTag));
                Assert.True(changeVector.Contains(hubClusterId));
                Assert.True(changeVector.Contains(sinkDatabaseId));

                await VerifyDatabaseChangeVector(sink);
            }

            var ageVal = await WaitForValueAsync(async () =>
            {
                using (var session = hub.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(usersDocId1);
                    return user.Age;
                }
            }, age);
            Assert.Equal(age, ageVal);

            using (var session = hub.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(usersDocId1);

                var changeVector = session.Advanced.GetChangeVectorFor(user);
                Assert.True(changeVector.Contains(ChangeVectorParser.TrxnTag));
                Assert.True(changeVector.Contains(ChangeVectorParser.RaftTag));
                Assert.True(changeVector.Contains(ChangeVectorParser.SinkTag));
                Assert.True(changeVector.Contains(hubClusterId));
                Assert.True(changeVector.Contains(sinkDatabaseId));

                await VerifyDatabaseChangeVector(hub);
            }

            using (var session = hub.OpenAsyncSession())
            {
                const string anotherUserId = "users/2";
                var user = new User();
                await session.StoreAsync(user, anotherUserId);
                await session.SaveChangesAsync();

                user.Name = "Grisha";
                await session.SaveChangesAsync();
            }

            async Task VerifyDatabaseChangeVector(DocumentStore store)
            {
                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.False(stats.DatabaseChangeVector.Contains(ChangeVectorParser.TrxnTag));
                Assert.False(stats.DatabaseChangeVector.Contains(ChangeVectorParser.SinkTag));
                Assert.False(stats.DatabaseChangeVector.Contains(hubClusterId));
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task ShouldKeepConnectionAlive_WhenFilteringTakesLongTime()
        {
            // 1. Configure settings
            var customSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Replication.ReplicationMinimalHeartbeat)] = "1",
                [RavenConfiguration.GetKey(x => x.Replication.ActiveConnectionTimeout)] = "8"
            };

            var certificates = Certificates.SetupServerAuthentication(customSettings: customSettings);
            var dbNameHub = GetDatabaseName();
            var dbNameSink = GetDatabaseName();

            var adminCert = Certificates.RegisterClientCertificate(
                certificates.ServerCertificateForCommunication.Value,
                certificates.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin);

            using var hubStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = _ => dbNameHub
            });

            using var sinkStore = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = _ => dbNameSink
            });

            // 2. Prepare Data
            // We replicate enough documents to complete several full 1024-cycles.
            const int documentsCount = 5000;

            // Using single transactions as requested
            for (int i = 0; i < documentsCount; i++)
            {
                using (var session = hubStore.OpenAsyncSession())
                {
                    await session.StoreAsync(new { Type = "Noise" }, $"items/skip/{i}");
                    await session.SaveChangesAsync();
                }
            }

            // The marker document
            using (var session = hubStore.OpenAsyncSession())
            {
                await session.StoreAsync(new { Type = "Important" }, "items/include/1");
                await session.SaveChangesAsync();
            }

            // 3. Inject dynamic Delay Logic
            var hubDb = await Databases.GetDocumentDatabaseInstanceFor(hubStore);
            hubDb.ReplicationLoader.ForTestingPurposesOnly().OnOutgoingReplicationStart = outgoingHandler =>
            {
                if (outgoingHandler.Destination.Database != sinkStore.Database)
                    return;

                int itemsProcessed = 0;
                var sw = Stopwatch.StartNew();

                outgoingHandler.ForTestingPurposesOnly().OnDocumentSenderFetchNewItem = () =>
                {
                    itemsProcessed++;

                    if ((itemsProcessed & 1023) != 0)
                        return;

                    // We want each batch of 1024 items to take AT LEAST this long.
                    // 2500ms > 1000ms HeartbeatInterval (triggers heartbeat)
                    // 2500ms < 8000ms ConnectionTimeout (safe for single batch)
                    // Total time for 5000 docs approx 10-12s > 8s (fails without heartbeat)
                    const int targetBatchDurationMs = 2500;

                    var elapsed = sw.ElapsedMilliseconds;
                    var timeToWait = targetBatchDurationMs - elapsed;

                    if (timeToWait > 0)
                        Thread.Sleep((int)timeToWait);

                    // Reset for the next batch
                    sw.Restart();
                };
            };

            // 4. Setup Pull Replication
            var pullCert = certificates.ClientCertificate2.Value;
            var pullCertBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx));
            var publicCertBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert));

            await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "slow-hub",
                Mode = PullReplicationMode.HubToSink,
                WithFiltering = true
            }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("slow-hub",
                new ReplicationHubAccess
                {
                    Name = "SinkUser",
                    CertificateBase64 = publicCertBase64,
                    AllowedHubToSinkPaths = ["items/include/*"]
                }));

            await sinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = hubStore.Database,
                Name = "HubConStr",
                TopologyDiscoveryUrls = hubStore.Urls
            }));

            await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                ConnectionStringName = "HubConStr",
                CertificateWithPrivateKey = pullCertBase64,
                HubName = "slow-hub",
                Mode = PullReplicationMode.HubToSink
            }));

            // 5. Assert
            // Total simulated time is approx 12 seconds. Connection timeout is 8 seconds.
            var replicated = WaitForDocument(sinkStore, "items/include/1", timeout: 30_000);
            Assert.True(replicated, "Document should be replicated. Failure implies connection timeout due to lack of heartbeats.");
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task Sink_to_hub_replication_inflates_hub_database_change_vector_causing_data_loss()
        {
            var tracePath = Path.Combine(Path.GetTempPath(), $"replication-issue-{Guid.NewGuid():N}.trace.log");
            ReplicationInvestigationTrace.Configure(tracePath, "internal/", "tickets/");
            Output.WriteLine($"Trace file: {tracePath}");
            Output.WriteLine($"Readable trace file: {ReplicationInvestigationTrace.ReadablePath}");

            try
            {
                // This test demonstrates that when a sink sends documents to a hub node in a cluster,
                // the hub's database change vector gets inflated with entries for OTHER hub nodes.
                // This causes internal replication to skip documents via the etag jump-ahead optimization,
                // resulting in permanent data loss on the receiving hub node.
                //
                // Setup: 3-node hub cluster (A, B, C) + 1 sink with bidirectional filtered pull replication.
                // We use TWO hub definitions:
                //   - one pinned to A for the initial hub-to-sink flow
                //   - one pinned to C for the later sink-to-hub inflation step
                // This avoids RavenDB's normal pull load-balancing from routing the second sink task back to A.
                // Bug flow:
                //   1. Node A writes non-filtered docs (internal/*) and filtered docs (tickets/*)
                //   2. Hub-to-sink sends tickets/* to sink (with A's high etag in CV)
                //   3. A second sink task connects to a hub definition pinned to C and sends tickets/* (sink-to-hub)
                //   4. ReplaceUnknownEntriesWithSinkTag returns A's entries as changeVectorToMerge
                //   5. C's database CV gets inflated with A:high_etag
                //   6. Internal replication A→C uses etag jump-ahead past the inflated value
                //   7. internal/* documents are permanently skipped on C

                var hubDbName = GetDatabaseName();
                var sinkDbName = GetDatabaseName();

                (List<RavenServer> hubNodes, RavenServer hubLeader, TestCertificatesHolder certs) = await CreateRaftClusterWithSsl(numberOfNodes: 3, watcherCluster: true);
                var serverA = hubNodes[0];
                var serverB = hubNodes[1];
                var serverC = hubNodes[2];

                // Hub store connects to the leader for cluster-wide operations
                using var hubStore = GetDocumentStore(new Options
                {
                    Server = hubLeader,
                    ReplicationFactor = 3,
                    AdminCertificate = certs.ServerCertificateForCommunication.Value,
                    ClientCertificate = certs.ServerCertificateForCommunication.Value,
                    ModifyDatabaseName = _ => hubDbName,
                    CreateDatabase = true
                });

                // Node-specific stores for targeted reads/writes
                var nodeStores =
                    Cluster.GetDocumentStores(nodes: [serverA, serverC], hubDbName, disableTopologyUpdates: true, certificate: certs.ServerCertificateForCommunication.Value);
                using var storeA = nodeStores[0];
                using var storeC = nodeStores[1];

                // Sink store (separate database on default server)
                using var sinkStore = GetDocumentStore(new Options
                {
                    AdminCertificate = certs.ServerCertificateForCommunication.Value,
                    ClientCertificate = certs.ServerCertificateForCommunication.Value,
                    ModifyDatabaseName = _ => sinkDbName
                });

                // Set up bidirectional pull replication with filtering on tickets/*
                var pullCert = new X509Certificate2(await File.ReadAllBytesAsync(certs.ClientCertificate2Path), password: (string)null, X509KeyStorageFlags.Exportable);
                var pullCertBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert));
                const string hubTaskOnA = "filtered-bidir-a";
                const string hubTaskOnC = "filtered-bidir-c";
                var allowedPaths = new[] { "tickets/*" };

                await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
                {
                    Name = hubTaskOnA,
                    Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                    WithFiltering = true,
                    MentorNode = serverA.ServerStore.NodeTag,
                    PinToMentorNode = true
                }));

                await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(hubTaskOnA,
                    new ReplicationHubAccess
                    {
                        Name = "SinkAccess-A",
                        CertificateBase64 = pullCertBase64,
                        AllowedHubToSinkPaths = allowedPaths,
                        AllowedSinkToHubPaths = allowedPaths
                    }));

                await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
                {
                    Name = hubTaskOnC,
                    Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                    WithFiltering = true,
                    MentorNode = serverC.ServerStore.NodeTag,
                    PinToMentorNode = true
                }));

                await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(hubTaskOnC,
                    new ReplicationHubAccess
                    {
                        Name = "SinkAccess-C",
                        CertificateBase64 = pullCertBase64,
                        AllowedHubToSinkPaths = allowedPaths,
                        AllowedSinkToHubPaths = allowedPaths
                    }));

                // Sink initially connects to node A
                await sinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(
                    new RavenConnectionString
                    {
                        Database = hubDbName,
                        Name = hubDbName + "ConStr",
                        TopologyDiscoveryUrls = [serverA.WebUrl]
                    }));

                await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
                {
                    ConnectionStringName = hubDbName + "ConStr",
                    Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                    CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                    HubName = hubTaskOnA,
                    AllowedHubToSinkPaths = allowedPaths,
                    AllowedSinkToHubPaths = allowedPaths
                }));

                // Phase 1: Write a seed document on A specifically, so the A -> C outgoing handler
                // has already advanced past the first etag before the controlled phase starts.
                using (var session = storeA.OpenAsyncSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(30), replicas: 2);
                    await session.StoreAsync(new User { Name = "Seed" }, id: "tickets/seed");
                    await session.SaveChangesAsync();
                }
                Assert.True(WaitForDocument(storeC, docId: "tickets/seed", timeout: 30_000));
                Assert.True(WaitForDocument(sinkStore, docId: "tickets/seed", timeout: 30_000));

                // Phase 2: Break outgoing replication on A and B, write documents on A
                var dbA = await GetDatabase(serverA, hubDbName);
                var dbB = await GetDatabase(serverB, hubDbName);

                var originalMaxItemsCountA = dbA.Configuration.Replication.MaxItemsCount;

                var mreA = new ManualResetEventSlim(initialState: false);
                dbA.ReplicationLoader.DebugWaitAndRunReplicationOnce = mreA;
                dbA.Configuration.Replication.MaxItemsCount = 1;

                var mreB = new ManualResetEventSlim(initialState: false);
                dbB.ReplicationLoader.DebugWaitAndRunReplicationOnce = mreB;

                // Write 20 non-filtered docs on A (bump A's etag significantly).
                for (int i = 1; i <= 20; i++)
                {
                    using (var session = storeA.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = $"Internal {i}" }, $"internal/{i}");
                        await session.SaveChangesAsync();
                    }
                }

                // Write 1 filtered doc on A (at high etag, after all internal/* docs)
                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Ticket 2" }, "tickets/2");
                    await session.SaveChangesAsync();
                }

                // Phase 3: Controlled send - release A's MRE once to send exactly 1 batch per destination
                // Internal replication (A -> B, A -> C): sends internal/1 (first unsynced item, low etag)
                // Hub-to-sink (A -> Sink): skips non-matching internal/*, sends tickets/2 (first matching, HIGH etag)
                // This creates the critical divergence: C knows A's DbId (from internal/1) but at a LOW value,
                // while the sink has tickets/2 with A's HIGH etag in its change vector.
                mreA.Set();

                Assert.True(WaitForDocument(storeC, "internal/1", timeout: 30_000));
                Assert.True(WaitForDocument(sinkStore, "tickets/2", timeout: 30_000));

                // A's handlers auto-blocked after 1 batch (they call Reset() and Wait() on the MRE)

                // Phase 4: Add a SECOND sink task that uses a hub definition pinned to C.
                // The connection string is only used for bootstrap; the actual responsible hub node is
                // selected by the hub definition topology lookup. Pinning this definition to C ensures
                // the sink-to-hub path really lands on C instead of being load-balanced back to A.
                await sinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(
                    new RavenConnectionString
                    {
                        Database = hubDbName,
                        Name = hubDbName + "ConStr-C",
                        TopologyDiscoveryUrls = [serverC.WebUrl]
                    }));

                await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
                {
                    ConnectionStringName = hubDbName + "ConStr-C",
                    Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink,
                    CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                    HubName = hubTaskOnC,
                    AllowedHubToSinkPaths = allowedPaths,
                    AllowedSinkToHubPaths = allowedPaths
                }));

                // The new task resolves to C and immediately starts sink-to-hub: sends tickets/seed
                // (AlreadyMerged on C) and then tickets/2 (NOT on C yet).
                // BUG: C's ReplaceUnknownEntriesWithSinkTag finds A's DbId as "known" in the incoming CV
                // of tickets/2 (since C received internal/1 from A in Phase 3, so dbA is in globalDbIds),
                // returns A:high_etag as changeVectorToMerge, which gets merged into C's database CV.
                // C's DB CV is now inflated: it claims to know about A's high etag, but C only has
                // documents up to A's low etag from internal replication.
                Assert.True(WaitForDocument(storeC, "tickets/2", timeout: 30_000));

                // Phase 5: Reproduce the bug under controlled conditions.
                // Keep B blocked and keep MaxItemsCount=1 on A until the missing document is proven.
                // This ensures A -> C first sends internal/2, then learns the inflated CV from C,
                // then jumps ahead and permanently skips the remaining backlog.
                //   - First batch: sends internal/2 to C
                //   - C responds with its DB CV (containing inflated A:high_etag)
                //   - A updates LastAcceptedChangeVector -> A:high_etag
                //   - Second batch: etag jump-ahead kicks in, skips to high_etag
                //   - internal/3-20 are PERMANENTLY SKIPPED
                dbA.ReplicationLoader.DebugWaitAndRunReplicationOnce = null;
                mreA.Set();

                // Write a marker document to confirm A -> C replication is functioning after all
                // temporary controls on A were relaxed.
                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Ticket 3" }, "tickets/3");
                    await session.SaveChangesAsync();
                }

                Assert.True(WaitForDocument(storeC, docId: "tickets/3", timeout: 30_000));

                // THE BUG: C is missing internal/* documents that were never replicated
                // due to database change vector inflation from sink-to-hub replication.
                // The etag jump-ahead optimization and/or ShouldSkip (AlreadyMerged) caused
                // these documents to be permanently skipped during internal replication A→C.
                using (var session = storeC.OpenAsyncSession())
                {
                    var missingDoc = await session.LoadAsync<User>("internal/10");
                    Assert.Null(missingDoc); // PROVES: internal/10 was skipped due to CV inflation
                }

                // Verify this is truly data loss: node A has the document
                using (var session = storeA.OpenAsyncSession())
                {
                    var existsOnA = await session.LoadAsync<User>("internal/10");
                    Assert.NotNull(existsOnA); // A has it, but C doesn't
                }

                // Phase 6: After the bug is proven, remove all artificial throttling/blocking and show
                // that replication is alive in general. We intentionally do NOT use the post-cleanup phase
                // to prove the missing gap, because once all controls are removed another node may later heal it.
                dbB.ReplicationLoader.DebugWaitAndRunReplicationOnce = null;
                dbA.Configuration.Replication.MaxItemsCount = originalMaxItemsCountA;
                mreB.Set();

                // Replication is still alive in general: create a NEW document outside A and make
                // sure A can receive it after all temporary controls were removed.
                using (var session = storeC.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "From C After Gap" }, "internal/from-c-after-gap");
                    await session.SaveChangesAsync();
                }

                Assert.True(WaitForDocument(storeA, docId: "internal/from-c-after-gap", timeout: 30_000));
            }
            finally
            {
                ReplicationInvestigationTrace.Reset();
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task Can_seed_persistent_recovery_dataset_with_mixed_consistency_states()
        {
            const int bulkScenarioSize = 3;
            const int mixedRun5ScenarioRepeatCount = 2;
            const int mixedRun1ScenarioRepeatCount = 2;
            const int totalDocumentsPerScenario = bulkScenarioSize + (5 * mixedRun5ScenarioRepeatCount) + mixedRun1ScenarioRepeatCount;
            var labRoot = Path.Combine(Path.GetTempPath(), $"replication-recovery-lab-{Guid.NewGuid():N}");
            Directory.CreateDirectory(labRoot);

            var customSettingsList = CreatePersistentClusterSettings(labRoot);
            var (hubNodes, hubLeader, certs) = await CreateRaftClusterWithSsl(numberOfNodes: 3, watcherCluster: true, shouldRunInMemory: false, customSettingsList: customSettingsList);
            var serverA = hubNodes[0];
            var serverB = hubNodes[1];
            var serverC = hubNodes[2];

            var hubDbName = GetDatabaseName();
            var diagnosticServerCertificatePath = Path.Combine(labRoot, "server-cert-for-communication.pfx");
            var usableClientCertificatePath = Path.Combine(labRoot, "usable-client-cert.pfx");
            var usableClientCertificatePasswordPath = Path.Combine(labRoot, "usable-client-cert.password.txt");
            var connectionInfoPath = Path.Combine(labRoot, "connection-info.txt");
            var tracePath = Path.Combine(labRoot, "replication-recovery-lab.trace.log");

            File.Copy(certs.ServerCertificateForCommunicationPath, diagnosticServerCertificatePath, overwrite: true);
            File.Copy(certs.ClientCertificate1Path, usableClientCertificatePath, overwrite: true);
            File.WriteAllText(usableClientCertificatePasswordPath, string.Empty);

            Certificates.RegisterClientCertificate(
                certs.ServerCertificateForCommunication.Value,
                certs.ClientCertificate1.Value,
                new Dictionary<string, DatabaseAccess>(),
                SecurityClearance.ClusterAdmin,
                server: hubLeader,
                certificateName: "recovery-lab-external-client");

            using var hubStore = GetDocumentStore(new Options
            {
                Server = hubLeader,
                ReplicationFactor = 3,
                RunInMemory = false,
                DeleteDatabaseOnDispose = false,
                AdminCertificate = certs.ServerCertificateForCommunication.Value,
                ClientCertificate = certs.ServerCertificateForCommunication.Value,
                ModifyDatabaseName = _ => hubDbName,
                CreateDatabase = true
            });

            var nodeStores = Cluster.GetDocumentStores(nodes: [serverA, serverB, serverC], hubDbName, disableTopologyUpdates: true, certificate: certs.ServerCertificateForCommunication.Value);
            var storeA = nodeStores[0];
            var storeB = nodeStores[1];
            var storeC = nodeStores[2];

            var dbA = await GetDatabase(serverA, hubDbName);
            var dbB = await GetDatabase(serverB, hubDbName);
            var dbC = await GetDatabase(serverC, hubDbName);
            var faultController = new ReplicationFaultController();
            var lab = new RecoveryLabClusterContext(
                hubDbName,
                serverA,
                serverB,
                serverC,
                dbA,
                dbB,
                dbC,
                storeA,
                storeB,
                storeC,
                faultController);

            ReplicationInvestigationTrace.Configure(tracePath, "recovery/", "lab/probe/");

            try
            {
                var scenarios = CreateScenarioCatalog();
                var initialConnectionInfo =
                    $"""
                     Recovery dataset seeding is starting.
                     Database: {hubDbName}
                     Lab root: {labRoot}

                     Connect now while the dataset is still being generated:
                     https://localhost:{new Uri(serverA.WebUrl).Port}
                     https://localhost:{new Uri(serverB.WebUrl).Port}
                     https://localhost:{new Uri(serverC.WebUrl).Port}

                     External client certificate (password: <empty>):
                     {usableClientCertificatePath}

                     Password file:
                     {usableClientCertificatePasswordPath}

                     Diagnostic server certificate:
                     {diagnosticServerCertificatePath}

                     Trace files:
                     Machine-readable: {tracePath}
                     Human-readable: {ReplicationInvestigationTrace.ReadablePath}

                     Planned scenario counts:
                     Repairable scenarios: {scenarios.Count(x => x.IsRepairable)}
                     Ambiguous scenarios: {scenarios.Count(x => x.IsRepairable == false)}
                     Documents per scenario (while debugging): {totalDocumentsPerScenario}

                     Planned index families:
                     Audit naming pattern: Recovery_Audit_<scenario>
                     Invalid naming pattern: Recovery_Invalid_<scenario>
                     Ambiguous naming pattern: Recovery_Ambiguous_<scenario>
                     Raw document oracle is authoritative; indexes are an operator-facing aggregate view.

                     Node URLs:
                     {serverA.WebUrl}
                     {serverB.WebUrl}
                     {serverC.WebUrl}

                     Node data directories:
                     A: {serverA.Configuration.Core.DataDirectory.FullPath}
                     B: {serverB.Configuration.Core.DataDirectory.FullPath}
                     C: {serverC.Configuration.Core.DataDirectory.FullPath}
                     """;

                File.WriteAllText(connectionInfoPath, initialConnectionInfo);
                Output.WriteLine(initialConnectionInfo);
                Console.WriteLine(initialConnectionInfo);

                await CreateScenarioValidationIndexesAsync(hubStore, scenarios);
                ConfigureFaultInjection(lab, faultController);
                await PrimeReplicationAsync(lab);

                var bulkPlans = BuildBulkPlans(scenarios, bulkScenarioSize);
                await SeedScenarioPlansAsync(lab, scenarios, bulkPlans, mixedExecution: false);

                var mixedRun5Plans = BuildMixedPlans(scenarios, batchKind: "mixed-run-5", runLength: 5, repeatsPerScenario: mixedRun5ScenarioRepeatCount);
                await SeedScenarioPlansAsync(lab, scenarios, mixedRun5Plans, mixedExecution: true);

                var mixedRun1Plans = BuildMixedPlans(scenarios, batchKind: "mixed-run-1", runLength: 1, repeatsPerScenario: mixedRun1ScenarioRepeatCount);
                await SeedScenarioPlansAsync(lab, scenarios, mixedRun1Plans, mixedExecution: true);

                ClearFaultInjection(lab);

                await AssertAllPlansAsync(lab, scenarios, bulkPlans, mixedRun5Plans, mixedRun1Plans);
                Indexes.WaitForIndexing(storeA, databaseName: hubDbName, timeout: TimeSpan.FromMinutes(5), nodeTag: "A");
                Indexes.WaitForIndexing(storeB, databaseName: hubDbName, timeout: TimeSpan.FromMinutes(5), nodeTag: "B");
                Indexes.WaitForIndexing(storeC, databaseName: hubDbName, timeout: TimeSpan.FromMinutes(5), nodeTag: "C");
                await AssertScenarioIndexCountsAsync(lab, scenarios, totalDocumentsPerScenario);
                await RunLiveClusterProbeAsync(lab);

                var scenarioIndexMatrixPath = Path.Combine(labRoot, "scenario-index-matrix.md");
                var scenarioIndexMatrix = CreateScenarioIndexMatrix(
                    scenarios,
                    totalDocumentsPerScenario);
                File.WriteAllText(scenarioIndexMatrixPath, scenarioIndexMatrix);

                Servers.Remove(serverA);
                Servers.Remove(serverB);
                Servers.Remove(serverC);

                var connectionInfo =
                    $"""
                     Recovery dataset is ready.
                     Database: {hubDbName}
                     Lab root: {labRoot}

                     Recommended URLs:
                     https://localhost:{new Uri(serverA.WebUrl).Port}
                     https://localhost:{new Uri(serverB.WebUrl).Port}
                     https://localhost:{new Uri(serverC.WebUrl).Port}

                     External client certificate (password: <empty>):
                     {usableClientCertificatePath}

                     Password file:
                     {usableClientCertificatePasswordPath}

                     Diagnostic server certificate:
                     {diagnosticServerCertificatePath}

                     Trace files:
                     Machine-readable: {tracePath}
                     Human-readable: {ReplicationInvestigationTrace.ReadablePath}

                     Scenario counts:
                     Repairable scenarios: {scenarios.Count(x => x.IsRepairable)}
                     Ambiguous scenarios: {scenarios.Count(x => x.IsRepairable == false)}
                     Documents per scenario: {totalDocumentsPerScenario}

                     Scenario indexes:
                     Audit index count: {scenarios.Count}
                     Invalid index count: {scenarios.Count(x => x.IsRepairable)}
                     Ambiguous index count: {scenarios.Count(x => x.IsRepairable == false)}
                     Audit naming pattern: Recovery_Audit_<scenario>
                     Invalid naming pattern: Recovery_Invalid_<scenario>
                     Ambiguous naming pattern: Recovery_Ambiguous_<scenario>
                     Audit example: {GetAuditIndexName(scenarios[0].Name)}
                     Check example: {GetCheckIndexName(scenarios[0])}
                     Raw document oracle is authoritative; indexes are an operator-facing aggregate view.

                     Scenario index matrix:
                     {scenarioIndexMatrixPath}

                     Node URLs:
                     {serverA.WebUrl}
                     {serverB.WebUrl}
                     {serverC.WebUrl}

                     Node data directories:
                     A: {serverA.Configuration.Core.DataDirectory.FullPath}
                     B: {serverB.Configuration.Core.DataDirectory.FullPath}
                     C: {serverC.Configuration.Core.DataDirectory.FullPath}
                     """;

                File.WriteAllText(connectionInfoPath, connectionInfo);

                var readyMessage =
                    $"""
                     {connectionInfo}

                     Dataset summary:
                     Bulk scenarios: {scenarios.Count} x {bulkScenarioSize} docs
                     Mixed batch `mixed-run-5`: {mixedRun5Plans.Count} docs
                     Mixed batch `mixed-run-1`: {mixedRun1Plans.Count} docs

                     Matrix usage:
                     1. Open a node-pinned store to A, B, or C.
                     2. Query `Recovery_Audit_<scenario>` to inspect the local copies and their change vectors.
                     3. Query `Recovery_Invalid_<scenario>` for repairable scenarios.
                     4. Query `Recovery_Ambiguous_<scenario>` for ambiguous negative controls.
                     5. Compare the node-local counts to scenario-index-matrix.md.
                     6. After recovery, repairable invalid indexes should be empty on all three nodes.
                     """;

                Output.WriteLine(readyMessage);
                Console.WriteLine(readyMessage);

                await Task.Delay(Timeout.InfiniteTimeSpan);
            }
            finally
            {
                ClearFaultInjection(lab);
                ReplicationInvestigationTrace.Reset();
            }
        }

        private async Task PrimeReplicationAsync(RecoveryLabClusterContext lab)
        {
            await StoreMarkerDocumentAsync(lab.Stores["A"], "internal/bootstrap/prime-from-a", "prime-from-a");
            Assert.True(WaitForDocument(lab.Stores["B"], "internal/bootstrap/prime-from-a", timeout: 60_000));
            Assert.True(WaitForDocument(lab.Stores["C"], "internal/bootstrap/prime-from-a", timeout: 60_000));

            await StoreMarkerDocumentAsync(lab.Stores["B"], "internal/bootstrap/prime-from-b", "prime-from-b");
            Assert.True(WaitForDocument(lab.Stores["A"], "internal/bootstrap/prime-from-b", timeout: 60_000));
            Assert.True(WaitForDocument(lab.Stores["C"], "internal/bootstrap/prime-from-b", timeout: 60_000));

            await StoreMarkerDocumentAsync(lab.Stores["C"], "internal/bootstrap/prime-from-c", "prime-from-c");
            Assert.True(WaitForDocument(lab.Stores["A"], "internal/bootstrap/prime-from-c", timeout: 60_000));
            Assert.True(WaitForDocument(lab.Stores["B"], "internal/bootstrap/prime-from-c", timeout: 60_000));
        }

        private static void ConfigureFaultInjection(RecoveryLabClusterContext lab, ReplicationFaultController faultController)
        {
            foreach (var database in lab.Databases.Values)
                database.ReplicationLoader.ForTestingPurposesOnly().OutgoingFaultController = faultController;
        }

        private static void ClearFaultInjection(RecoveryLabClusterContext lab)
        {
            foreach (var database in lab.Databases.Values)
                database.ReplicationLoader.ForTestingPurposesOnly().OutgoingFaultController = null;
        }

        private List<ScenarioDefinition> CreateScenarioCatalog()
        {
            return
            [
                CreateRepairableScenario("only-a", "single-node", "A", 1, "A",
                    CreateStep(ScenarioStepKind.Create, "A", 1, "A", "B", "C")),

                CreateRepairableScenario("only-b", "single-node", "B", 1, "B",
                    CreateStep(ScenarioStepKind.Create, "B", 1, "B", "A", "C")),

                CreateRepairableScenario("only-c", "single-node", "C", 1, "C",
                    CreateStep(ScenarioStepKind.Create, "C", 1, "C", "A", "B")),


                CreateRepairableScenario("ab-consistent-missing-c", "two-node-consistent", "AB", 1, "A",
                    CreateStep(ScenarioStepKind.Create, "A", 1, "A", "C")),

                CreateRepairableScenario("ac-consistent-missing-b", "two-node-consistent", "AC", 1, "A",
                    CreateStep(ScenarioStepKind.Create, "A", 1, "A", "B")),

                CreateRepairableScenario("bc-consistent-missing-a", "two-node-consistent", "BC", 1, "B",
                    CreateStep(ScenarioStepKind.Create, "B", 1, "B", "A")),


                CreateRepairableScenario("ab-inconsistent-a-wins-missing-c", "two-node-inconsistent", "A", 2, "A",
                    CreateStep(ScenarioStepKind.Create, "B", 1, "B", "C"),
                    CreateStep(ScenarioStepKind.Update, "A", 2, "A", "B", "C")),

                CreateRepairableScenario("ab-inconsistent-b-wins-missing-c", "two-node-inconsistent", "B", 2, "B",
                    CreateStep(ScenarioStepKind.Create, "A", 1, "A", "C"),
                    CreateStep(ScenarioStepKind.Update, "B", 2, "B", "A", "C")),

                CreateRepairableScenario("ac-inconsistent-a-wins-missing-b", "two-node-inconsistent", "A", 2, "A",
                    CreateStep(ScenarioStepKind.Create, "C", 1, "C", "B"),
                    CreateStep(ScenarioStepKind.Update, "A", 2, "A", "B", "C")),

                CreateRepairableScenario("ac-inconsistent-c-wins-missing-b", "two-node-inconsistent", "C", 2, "C",
                    CreateStep(ScenarioStepKind.Create, "A", 1, "A", "B"),
                    CreateStep(ScenarioStepKind.Update, "C", 2, "C", "A", "B")),

                CreateRepairableScenario("bc-inconsistent-b-wins-missing-a", "two-node-inconsistent", "B", 2, "B",
                    CreateStep(ScenarioStepKind.Create, "C", 1, "C", "A"),
                    CreateStep(ScenarioStepKind.Update, "B", 2, "B", "A", "C")),

                CreateRepairableScenario("bc-inconsistent-c-wins-missing-a", "two-node-inconsistent", "C", 2, "C",
                    CreateStep(ScenarioStepKind.Create, "B", 1, "B", "A"),
                    CreateStep(ScenarioStepKind.Update, "C", 2, "C", "A", "B")),


                CreateRepairableScenario("all-three-a-wins-one-stale-b", "three-node-one-stale", "A", 2, "A",
                    CreateStep(ScenarioStepKind.Create, "C", 1, "C"),
                    CreateStep(ScenarioStepKind.Update, "A", 2, "A", "B")),

                CreateRepairableScenario("all-three-a-wins-one-stale-c", "three-node-one-stale", "A", 2, "A",
                    CreateStep(ScenarioStepKind.Create, "B", 1, "B"),
                    CreateStep(ScenarioStepKind.Update, "A", 2, "A", "C")),

                CreateRepairableScenario("all-three-b-wins-one-stale-a", "three-node-one-stale", "B", 2, "B",
                    CreateStep(ScenarioStepKind.Create, "C", 1, "C"),
                    CreateStep(ScenarioStepKind.Update, "B", 2, "B", "A")),

                CreateRepairableScenario("all-three-b-wins-one-stale-c", "three-node-one-stale", "B", 2, "B",
                    CreateStep(ScenarioStepKind.Create, "A", 1, "A"),
                    CreateStep(ScenarioStepKind.Update, "B", 2, "B", "C")),

                CreateRepairableScenario("all-three-c-wins-one-stale-a", "three-node-one-stale", "C", 2, "C",
                    CreateStep(ScenarioStepKind.Create, "B", 1, "B"),
                    CreateStep(ScenarioStepKind.Update, "C", 2, "C", "A")),

                CreateRepairableScenario("all-three-c-wins-one-stale-b", "three-node-one-stale", "C", 2, "C",
                    CreateStep(ScenarioStepKind.Create, "A", 1, "A"),
                    CreateStep(ScenarioStepKind.Update, "C", 2, "C", "B")),


                CreateRepairableScenario("all-three-a-wins-two-stale", "three-node-two-stale", "A", 2, "A",
                    CreateStep(ScenarioStepKind.Create, "C", 1, "C"),
                    CreateStep(ScenarioStepKind.Update, "A", 2, "A", "B", "C")),

                CreateRepairableScenario("all-three-b-wins-two-stale", "three-node-two-stale", "B", 2, "B",
                    CreateStep(ScenarioStepKind.Create, "A", 1, "A"),
                    CreateStep(ScenarioStepKind.Update, "B", 2, "B", "A", "C")),

                CreateRepairableScenario("all-three-c-wins-two-stale", "three-node-two-stale", "C", 2, "C",
                    CreateStep(ScenarioStepKind.Create, "B", 1, "B"),
                    CreateStep(ScenarioStepKind.Update, "C", 2, "C", "A", "B")),


                CreateRepairableScenario("all-three-all-different-a-wins", "three-node-all-different", "A", 3, "A",
                    CreateStep(ScenarioStepKind.Create, "C", 1, "C"),
                    CreateStep(ScenarioStepKind.Update, "B", 2, "B", "C"),
                    CreateStep(ScenarioStepKind.Update, "A", 3, "A", "B", "C")),

                CreateRepairableScenario("all-three-all-different-b-wins", "three-node-all-different", "B", 3, "B",
                    CreateStep(ScenarioStepKind.Create, "A", 1, "A"),
                    CreateStep(ScenarioStepKind.Update, "C", 2, "C", "A"),
                    CreateStep(ScenarioStepKind.Update, "B", 3, "B", "A", "C")),

                CreateRepairableScenario("all-three-all-different-c-wins", "three-node-all-different", "C", 3, "C",
                    CreateStep(ScenarioStepKind.Create, "B", 1, "B"),
                    CreateStep(ScenarioStepKind.Update, "A", 2, "A", "B"),
                    CreateStep(ScenarioStepKind.Update, "C", 3, "C", "A", "B")),


                CreateRepairableScenario("all-three-tie-ab-stale-c", "three-node-tie", "AB", 2, "A",
                    CreateStep(ScenarioStepKind.Create, "C", 1, "C"),
                    CreateStep(ScenarioStepKind.Update, "A", 2, "A", "C")),

                CreateRepairableScenario("all-three-tie-ac-stale-b", "three-node-tie", "AC", 2, "A",
                    CreateStep(ScenarioStepKind.Create, "B", 1, "B"),
                    CreateStep(ScenarioStepKind.Update, "A", 2, "A", "B")),

                CreateRepairableScenario("all-three-tie-bc-stale-a", "three-node-tie", "BC", 2, "B",
                    CreateStep(ScenarioStepKind.Create, "A", 1, "A"),
                    CreateStep(ScenarioStepKind.Update, "B", 2, "B", "A")),


                CreateAmbiguousScenario("ab-ambiguous-a-vs-b-missing-c", "two-node-ambiguous",
                    CreateStep(ScenarioStepKind.Create, "A", 1, "A", "B", "C"),
                    CreateStep(ScenarioStepKind.Create, "B", 1, "B", "A", "C")),

                CreateAmbiguousScenario("ac-ambiguous-a-vs-c-missing-b", "two-node-ambiguous",
                    CreateStep(ScenarioStepKind.Create, "A", 1, "A", "B", "C"),
                    CreateStep(ScenarioStepKind.Create, "C", 1, "C", "A", "B")),

                CreateAmbiguousScenario("bc-ambiguous-b-vs-c-missing-a", "two-node-ambiguous",
                    CreateStep(ScenarioStepKind.Create, "B", 1, "B", "A", "C"),
                    CreateStep(ScenarioStepKind.Create, "C", 1, "C", "A", "B")),


                CreateAmbiguousScenario("all-three-ambiguous-a-vs-b-base-c", "three-node-ambiguous",
                    CreateStep(ScenarioStepKind.Create, "C", 1, "C"),
                    CreateStep(ScenarioStepKind.Update, "A", 2, "A", "B", "C"),
                    CreateStep(ScenarioStepKind.Update, "B", 2, "B", "A", "C")),

                CreateAmbiguousScenario("all-three-ambiguous-a-vs-c-base-b", "three-node-ambiguous",
                    CreateStep(ScenarioStepKind.Create, "B", 1, "B"),
                    CreateStep(ScenarioStepKind.Update, "A", 2, "A", "B", "C"),
                    CreateStep(ScenarioStepKind.Update, "C", 2, "C", "A", "B")),

                CreateAmbiguousScenario("all-three-ambiguous-b-vs-c-base-a", "three-node-ambiguous",
                    CreateStep(ScenarioStepKind.Create, "A", 1, "A"),
                    CreateStep(ScenarioStepKind.Update, "B", 2, "B", "A", "C"),
                    CreateStep(ScenarioStepKind.Update, "C", 2, "C", "A", "B"))
            ];
        }

        private ScenarioDefinition CreateRepairableScenario(string name, string scenarioGroup, string expectedWinnerNode, int expectedWinnerVersion, string expectedWinnerWrittenBy, params ScenarioStepDefinition[] steps) =>
            CreateScenario(name, scenarioGroup, RepairPolicyRepairable, expectedWinnerNode, expectedWinnerVersion, expectedWinnerWrittenBy, steps);

        private ScenarioDefinition CreateAmbiguousScenario(string name, string scenarioGroup, params ScenarioStepDefinition[] steps) =>
            CreateScenario(name, scenarioGroup, RepairPolicyAmbiguousSkip, null, null, null, steps);

        private ScenarioDefinition CreateScenario(string name, string scenarioGroup, string repairPolicy, string expectedWinnerNode, int? expectedWinnerVersion, string expectedWinnerWrittenBy, params ScenarioStepDefinition[] steps)
        {
            var simulation = SimulateScenarioStates(steps);

            return new ScenarioDefinition
            {
                Name = name,
                ScenarioGroup = scenarioGroup,
                RepairPolicy = repairPolicy,
                ExpectedWinnerNode = expectedWinnerNode,
                ExpectedWinnerVersion = expectedWinnerVersion,
                ExpectedWinnerWrittenBy = expectedWinnerWrittenBy,
                SeedPattern = BuildSeedPattern(simulation.FinalStates),
                ValidatedIn = ValidatedInAllBatchKinds,
                Steps = steps.ToList(),
                StatesBeforeEachStep = simulation.StatesBeforeEachStep,
                StatesAfterEachStep = simulation.StatesAfterEachStep,
                FinalStates = simulation.FinalStates,
                CvTopology = BuildExpectedCvTopology(scenarioGroup, repairPolicy, expectedWinnerNode, simulation.FinalStates)
            };
        }

        private ExpectedCvTopology BuildExpectedCvTopology(
            string scenarioGroup,
            string repairPolicy,
            string expectedWinnerNode,
            IReadOnlyDictionary<string, SimulatedNodeState> finalStates)
        {
            var groups = finalStates
                .Where(x => x.Value.Exists)
                .GroupBy(x => x.Value.LastActionOrdinal)
                .Select(group =>
                {
                    var nodeTags = group
                        .Select(x => x.Key)
                        .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
                        .ToArray();

                    return new ExpectedCvGroup
                    {
                        GroupKey = CreateGroupKey(nodeTags),
                        NodeTags = nodeTags,
                        ActionOrdinal = group.Key
                    };
                })
                .OrderBy(x => x.ActionOrdinal)
                .ToList();

            var topology = new ExpectedCvTopology
            {
                ExpectedExistingNodes = finalStates
                    .Where(x => x.Value.Exists)
                    .Select(x => x.Key)
                    .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
                    .ToArray(),
                Groups = groups,
                GroupKeys = groups
                    .Select(x => x.GroupKey)
                    .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
                    .ToArray(),
                ExpectAmbiguous = string.Equals(repairPolicy, RepairPolicyAmbiguousSkip, StringComparison.Ordinal)
            };

            if (topology.ExpectAmbiguous == false)
            {
                topology.DominantGroupKey = ResolveDominantGroupKey(expectedWinnerNode, groups);
                foreach (var group in groups.Where(x => string.Equals(x.GroupKey, topology.DominantGroupKey, StringComparison.OrdinalIgnoreCase) == false))
                {
                    topology.DominancePairs.Add(new ExpectedCvRelation
                    {
                        LeftGroupKey = topology.DominantGroupKey,
                        RightGroupKey = group.GroupKey
                    });
                }

                if (string.Equals(scenarioGroup, "three-node-all-different", StringComparison.OrdinalIgnoreCase) && groups.Count == 3)
                {
                    topology.DominancePairs.Add(new ExpectedCvRelation
                    {
                        LeftGroupKey = groups[1].GroupKey,
                        RightGroupKey = groups[0].GroupKey
                    });
                }

                return topology;
            }

            if (string.Equals(scenarioGroup, "two-node-ambiguous", StringComparison.OrdinalIgnoreCase) && groups.Count == 2)
            {
                topology.IncomparablePairs.Add(new ExpectedCvRelation
                {
                    LeftGroupKey = groups[0].GroupKey,
                    RightGroupKey = groups[1].GroupKey
                });
            }

            if (string.Equals(scenarioGroup, "three-node-ambiguous", StringComparison.OrdinalIgnoreCase) && groups.Count >= 2)
            {
                var baseGroup = groups[0];
                var competitorGroups = groups.Skip(1).ToArray();

                foreach (var competitorGroup in competitorGroups)
                {
                    topology.DominancePairs.Add(new ExpectedCvRelation
                    {
                        LeftGroupKey = competitorGroup.GroupKey,
                        RightGroupKey = baseGroup.GroupKey
                    });
                }

                if (competitorGroups.Length == 2)
                {
                    topology.IncomparablePairs.Add(new ExpectedCvRelation
                    {
                        LeftGroupKey = competitorGroups[0].GroupKey,
                        RightGroupKey = competitorGroups[1].GroupKey
                    });
                }
            }

            return topology;
        }

        private string ResolveDominantGroupKey(string expectedWinnerNode, IReadOnlyList<ExpectedCvGroup> groups)
        {
            if (string.IsNullOrWhiteSpace(expectedWinnerNode))
                return null;

            var winnerNodeTags = expectedWinnerNode
                .Where(char.IsLetter)
                .Select(ch => ch.ToString())
                .ToArray();

            var dominantGroup = groups.Single(group => winnerNodeTags.All(winnerTag => group.NodeTags.Contains(winnerTag, StringComparer.OrdinalIgnoreCase)));
            return dominantGroup.GroupKey;
        }

        private ScenarioStepDefinition CreateStep(ScenarioStepKind kind, string writerNode, int version, string writtenBy, params string[] skippedTargets) =>
            new(kind, writerNode, version, writtenBy, skippedTargets);

        private ScenarioSimulation SimulateScenarioStates(IReadOnlyList<ScenarioStepDefinition> steps)
        {
            var currentStates = CreateEmptyNodeStates();
            var beforeStates = new List<Dictionary<string, SimulatedNodeState>>(steps.Count);
            var afterStates = new List<Dictionary<string, SimulatedNodeState>>(steps.Count);

            for (var stepIndex = 0; stepIndex < steps.Count; stepIndex++)
            {
                var step = steps[stepIndex];
                beforeStates.Add(CloneStates(currentStates));

                var writerState = currentStates[step.WriterNode];
                if (step.Kind == ScenarioStepKind.Create && writerState.Exists)
                    throw new InvalidOperationException($"Scenario '{step.WriterNode}' create step '{stepIndex + 1}' expected a missing document on the writer.");

                if (step.Kind == ScenarioStepKind.Update && writerState.Exists == false)
                    throw new InvalidOperationException($"Scenario '{step.WriterNode}' update step '{stepIndex + 1}' expected the document to already exist on the writer.");

                var replicatedState = new SimulatedNodeState
                {
                    Exists = true,
                    Version = step.Version,
                    WrittenBy = step.WrittenBy,
                    LastActionOrdinal = stepIndex + 1
                };

                currentStates[step.WriterNode] = replicatedState.Clone();

                foreach (var nodeTag in OrderedNodeTags)
                {
                    if (string.Equals(nodeTag, step.WriterNode, StringComparison.OrdinalIgnoreCase))
                        continue;

                    if (step.SkippedTargets.Contains(nodeTag, StringComparer.OrdinalIgnoreCase))
                        continue;

                    currentStates[nodeTag] = replicatedState.Clone();
                }

                afterStates.Add(CloneStates(currentStates));
            }

            return new ScenarioSimulation(beforeStates, afterStates, CloneStates(currentStates));
        }

        private static Dictionary<string, SimulatedNodeState> CreateEmptyNodeStates() =>
            OrderedNodeTags.ToDictionary(
                nodeTag => nodeTag,
                _ => new SimulatedNodeState(),
                StringComparer.OrdinalIgnoreCase);

        private static Dictionary<string, SimulatedNodeState> CloneStates(IReadOnlyDictionary<string, SimulatedNodeState> states) =>
            states.ToDictionary(
                kvp => kvp.Key,
                kvp => kvp.Value.Clone(),
                StringComparer.OrdinalIgnoreCase);

        private static string BuildSeedPattern(IReadOnlyDictionary<string, SimulatedNodeState> finalStates) =>
            string.Join("|", OrderedNodeTags.Select(nodeTag =>
            {
                var state = finalStates[nodeTag];
                return state.Exists ? $"{nodeTag}:v{state.Version}/{state.WrittenBy}" : $"{nodeTag}:missing";
            }));

        private List<RecoveryDocumentPlan> BuildBulkPlans(IReadOnlyList<ScenarioDefinition> scenarios, int documentsPerScenario)
        {
            var plans = new List<RecoveryDocumentPlan>(scenarios.Count * documentsPerScenario);
            var sequence = 0;

            foreach (var scenario in scenarios)
            {
                for (var scenarioSequence = 1; scenarioSequence <= documentsPerScenario; scenarioSequence++)
                {
                    sequence++;
                    plans.Add(CreateDocumentPlan(
                        scenario,
                        batchKind: "bulk",
                        batchSequence: sequence,
                        scenarioSequence: scenarioSequence,
                        id: GetRecoveryDocumentId("bulk", scenario.Name, scenarioSequence)));
                }
            }

            return plans;
        }

        private List<RecoveryDocumentPlan> BuildMixedPlans(IReadOnlyList<ScenarioDefinition> scenarios, string batchKind, int runLength, int repeatsPerScenario)
        {
            var plans = new List<RecoveryDocumentPlan>();
            var scenarioSequences = scenarios.ToDictionary(x => x.Name, _ => 0, StringComparer.OrdinalIgnoreCase);
            var batchSequence = 0;

            for (var repeat = 0; repeat < repeatsPerScenario; repeat++)
            {
                foreach (var scenario in scenarios)
                {
                    for (var runItem = 0; runItem < runLength; runItem++)
                    {
                        batchSequence++;
                        scenarioSequences[scenario.Name]++;

                        plans.Add(CreateDocumentPlan(
                            scenario,
                            batchKind,
                            batchSequence,
                            scenarioSequences[scenario.Name],
                            GetRecoveryDocumentId(batchKind, batchSequence)));
                    }
                }
            }

            return plans;
        }

        private static RecoveryDocumentPlan CreateDocumentPlan(ScenarioDefinition scenario, string batchKind, int batchSequence, int scenarioSequence, string id)
        {
            return new RecoveryDocumentPlan
            {
                Id = id,
                BatchKind = batchKind,
                Sequence = batchSequence,
                Scenario = scenario.Name,
                ScenarioGroup = scenario.ScenarioGroup,
                ScenarioSequence = scenarioSequence,
                RepairPolicy = scenario.RepairPolicy,
                ExpectedWinnerNode = scenario.ExpectedWinnerNode,
                ExpectedWinnerVersion = scenario.ExpectedWinnerVersion,
                ExpectedWinnerWrittenBy = scenario.ExpectedWinnerWrittenBy,
                SeedPattern = scenario.SeedPattern
            };
        }

        private async Task SeedScenarioPlansAsync(RecoveryLabClusterContext lab, IReadOnlyList<ScenarioDefinition> scenarios, IReadOnlyList<RecoveryDocumentPlan> plans, bool mixedExecution)
        {
            var scenariosByName = scenarios.ToDictionary(x => x.Name, StringComparer.OrdinalIgnoreCase);

            if (mixedExecution)
            {
                foreach (var plan in plans.OrderBy(x => x.Sequence))
                {
                    var scenario = scenariosByName[plan.Scenario];
                    var chunk = new[] { plan };
                    await ExecuteScenarioChunkAsync(lab, scenario, chunk, writeChunkSize: 1);
                    await AssertPlansChunkAsync(lab, scenario, chunk);
                }

                return;
            }

            foreach (var scenario in scenarios)
            {
                var scenarioPlans = plans
                    .Where(x => string.Equals(x.Scenario, scenario.Name, StringComparison.OrdinalIgnoreCase))
                    .OrderBy(x => x.ScenarioSequence)
                    .ToList();

                for (var start = 0; start < scenarioPlans.Count; start += 128)
                {
                    var chunk = scenarioPlans.Skip(start).Take(128).ToArray();
                    await ExecuteScenarioChunkAsync(lab, scenario, chunk, writeChunkSize: 128);
                    await AssertPlansChunkAsync(lab, scenario, chunk);
                }
            }
        }

        private async Task ExecuteScenarioChunkAsync(RecoveryLabClusterContext lab, ScenarioDefinition scenario, IReadOnlyList<RecoveryDocumentPlan> plans, int writeChunkSize)
        {
            var documentIds = plans.Select(x => x.Id).ToArray();
            var heartbeatSuppressionHandles = new List<ReplicationHeartbeatSuppressionHandle>();
            var suppressedHeartbeatLinks = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var persistentRuleHandles = new List<ReplicationFaultRuleHandle>();
            var progressTargetsBySource = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

            try
            {
                for (var stepIndex = 0; stepIndex < scenario.Steps.Count; stepIndex++)
                {
                    var step = scenario.Steps[stepIndex];
                    var directRuleHandles = new List<ReplicationFaultRuleHandle>();
                    var relayRuleHandles = new List<ReplicationFaultRuleHandle>();

                    try
                    {
                        var skippedTargets = step.SkippedTargets
                            .Distinct(StringComparer.OrdinalIgnoreCase)
                            .ToArray();
                        var sourceEtagsBeforeStep = OrderedNodeTags.ToDictionary(
                            nodeTag => nodeTag,
                            nodeTag => lab.Databases[nodeTag].ReadLastEtag(),
                            StringComparer.OrdinalIgnoreCase);
                        var relaySourceTags = OrderedNodeTags
                            .Where(nodeTag => string.Equals(nodeTag, step.WriterNode, StringComparison.OrdinalIgnoreCase) == false)
                            .Where(nodeTag => skippedTargets.Contains(nodeTag, StringComparer.OrdinalIgnoreCase) == false)
                            .Where(nodeTag => scenario.StatesAfterEachStep[stepIndex][nodeTag].Exists)
                            .ToArray();

                        foreach (var targetNodeTag in skippedTargets)
                        {
                            var directRuleHandle = lab.FaultController.ArmSkipAndAdvance(
                                lab.DatabaseName,
                                step.WriterNode,
                                targetNodeTag,
                                sourceEtagsBeforeStep[step.WriterNode],
                                documentIds,
                                $"{scenario.Name}/{plans[0].BatchKind}/step-{stepIndex + 1}:writer:{step.WriterNode}->{targetNodeTag}");

                            if (scenario.IsRepairable)
                                directRuleHandles.Add(directRuleHandle);
                            else
                            {
                                persistentRuleHandles.Add(directRuleHandle);
                                AddProgressTarget(progressTargetsBySource, step.WriterNode, targetNodeTag);
                            }

                            if (scenario.IsRepairable == false)
                            {
                                var heartbeatKey = $"{step.WriterNode}->{targetNodeTag}";
                                if (suppressedHeartbeatLinks.Add(heartbeatKey))
                                {
                                    heartbeatSuppressionHandles.Add(lab.FaultController.ArmHeartbeatSuppression(
                                        lab.DatabaseName,
                                        step.WriterNode,
                                        targetNodeTag,
                                        $"{scenario.Name}/{plans[0].BatchKind}/step-{stepIndex + 1}:writer:{step.WriterNode}->{targetNodeTag}"));
                                }
                            }
                        }

                        foreach (var sourceNodeTag in relaySourceTags)
                        {
                            foreach (var targetNodeTag in skippedTargets)
                            {
                                var relayRuleHandle = lab.FaultController.ArmSkipAndAdvance(
                                    lab.DatabaseName,
                                    sourceNodeTag,
                                    targetNodeTag,
                                    sourceEtagsBeforeStep[sourceNodeTag],
                                    documentIds,
                                    $"{scenario.Name}/{plans[0].BatchKind}/step-{stepIndex + 1}:relay:{sourceNodeTag}->{targetNodeTag}");

                                if (scenario.IsRepairable)
                                {
                                    relayRuleHandles.Add(relayRuleHandle);
                                }
                                else
                                {
                                    persistentRuleHandles.Add(relayRuleHandle);
                                    AddProgressTarget(progressTargetsBySource, sourceNodeTag, targetNodeTag);

                                    var heartbeatKey = $"{sourceNodeTag}->{targetNodeTag}";
                                    if (suppressedHeartbeatLinks.Add(heartbeatKey))
                                    {
                                        heartbeatSuppressionHandles.Add(lab.FaultController.ArmHeartbeatSuppression(
                                            lab.DatabaseName,
                                            sourceNodeTag,
                                            targetNodeTag,
                                            $"{scenario.Name}/{plans[0].BatchKind}/step-{stepIndex + 1}:relay:{sourceNodeTag}->{targetNodeTag}"));
                                    }
                                }
                            }
                        }

                        var expectedWriterState = scenario.StatesBeforeEachStep[stepIndex][step.WriterNode];
                        await WriteScenarioChunkAsync(lab.Stores[step.WriterNode], plans, step, expectedWriterState, writeChunkSize);

                        foreach (var nodeTag in OrderedNodeTags)
                        {
                            if (string.Equals(nodeTag, step.WriterNode, StringComparison.OrdinalIgnoreCase))
                                continue;

                            if (step.SkippedTargets.Contains(nodeTag, StringComparer.OrdinalIgnoreCase))
                                continue;

                            await WaitForChunkStateAsync(lab.Stores[nodeTag], documentIds, scenario.StatesAfterEachStep[stepIndex][nodeTag]);
                        }

                        if (scenario.IsRepairable)
                        {
                            // Ambiguous negative controls must not teach competing nodes a newer writer etag
                            // via an unrestricted trigger write, or one side can become dominant by construction.
                            var progressSourceTags = relaySourceTags
                                .Append(step.WriterNode)
                                .Distinct(StringComparer.OrdinalIgnoreCase)
                                .ToArray();
                            var progressTriggerIds = await TriggerReplicationProgressAsync(lab, progressSourceTags, scenario.Name, plans[0].BatchKind, stepIndex, plans[0].Sequence);

                            foreach (var sourceNodeTag in progressSourceTags)
                            {
                                var progressTriggerId = progressTriggerIds[sourceNodeTag];
                                foreach (var targetNodeTag in skippedTargets)
                                {
                                    Assert.True(
                                        WaitForDocument(lab.Stores[targetNodeTag], progressTriggerId, timeout: 120_000),
                                        $"Expected progress trigger '{progressTriggerId}' from {sourceNodeTag} to arrive on {targetNodeTag} for scenario '{scenario.Name}', batch '{plans[0].BatchKind}', step {stepIndex + 1}.");
                                }
                            }
                        }

                        await WaitForPartiallyMatchedRulesToCompleteAsync(directRuleHandles);
                        await WaitForPartiallyMatchedRulesToCompleteAsync(relayRuleHandles);

                        foreach (var targetNodeTag in skippedTargets)
                            await WaitForChunkStateAsync(
                                lab.Stores[targetNodeTag],
                                documentIds,
                                scenario.StatesAfterEachStep[stepIndex][targetNodeTag],
                                stabilityWindow: TimeSpan.FromSeconds(1));
                    }
                    finally
                    {
                        if (scenario.IsRepairable)
                        {
                            foreach (var ruleHandle in directRuleHandles)
                                ruleHandle.Dispose();

                            foreach (var ruleHandle in relayRuleHandles)
                                ruleHandle.Dispose();
                        }
                    }
                }

                if (scenario.IsRepairable == false && progressTargetsBySource.Count > 0)
                {
                    var progressSourceTags = OrderedNodeTags
                        .Where(progressTargetsBySource.ContainsKey)
                        .ToArray();
                    var progressTriggerIds = await TriggerReplicationProgressAsync(lab, progressSourceTags, scenario.Name, plans[0].BatchKind, scenario.Steps.Count, plans[0].Sequence);

                    foreach (var sourceNodeTag in progressSourceTags)
                    {
                        var progressTriggerId = progressTriggerIds[sourceNodeTag];
                        foreach (var targetNodeTag in progressTargetsBySource[sourceNodeTag])
                        {
                            Assert.True(
                                WaitForDocument(lab.Stores[targetNodeTag], progressTriggerId, timeout: 120_000),
                                $"Expected final ambiguous progress trigger '{progressTriggerId}' from {sourceNodeTag} to arrive on {targetNodeTag} for scenario '{scenario.Name}', batch '{plans[0].BatchKind}'.");
                        }
                    }

                    await WaitForPartiallyMatchedRulesToCompleteAsync(persistentRuleHandles);

                    foreach (var nodeTag in OrderedNodeTags)
                    {
                        await WaitForChunkStateAsync(
                            lab.Stores[nodeTag],
                            documentIds,
                            scenario.FinalStates[nodeTag],
                            stabilityWindow: TimeSpan.FromSeconds(1));
                    }
                }
            }
            finally
            {
                foreach (var ruleHandle in persistentRuleHandles)
                    ruleHandle.Dispose();

                foreach (var suppressionHandle in heartbeatSuppressionHandles)
                    suppressionHandle.Dispose();
            }
        }

        private static void AddProgressTarget(Dictionary<string, HashSet<string>> progressTargetsBySource, string sourceNodeTag, string targetNodeTag)
        {
            if (progressTargetsBySource.TryGetValue(sourceNodeTag, out var targets) == false)
            {
                targets = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                progressTargetsBySource[sourceNodeTag] = targets;
            }

            targets.Add(targetNodeTag);
        }

        private async Task<Dictionary<string, string>> TriggerReplicationProgressAsync(
            RecoveryLabClusterContext lab,
            IReadOnlyList<string> sourceNodeTags,
            string scenarioName,
            string batchKind,
            int stepIndex,
            int chunkSequence)
        {
            var triggerIds = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            foreach (var sourceNodeTag in sourceNodeTags)
            {
                var triggerId = $"internal/relay-trigger/{SanitizeScenarioName(scenarioName)}/{batchKind}/step-{stepIndex + 1}/{sourceNodeTag}/{chunkSequence:D6}";
                await StoreMarkerDocumentAsync(
                    lab.Stores[sourceNodeTag],
                    triggerId,
                    $"relay-trigger-{scenarioName}-{batchKind}-{stepIndex + 1}-{sourceNodeTag}-{chunkSequence:D6}");
                triggerIds[sourceNodeTag] = triggerId;
            }

            return triggerIds;
        }

        private async Task WaitForPartiallyMatchedRulesToCompleteAsync(IReadOnlyList<ReplicationFaultRuleHandle> ruleHandles)
        {
            foreach (var ruleHandle in ruleHandles)
            {
                if (ruleHandle.IsCompleted)
                    continue;

                if (ruleHandle.MatchedMatches <= 0)
                    continue;

                var completionTask = ruleHandle.WaitForCompletionAsync();
                var completedTask = await Task.WhenAny(completionTask, Task.Delay(TimeSpan.FromSeconds(30)));
                Assert.True(
                    completedTask == completionTask,
                    $"Fault rule '{ruleHandle.Label}' matched {ruleHandle.MatchedMatches}/{ruleHandle.ExpectedMatches} documents but did not complete within the timeout.");

                await completionTask;
            }
        }

        private async Task WriteScenarioChunkAsync(IDocumentStore store, IReadOnlyList<RecoveryDocumentPlan> plans, ScenarioStepDefinition step, SimulatedNodeState expectedWriterState, int chunkSize)
        {
            for (var start = 0; start < plans.Count; start += chunkSize)
            {
                var chunk = plans.Skip(start).Take(chunkSize).ToArray();
                var ids = chunk.Select(x => x.Id).ToArray();

                using var session = store.OpenAsyncSession();
                var documents = await session.LoadAsync<RecoveryScenarioDocument>(ids);

                foreach (var plan in chunk)
                {
                    documents.TryGetValue(plan.Id, out var document);

                    Assert.Equal(expectedWriterState.Exists, document != null);
                    if (expectedWriterState.Exists)
                    {
                        Assert.NotNull(document);
                        Assert.Equal(expectedWriterState.Version, document.Version);
                        Assert.Equal(expectedWriterState.WrittenBy, document.WrittenBy);
                    }

                    if (document == null)
                    {
                        document = new RecoveryScenarioDocument();
                        await session.StoreAsync(document, plan.Id);
                    }

                    document.Scenario = plan.Scenario;
                    document.Sequence = plan.Sequence;
                    document.ScenarioSequence = plan.ScenarioSequence;
                    document.Version = step.Version;
                    document.WrittenBy = step.WrittenBy;
                    document.ExpectedWinnerNode = plan.ExpectedWinnerNode;
                    document.ExpectedWinnerVersion = plan.ExpectedWinnerVersion;
                    document.ExpectedWinnerWrittenBy = plan.ExpectedWinnerWrittenBy;
                    document.ScenarioGroup = plan.ScenarioGroup;
                    document.SeedPattern = plan.SeedPattern;
                    document.BatchKind = plan.BatchKind;
                    document.RepairPolicy = plan.RepairPolicy;
                }

                await session.SaveChangesAsync();
            }
        }

        private async Task WaitForChunkStateAsync(
            IDocumentStore store,
            IReadOnlyList<string> documentIds,
            SimulatedNodeState expectedState,
            TimeSpan? stabilityWindow = null)
        {
            var expectedCount = expectedState.Exists ? documentIds.Count : 0;
            var requiredStableDuration = stabilityWindow.GetValueOrDefault(TimeSpan.Zero);

            if (requiredStableDuration <= TimeSpan.Zero)
            {
                await AssertWaitForValueAsync(
                    () => GetChunkStateMatchCountAsync(store, documentIds, expectedState),
                    expectedCount,
                    timeout: 120_000,
                    interval: 250);
                return;
            }

            var overallStopwatch = Stopwatch.StartNew();
            Stopwatch stableStopwatch = null;
            var lastObservedCount = -1;

            while (overallStopwatch.Elapsed < TimeSpan.FromSeconds(120))
            {
                lastObservedCount = await GetChunkStateMatchCountAsync(store, documentIds, expectedState);
                if (lastObservedCount == expectedCount)
                {
                    stableStopwatch ??= Stopwatch.StartNew();
                    if (stableStopwatch.Elapsed >= requiredStableDuration)
                        return;
                }
                else
                {
                    stableStopwatch = null;
                }

                await Task.Delay(250);
            }

            Assert.Equal(expectedCount, lastObservedCount);
        }

        private async Task<int> GetChunkStateMatchCountAsync(IDocumentStore store, IReadOnlyList<string> documentIds, SimulatedNodeState expectedState)
        {
            using var session = store.OpenAsyncSession();
            var documents = await session.LoadAsync<RecoveryScenarioDocument>(documentIds.ToArray());

            if (expectedState.Exists == false)
                return documents.Values.Count(document => document != null);

            return documents.Values.Count(document =>
                document != null &&
                document.Version == expectedState.Version &&
                string.Equals(document.WrittenBy, expectedState.WrittenBy, StringComparison.OrdinalIgnoreCase));
        }

        private async Task AssertAllPlansAsync(RecoveryLabClusterContext lab, IReadOnlyList<ScenarioDefinition> scenarios, IReadOnlyList<RecoveryDocumentPlan> bulkPlans, IReadOnlyList<RecoveryDocumentPlan> mixedRun5Plans, IReadOnlyList<RecoveryDocumentPlan> mixedRun1Plans)
        {
            var scenariosByName = scenarios.ToDictionary(x => x.Name, StringComparer.OrdinalIgnoreCase);

            await AssertPlansAsync(lab, bulkPlans, scenariosByName);
            await AssertPlansAsync(lab, mixedRun5Plans, scenariosByName);
            await AssertPlansAsync(lab, mixedRun1Plans, scenariosByName);
        }

        private async Task AssertPlansChunkAsync(RecoveryLabClusterContext lab, ScenarioDefinition scenario, IReadOnlyList<RecoveryDocumentPlan> plans)
        {
            if (plans.Count == 0)
                return;

            await AssertPlansAsync(
                lab,
                plans,
                new Dictionary<string, ScenarioDefinition>(StringComparer.OrdinalIgnoreCase)
                {
                    [scenario.Name] = scenario
                });
        }

        private async Task AssertPlansAsync(RecoveryLabClusterContext lab, IReadOnlyList<RecoveryDocumentPlan> plans, IReadOnlyDictionary<string, ScenarioDefinition> scenariosByName)
        {
            if (plans.Count == 0)
                return;

            var planIds = plans
                .Select(x => x.Id)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();

            var snapshotsByNode = new Dictionary<string, IReadOnlyDictionary<string, ScenarioSnapshot>>(StringComparer.OrdinalIgnoreCase);
            foreach (var nodeTag in OrderedNodeTags)
                snapshotsByNode[nodeTag] = await LoadScenarioSnapshotsAsync(lab.Stores[nodeTag], planIds);

            foreach (var plan in plans.OrderBy(x => x.Sequence))
                AssertScenarioPlanStateAndCv(lab, plan, scenariosByName[plan.Scenario], snapshotsByNode);
        }

        private void AssertScenarioPlanStateAndCv(
            RecoveryLabClusterContext lab,
            RecoveryDocumentPlan plan,
            ScenarioDefinition scenario,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScenarioSnapshot>> snapshotsByNode)
        {
            var snapshots = new Dictionary<string, ScenarioSnapshot>(StringComparer.OrdinalIgnoreCase)
            {
                ["A"] = snapshotsByNode["A"][plan.Id],
                ["B"] = snapshotsByNode["B"][plan.Id],
                ["C"] = snapshotsByNode["C"][plan.Id]
            };

            try
            {
                AssertNodeState(snapshots["A"], scenario.FinalStates["A"], plan);
                AssertNodeState(snapshots["B"], scenario.FinalStates["B"], plan);
                AssertNodeState(snapshots["C"], scenario.FinalStates["C"], plan);

                AssertCvTopology(scenario, snapshots);
            }
            catch (Exception e)
            {
                throw new Xunit.Sdk.XunitException(BuildScenarioValidationFailureMessage(lab, plan, scenario, snapshots, e));
            }
        }

        private void AssertNodeState(ScenarioSnapshot snapshot, SimulatedNodeState expectedState, RecoveryDocumentPlan plan)
        {
            Assert.Equal(expectedState.Exists, snapshot.Exists);
            if (expectedState.Exists == false)
                return;

            Assert.Equal(plan.Scenario, snapshot.Scenario);
            Assert.Equal(plan.BatchKind, snapshot.BatchKind);
            Assert.Equal(plan.ScenarioGroup, snapshot.ScenarioGroup);
            Assert.Equal(plan.RepairPolicy, snapshot.RepairPolicy);
            Assert.Equal(plan.ExpectedWinnerNode, snapshot.ExpectedWinnerNode);
            Assert.Equal(plan.ExpectedWinnerVersion, snapshot.ExpectedWinnerVersion);
            Assert.Equal(plan.ExpectedWinnerWrittenBy, snapshot.ExpectedWinnerWrittenBy);
            Assert.Equal(plan.SeedPattern, snapshot.SeedPattern);
            Assert.Equal(expectedState.Version, snapshot.Version);
            Assert.Equal(expectedState.WrittenBy, snapshot.WrittenBy);
            Assert.False(string.IsNullOrWhiteSpace(snapshot.ChangeVector));
        }

        private void AssertCvTopology(ScenarioDefinition scenario, IReadOnlyDictionary<string, ScenarioSnapshot> snapshots)
        {
            var actualExistingNodes = snapshots
                .Where(x => x.Value.Exists)
                .Select(x => x.Key)
                .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
                .ToArray();

            Assert.Equal(scenario.CvTopology.ExpectedExistingNodes, actualExistingNodes);

            var actualGroups = BuildActualChangeVectorGroups(snapshots);
            Assert.Equal(scenario.CvTopology.GroupKeys, actualGroups.Keys.OrderBy(x => x, StringComparer.OrdinalIgnoreCase).ToArray());

            var actualDominantGroupKey = DetermineDominantGroupKey(actualGroups);
            if (scenario.CvTopology.ExpectAmbiguous)
            {
                Assert.True(
                    actualDominantGroupKey == null,
                    $"Ambiguous scenario produced a dominant winner group '{actualDominantGroupKey}'.");
            }
            else
                Assert.Equal(scenario.CvTopology.DominantGroupKey, actualDominantGroupKey);

            foreach (var dominancePair in scenario.CvTopology.DominancePairs)
            {
                Assert.True(
                    Dominates(actualGroups[dominancePair.LeftGroupKey].ParsedChangeVector, actualGroups[dominancePair.RightGroupKey].ParsedChangeVector),
                    $"Expected CV group '{dominancePair.LeftGroupKey}' to dominate '{dominancePair.RightGroupKey}'.");
            }

            foreach (var incomparablePair in scenario.CvTopology.IncomparablePairs)
            {
                var left = actualGroups[incomparablePair.LeftGroupKey].ParsedChangeVector;
                var right = actualGroups[incomparablePair.RightGroupKey].ParsedChangeVector;

                Assert.False(
                    Dominates(left, right),
                    $"Expected CV groups '{incomparablePair.LeftGroupKey}' and '{incomparablePair.RightGroupKey}' to be incomparable, but left dominated right.");
                Assert.False(
                    Dominates(right, left),
                    $"Expected CV groups '{incomparablePair.LeftGroupKey}' and '{incomparablePair.RightGroupKey}' to be incomparable, but right dominated left.");
            }
        }

        private IReadOnlyDictionary<string, ChangeVectorGroup> BuildActualChangeVectorGroups(IReadOnlyDictionary<string, ScenarioSnapshot> snapshots)
        {
            return snapshots
                .Where(x => x.Value.Exists)
                .GroupBy(x => NormalizeChangeVector(x.Value.ChangeVector), StringComparer.OrdinalIgnoreCase)
                .Select(group =>
                {
                    var nodeTags = group
                        .Select(x => x.Key)
                        .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
                        .ToArray();
                    var groupKey = CreateGroupKey(nodeTags);

                    return new KeyValuePair<string, ChangeVectorGroup>(
                        groupKey,
                        new ChangeVectorGroup
                        {
                            GroupKey = groupKey,
                            NormalizedChangeVector = group.Key,
                            NodeTags = nodeTags,
                            ParsedChangeVector = ParseChangeVector(group.First().Value.ChangeVector),
                            Snapshots = group.Select(x => x.Value).ToList()
                        });
                })
                .ToDictionary(x => x.Key, x => x.Value, StringComparer.OrdinalIgnoreCase);
        }

        private string DetermineDominantGroupKey(IReadOnlyDictionary<string, ChangeVectorGroup> groups)
        {
            if (groups.Count == 0)
                return null;

            if (groups.Count == 1)
                return groups.Keys.Single();

            string dominantGroupKey = null;
            foreach (var candidate in groups.Values)
            {
                var dominatesAllOthers = groups.Values
                    .Where(group => ReferenceEquals(group, candidate) == false)
                    .All(other => Dominates(candidate.ParsedChangeVector, other.ParsedChangeVector));

                if (dominatesAllOthers == false)
                    continue;

                if (dominantGroupKey != null)
                    return null;

                dominantGroupKey = candidate.GroupKey;
            }

            return dominantGroupKey;
        }

        private string NormalizeChangeVector(string changeVector)
        {
            if (string.IsNullOrWhiteSpace(changeVector))
                return string.Empty;

            return string.Join("|", ParseChangeVector(changeVector)
                .OrderBy(x => x.Key, StringComparer.OrdinalIgnoreCase)
                .Select(x => $"{x.Key}:{x.Value}"));
        }

        private Dictionary<string, long> ParseChangeVector(string changeVector)
        {
            var result = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
            if (string.IsNullOrWhiteSpace(changeVector))
                return result;

            var entries = changeVector.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            foreach (var entry in entries)
            {
                var colonIndex = entry.IndexOf(':');
                if (colonIndex <= 0)
                    continue;

                var dashIndex = entry.IndexOf('-', colonIndex + 1);
                var tag = entry.Substring(0, colonIndex).Trim();
                var etagText = dashIndex > colonIndex
                    ? entry.Substring(colonIndex + 1, dashIndex - colonIndex - 1)
                    : entry[(colonIndex + 1)..];

                if (long.TryParse(etagText, out var etag) == false)
                    continue;

                result[tag] = etag;
            }

            return result;
        }

        private bool Dominates(Dictionary<string, long> left, Dictionary<string, long> right)
        {
            var hasStrictlyGreaterEntry = false;
            foreach (var tag in left.Keys.Concat(right.Keys).Distinct(StringComparer.OrdinalIgnoreCase))
            {
                left.TryGetValue(tag, out var leftEtag);
                right.TryGetValue(tag, out var rightEtag);

                if (leftEtag < rightEtag)
                    return false;

                if (leftEtag > rightEtag)
                    hasStrictlyGreaterEntry = true;
            }

            return hasStrictlyGreaterEntry;
        }

        private async Task CreateScenarioValidationIndexesAsync(IDocumentStore store, IReadOnlyList<ScenarioDefinition> scenarios)
        {
            var definitions = new List<IndexDefinition>(scenarios.Count * 2);

            foreach (var scenario in scenarios)
            {
                definitions.Add(BuildAuditIndexDefinition(scenario));
                definitions.Add(scenario.IsRepairable
                    ? BuildInvalidIndexDefinition(scenario)
                    : BuildAmbiguousIndexDefinition(scenario));
            }

            await store.Maintenance.SendAsync(new PutIndexesOperation(definitions.ToArray()));
        }

        private IndexDefinition BuildAuditIndexDefinition(ScenarioDefinition scenario)
        {
            return new IndexDefinition
            {
                Name = GetAuditIndexName(scenario.Name),
                Maps =
                {
                    BuildAuditIndexMap(scenario)
                }
            };
        }

        private IndexDefinition BuildInvalidIndexDefinition(ScenarioDefinition scenario)
        {
            return new IndexDefinition
            {
                Name = GetInvalidIndexName(scenario.Name),
                Maps =
                {
                    BuildInvalidIndexMap(scenario)
                }
            };
        }

        private IndexDefinition BuildAmbiguousIndexDefinition(ScenarioDefinition scenario)
        {
            return new IndexDefinition
            {
                Name = GetAmbiguousIndexName(scenario.Name),
                Maps =
                {
                    BuildAuditIndexMap(scenario)
                }
            };
        }

        private string BuildAuditIndexMap(ScenarioDefinition scenario)
        {
            var escapedScenarioName = EscapeIndexLiteral(scenario.Name);
            return $@"from doc in docs.RecoveryScenarioDocuments
where doc.Scenario == ""{escapedScenarioName}""
select new
{{
    Id = MetadataFor(doc)[""@id""],
    Scenario = doc.Scenario,
    ScenarioGroup = doc.ScenarioGroup,
    BatchKind = doc.BatchKind,
    Sequence = doc.Sequence,
    ScenarioSequence = doc.ScenarioSequence,
    Version = doc.Version,
    WrittenBy = doc.WrittenBy,
    RepairPolicy = doc.RepairPolicy,
    ExpectedWinnerNode = doc.ExpectedWinnerNode,
    ExpectedWinnerVersion = doc.ExpectedWinnerVersion,
    ExpectedWinnerWrittenBy = doc.ExpectedWinnerWrittenBy,
    SeedPattern = doc.SeedPattern,
    CurrentChangeVector = MetadataFor(doc)[""@change-vector""]
}}";
        }

        private string BuildInvalidIndexMap(ScenarioDefinition scenario)
        {
            var escapedScenarioName = EscapeIndexLiteral(scenario.Name);
            return $@"from doc in docs.RecoveryScenarioDocuments
where doc.Scenario == ""{escapedScenarioName}""
   && (doc.Version != doc.ExpectedWinnerVersion || doc.WrittenBy != doc.ExpectedWinnerWrittenBy)
select new
{{
    Id = MetadataFor(doc)[""@id""],
    Scenario = doc.Scenario,
    ScenarioGroup = doc.ScenarioGroup,
    BatchKind = doc.BatchKind,
    Sequence = doc.Sequence,
    ScenarioSequence = doc.ScenarioSequence,
    Version = doc.Version,
    WrittenBy = doc.WrittenBy,
    RepairPolicy = doc.RepairPolicy,
    ExpectedWinnerNode = doc.ExpectedWinnerNode,
    ExpectedWinnerVersion = doc.ExpectedWinnerVersion,
    ExpectedWinnerWrittenBy = doc.ExpectedWinnerWrittenBy,
    SeedPattern = doc.SeedPattern
}}";
        }

        private async Task AssertScenarioIndexCountsAsync(RecoveryLabClusterContext lab, IReadOnlyList<ScenarioDefinition> scenarios, int totalDocumentsPerScenario)
        {
            foreach (var scenario in scenarios)
            {
                var auditIndexName = GetAuditIndexName(scenario.Name);
                var checkIndexName = GetCheckIndexName(scenario);

                foreach (var nodeTag in OrderedNodeTags)
                {
                    var store = lab.Stores[nodeTag];
                    Assert.Equal(GetAuditCountBeforeRecovery(scenario, nodeTag, totalDocumentsPerScenario), await GetIndexCountAsync(store, auditIndexName));
                    Assert.Equal(GetCheckCountBeforeRecovery(scenario, nodeTag, totalDocumentsPerScenario), await GetIndexCountAsync(store, checkIndexName));
                }
            }
        }

        private async Task<long> GetIndexCountAsync(IDocumentStore store, string indexName)
        {
            var result = await store.Commands().QueryAsync(new IndexQuery
            {
                Query = $"from index '{indexName}'"
            });

            return result.TotalResults;
        }

        private long GetAuditCountBeforeRecovery(ScenarioDefinition scenario, string nodeTag, int totalDocumentsPerScenario)
        {
            return scenario.FinalStates[nodeTag].Exists ? totalDocumentsPerScenario : 0;
        }

        private long GetCheckCountBeforeRecovery(ScenarioDefinition scenario, string nodeTag, int totalDocumentsPerScenario)
        {
            var state = scenario.FinalStates[nodeTag];
            if (state.Exists == false)
                return 0;

            if (scenario.IsRepairable == false)
                return totalDocumentsPerScenario;

            return state.Version == scenario.ExpectedWinnerVersion &&
                   string.Equals(state.WrittenBy, scenario.ExpectedWinnerWrittenBy, StringComparison.OrdinalIgnoreCase)
                ? 0
                : totalDocumentsPerScenario;
        }

        private async Task RunLiveClusterProbeAsync(RecoveryLabClusterContext lab)
        {
            await StoreMarkerDocumentAsync(lab.Stores["A"], "lab/probe/from-a", "probe-from-a");
            Assert.True(WaitForDocument(lab.Stores["B"], "lab/probe/from-a", timeout: 60_000));
            Assert.True(WaitForDocument(lab.Stores["C"], "lab/probe/from-a", timeout: 60_000));

            await StoreMarkerDocumentAsync(lab.Stores["B"], "lab/probe/from-b", "probe-from-b");
            Assert.True(WaitForDocument(lab.Stores["A"], "lab/probe/from-b", timeout: 60_000));
            Assert.True(WaitForDocument(lab.Stores["C"], "lab/probe/from-b", timeout: 60_000));

            await StoreMarkerDocumentAsync(lab.Stores["C"], "lab/probe/from-c", "probe-from-c");
            Assert.True(WaitForDocument(lab.Stores["A"], "lab/probe/from-c", timeout: 60_000));
            Assert.True(WaitForDocument(lab.Stores["B"], "lab/probe/from-c", timeout: 60_000));
        }

        private string CreateScenarioIndexMatrix(IReadOnlyList<ScenarioDefinition> scenarios, int totalDocumentsPerScenario)
        {
            var builder = new StringBuilder();
            builder.AppendLine("# Scenario Index Matrix");
            builder.AppendLine();
            builder.AppendLine("Raw document oracle is authoritative; indexes are an operator-facing aggregate view.");
            builder.AppendLine("All counts below assume node-local inspection through a store pinned to A, B, or C.");
            builder.AppendLine("`Recovery_Audit_*` indexes show all local copies for the scenario on the inspected node.");
            builder.AppendLine("`Recovery_Invalid_*` indexes exist only for repairable scenarios and must go to `0` on all nodes after recovery.");
            builder.AppendLine("`Recovery_Ambiguous_*` indexes exist only for ambiguous negative-control scenarios and are expected to remain as seeded.");
            builder.AppendLine();
            builder.AppendLine("| Scenario | ScenarioGroup | RepairPolicy | Winner | SeedPattern | ValidatedIn | AuditIndex | AuditCountA_Before | AuditCountB_Before | AuditCountC_Before | CheckIndex | CheckCountA_Before | CheckCountB_Before | CheckCountC_Before | ExpectedAfterRecoveryA | ExpectedAfterRecoveryB | ExpectedAfterRecoveryC |");
            builder.AppendLine("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |");

            foreach (var scenario in scenarios)
            {
                builder.AppendLine(
                    $"| {scenario.Name} | {scenario.ScenarioGroup} | {scenario.RepairPolicy} | {FormatWinner(scenario)} | {scenario.SeedPattern} | {scenario.ValidatedIn} | {GetAuditIndexName(scenario.Name)} | {GetAuditCountBeforeRecovery(scenario, "A", totalDocumentsPerScenario)} | {GetAuditCountBeforeRecovery(scenario, "B", totalDocumentsPerScenario)} | {GetAuditCountBeforeRecovery(scenario, "C", totalDocumentsPerScenario)} | {GetCheckIndexName(scenario)} | {GetCheckCountBeforeRecovery(scenario, "A", totalDocumentsPerScenario)} | {GetCheckCountBeforeRecovery(scenario, "B", totalDocumentsPerScenario)} | {GetCheckCountBeforeRecovery(scenario, "C", totalDocumentsPerScenario)} | {GetExpectedAfterRecoveryDescription(scenario, "A", totalDocumentsPerScenario)} | {GetExpectedAfterRecoveryDescription(scenario, "B", totalDocumentsPerScenario)} | {GetExpectedAfterRecoveryDescription(scenario, "C", totalDocumentsPerScenario)} |");
            }

            return builder.ToString();
        }

        private string FormatWinner(ScenarioDefinition scenario)
        {
            return scenario.IsRepairable
                ? $"{scenario.ExpectedWinnerNode} -> v{scenario.ExpectedWinnerVersion}/{scenario.ExpectedWinnerWrittenBy}"
                : "ambiguous/no-dominant-cv";
        }

        private string GetExpectedAfterRecoveryDescription(ScenarioDefinition scenario, string nodeTag, int totalDocumentsPerScenario)
        {
            if (scenario.IsRepairable)
                return $"audit={totalDocumentsPerScenario}, invalid=0";

            return $"audit={GetAuditCountBeforeRecovery(scenario, nodeTag, totalDocumentsPerScenario)}, ambiguous={GetCheckCountBeforeRecovery(scenario, nodeTag, totalDocumentsPerScenario)}, skipped";
        }

        private static string GetAuditIndexName(string scenarioName)
        {
            return $"Recovery_Audit_{SanitizeScenarioName(scenarioName)}";
        }

        private static string GetInvalidIndexName(string scenarioName)
        {
            return $"Recovery_Invalid_{SanitizeScenarioName(scenarioName)}";
        }

        private static string GetAmbiguousIndexName(string scenarioName)
        {
            return $"Recovery_Ambiguous_{SanitizeScenarioName(scenarioName)}";
        }

        private static string GetCheckIndexName(ScenarioDefinition scenario)
        {
            return scenario.IsRepairable
                ? GetInvalidIndexName(scenario.Name)
                : GetAmbiguousIndexName(scenario.Name);
        }

        private static string SanitizeScenarioName(string scenarioName)
        {
            return scenarioName.Replace('-', '_');
        }

        private static string EscapeIndexLiteral(string value)
        {
            return value.Replace("\"", "\"\"");
        }

        private async Task StoreMarkerDocumentAsync(IDocumentStore store, string id, string marker)
        {
            using var session = store.OpenAsyncSession();
            await session.StoreAsync(new User
            {
                Name = marker
            }, id);
            await session.SaveChangesAsync();
        }

        private async Task<IReadOnlyDictionary<string, ScenarioSnapshot>> LoadScenarioSnapshotsAsync(IDocumentStore store, IReadOnlyList<string> ids)
        {
            var snapshots = new Dictionary<string, ScenarioSnapshot>(ids.Count, StringComparer.OrdinalIgnoreCase);

            for (var start = 0; start < ids.Count; start += 128)
            {
                var chunk = ids.Skip(start).Take(128).ToArray();

                using var session = store.OpenAsyncSession();
                var documents = await session.LoadAsync<RecoveryScenarioDocument>(chunk);

                foreach (var id in chunk)
                {
                    if (documents.TryGetValue(id, out var document) == false || document == null)
                    {
                        snapshots[id] = new ScenarioSnapshot
                        {
                            Exists = false
                        };

                        continue;
                    }

                    snapshots[id] = new ScenarioSnapshot
                    {
                        Exists = true,
                        Scenario = document.Scenario,
                        Sequence = document.Sequence,
                        ScenarioSequence = document.ScenarioSequence,
                        Version = document.Version,
                        WrittenBy = document.WrittenBy,
                        RepairPolicy = document.RepairPolicy,
                        ExpectedWinnerNode = document.ExpectedWinnerNode,
                        ExpectedWinnerVersion = document.ExpectedWinnerVersion,
                        ExpectedWinnerWrittenBy = document.ExpectedWinnerWrittenBy,
                        ScenarioGroup = document.ScenarioGroup,
                        SeedPattern = document.SeedPattern,
                        BatchKind = document.BatchKind,
                        ChangeVector = session.Advanced.GetChangeVectorFor(document)
                    };
                }
            }

            return snapshots;
        }

        private string BuildScenarioValidationFailureMessage(
            RecoveryLabClusterContext lab,
            RecoveryDocumentPlan plan,
            ScenarioDefinition scenario,
            IReadOnlyDictionary<string, ScenarioSnapshot> snapshots,
            Exception exception)
        {
            var actualGroups = BuildActualChangeVectorGroups(snapshots);
            var dominantGroupKey = DetermineDominantGroupKey(actualGroups) ?? "<none>";
            var builder = new StringBuilder();
            builder.AppendLine("Scenario document validation failed.");
            builder.AppendLine($"Database: {lab.DatabaseName}");
            builder.AppendLine($"Document: {plan.Id}");
            builder.AppendLine($"Scenario: {plan.Scenario}");
            builder.AppendLine($"BatchKind: {plan.BatchKind}");
            builder.AppendLine($"Sequence: {plan.Sequence}");
            builder.AppendLine($"ScenarioSequence: {plan.ScenarioSequence}");
            builder.AppendLine($"RepairPolicy: {plan.RepairPolicy}");
            builder.AppendLine($"SeedPattern: {plan.SeedPattern}");
            builder.AppendLine($"Expected final states: A={FormatNodeState(scenario.FinalStates["A"])}, B={FormatNodeState(scenario.FinalStates["B"])}, C={FormatNodeState(scenario.FinalStates["C"])}");
            builder.AppendLine($"Expected CV groups: {string.Join(", ", scenario.CvTopology.GroupKeys)}");
            builder.AppendLine($"Expected dominant group: {scenario.CvTopology.DominantGroupKey ?? "<none>"}");
            builder.AppendLine($"Expected ambiguous: {scenario.CvTopology.ExpectAmbiguous}");
            builder.AppendLine($"Actual dominant group: {dominantGroupKey}");
            builder.AppendLine($"Actual CV groups: {(actualGroups.Count == 0 ? "<none>" : string.Join(", ", actualGroups.Values.OrderBy(x => x.GroupKey, StringComparer.OrdinalIgnoreCase).Select(FormatActualGroup)))}");
            builder.AppendLine("Snapshots:");
            foreach (var nodeTag in OrderedNodeTags)
                builder.AppendLine($"  {nodeTag}: {FormatSnapshotForDiagnostics(snapshots[nodeTag])}");

            builder.AppendLine($"Failure: {exception.GetType().Name}: {exception.Message}");
            return builder.ToString();
        }

        private string FormatActualGroup(ChangeVectorGroup group)
        {
            return $"{group.GroupKey}=[{string.Join(",", group.NodeTags)}] => {group.NormalizedChangeVector}";
        }

        private string FormatSnapshotForDiagnostics(ScenarioSnapshot snapshot)
        {
            if (snapshot.Exists == false)
                return "missing";

            return $"exists v{snapshot.Version}/{snapshot.WrittenBy}, cv={NormalizeChangeVector(snapshot.ChangeVector)}, rawCv={snapshot.ChangeVector}";
        }

        private string FormatNodeState(SimulatedNodeState state)
        {
            return state.Exists ? $"v{state.Version}/{state.WrittenBy}" : "missing";
        }

        private string CreateGroupKey(IEnumerable<string> nodeTags)
        {
            return string.Concat(nodeTags.OrderBy(x => x, StringComparer.OrdinalIgnoreCase));
        }

        private List<IDictionary<string, string>> CreatePersistentClusterSettings(string labRoot)
        {
            var settings = new List<IDictionary<string, string>>();
            foreach (var nodeTag in new[] { "A", "B", "C" })
            {
                var dataDirectory = Path.Combine(labRoot, $"server-{nodeTag}");
                Directory.CreateDirectory(dataDirectory);

                var nodeSettings = new Dictionary<string, string>(DefaultClusterSettings)
                {
                    [RavenConfiguration.GetKey(x => x.Core.DataDirectory)] = dataDirectory
                };
                settings.Add(nodeSettings);
            }

            return settings;
        }

        private static string GetRecoveryDocumentId(string batchKind, string scenario, int scenarioSequence)
        {
            return $"recovery/{batchKind}/{scenario}/{scenarioSequence:D5}";
        }

        private static string GetRecoveryDocumentId(string batchKind, int batchSequence)
        {
            return $"recovery/{batchKind}/{batchSequence:D6}";
        }

        private const string RepairPolicyRepairable = "Repairable";
        private const string RepairPolicyAmbiguousSkip = "AmbiguousSkip";
        private const string ValidatedInAllBatchKinds = "bulk,mixed-run-5,mixed-run-1";

        private sealed class RecoveryScenarioDocument
        {
            public string Scenario { get; set; }

            public int Sequence { get; set; }

            public int ScenarioSequence { get; set; }

            public int Version { get; set; }

            public string WrittenBy { get; set; }

            public string ExpectedWinnerNode { get; set; }

            public int? ExpectedWinnerVersion { get; set; }

            public string ExpectedWinnerWrittenBy { get; set; }

            public string ScenarioGroup { get; set; }

            public string SeedPattern { get; set; }

            public string BatchKind { get; set; }

            public string RepairPolicy { get; set; }
        }

        private sealed class ScenarioSnapshot
        {
            public bool Exists { get; set; }

            public string Scenario { get; set; }

            public int Sequence { get; set; }

            public int ScenarioSequence { get; set; }

            public int Version { get; set; }

            public string WrittenBy { get; set; }

            public string RepairPolicy { get; set; }

            public string ExpectedWinnerNode { get; set; }

            public int? ExpectedWinnerVersion { get; set; }

            public string ExpectedWinnerWrittenBy { get; set; }

            public string ScenarioGroup { get; set; }

            public string SeedPattern { get; set; }

            public string BatchKind { get; set; }

            public string ChangeVector { get; set; }
        }

        private sealed class RecoveryLabClusterContext
        {
            public RecoveryLabClusterContext(
                string databaseName,
                RavenServer serverA,
                RavenServer serverB,
                RavenServer serverC,
                DocumentDatabase dbA,
                DocumentDatabase dbB,
                DocumentDatabase dbC,
                IDocumentStore storeA,
                IDocumentStore storeB,
                IDocumentStore storeC,
                ReplicationFaultController faultController)
            {
                DatabaseName = databaseName;
                FaultController = faultController;
                Servers = new Dictionary<string, RavenServer>(StringComparer.OrdinalIgnoreCase)
                {
                    ["A"] = serverA,
                    ["B"] = serverB,
                    ["C"] = serverC
                };
                Databases = new Dictionary<string, DocumentDatabase>(StringComparer.OrdinalIgnoreCase)
                {
                    ["A"] = dbA,
                    ["B"] = dbB,
                    ["C"] = dbC
                };
                Stores = new Dictionary<string, IDocumentStore>(StringComparer.OrdinalIgnoreCase)
                {
                    ["A"] = storeA,
                    ["B"] = storeB,
                    ["C"] = storeC
                };
            }

            public string DatabaseName { get; }

            public ReplicationFaultController FaultController { get; }

            public Dictionary<string, RavenServer> Servers { get; }

            public Dictionary<string, DocumentDatabase> Databases { get; }

            public Dictionary<string, IDocumentStore> Stores { get; }
        }

        private sealed class ScenarioDefinition
        {
            public string Name { get; set; }

            public string ScenarioGroup { get; set; }

            public string RepairPolicy { get; set; }

            public string ExpectedWinnerNode { get; set; }

            public int? ExpectedWinnerVersion { get; set; }

            public string ExpectedWinnerWrittenBy { get; set; }

            public string SeedPattern { get; set; }

            public string ValidatedIn { get; set; }

            public List<ScenarioStepDefinition> Steps { get; set; }

            public List<Dictionary<string, SimulatedNodeState>> StatesBeforeEachStep { get; set; }

            public List<Dictionary<string, SimulatedNodeState>> StatesAfterEachStep { get; set; }

            public Dictionary<string, SimulatedNodeState> FinalStates { get; set; }

            public ExpectedCvTopology CvTopology { get; set; }

            public bool IsRepairable => string.Equals(RepairPolicy, "Repairable", StringComparison.Ordinal);
        }

        private sealed class ScenarioStepDefinition
        {
            public ScenarioStepDefinition(ScenarioStepKind kind, string writerNode, int version, string writtenBy, params string[] skippedTargets)
            {
                Kind = kind;
                WriterNode = writerNode;
                Version = version;
                WrittenBy = writtenBy;
                SkippedTargets = skippedTargets?
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToArray() ?? Array.Empty<string>();
            }

            public ScenarioStepKind Kind { get; }

            public string WriterNode { get; }

            public int Version { get; }

            public string WrittenBy { get; }

            public string[] SkippedTargets { get; }
        }

        private sealed class RecoveryDocumentPlan
        {
            public string Id { get; set; }

            public string BatchKind { get; set; }

            public string Scenario { get; set; }

            public string ScenarioGroup { get; set; }

            public int Sequence { get; set; }

            public int ScenarioSequence { get; set; }

            public string RepairPolicy { get; set; }

            public string ExpectedWinnerNode { get; set; }

            public int? ExpectedWinnerVersion { get; set; }

            public string ExpectedWinnerWrittenBy { get; set; }

            public string SeedPattern { get; set; }
        }

        private sealed class SimulatedNodeState
        {
            public bool Exists { get; set; }

            public int Version { get; set; }

            public string WrittenBy { get; set; }

            public int LastActionOrdinal { get; set; }

            public SimulatedNodeState Clone()
            {
                return new SimulatedNodeState
                {
                    Exists = Exists,
                    Version = Version,
                    WrittenBy = WrittenBy,
                    LastActionOrdinal = LastActionOrdinal
                };
            }
        }

        private sealed class ScenarioSimulation
        {
            public ScenarioSimulation(List<Dictionary<string, SimulatedNodeState>> statesBeforeEachStep, List<Dictionary<string, SimulatedNodeState>> statesAfterEachStep, Dictionary<string, SimulatedNodeState> finalStates)
            {
                StatesBeforeEachStep = statesBeforeEachStep;
                StatesAfterEachStep = statesAfterEachStep;
                FinalStates = finalStates;
            }

            public List<Dictionary<string, SimulatedNodeState>> StatesBeforeEachStep { get; }

            public List<Dictionary<string, SimulatedNodeState>> StatesAfterEachStep { get; }

            public Dictionary<string, SimulatedNodeState> FinalStates { get; }
        }

        private sealed class ChangeVectorGroup
        {
            public string GroupKey { get; set; }

            public string NormalizedChangeVector { get; set; }

            public string[] NodeTags { get; set; }

            public Dictionary<string, long> ParsedChangeVector { get; set; }

            public List<ScenarioSnapshot> Snapshots { get; set; }
        }

        private sealed class ExpectedCvTopology
        {
            public string[] ExpectedExistingNodes { get; set; }

            public string[] GroupKeys { get; set; }

            public List<ExpectedCvGroup> Groups { get; set; } = new();

            public string DominantGroupKey { get; set; }

            public bool ExpectAmbiguous { get; set; }

            public List<ExpectedCvRelation> DominancePairs { get; set; } = new();

            public List<ExpectedCvRelation> IncomparablePairs { get; set; } = new();
        }

        private sealed class ExpectedCvGroup
        {
            public string GroupKey { get; set; }

            public string[] NodeTags { get; set; }

            public int ActionOrdinal { get; set; }
        }

        private sealed class ExpectedCvRelation
        {
            public string LeftGroupKey { get; set; }

            public string RightGroupKey { get; set; }
        }

        private enum ScenarioStepKind
        {
            Create,
            Update
        }

        private static readonly string[] OrderedNodeTags = { "A", "B", "C" };
    }
}
