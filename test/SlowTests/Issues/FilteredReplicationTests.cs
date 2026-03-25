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
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
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
    }
}
