﻿using System.Linq;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues
{
    public class RavenDB_20487 : ReplicationTestBase
    {
        public RavenDB_20487(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        public async Task ReplicationToShardedAndThenToNonShardedShouldWork()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = Sharding.GetDocumentStore(new Options()
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Replication.MaxItemsCount)] = "10"
            }))
            using (var store3 = GetDocumentStore())
            {
                var count = 100;
                for (int i = 0; i < count; i++)
                {
                    using (var session = store1.OpenSession())
                    {
                        session.Store(new User() { Age = i }, $"Users/{i}");
                        session.SaveChanges();
                    }
                }

                await SetupReplicationAsync(store1, store2);

                var res = WaitForValue(() =>
                {
                    using (var session = store2.OpenSession())
                    {
                        return session.Query<User>().Count();
                    }
                }, count, timeout: 60_000, interval: 333);

                Assert.Equal(count, res);

                await SetupReplicationAsync(store2, store3);

                res = WaitForValue(() =>
                {
                    using (var session = store3.OpenSession())
                    {
                        return session.Query<User>().Count();
                    }
                }, count, timeout: 60_000, interval: 333);

                Assert.Equal(count, res);
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Revisions | RavenTestCategory.Sharding)]
        public async Task ReplicationWithRevisionsToShardedAndThenToNonShardedShouldWork()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = Sharding.GetDocumentStore(new Options()
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Replication.MaxItemsCount)] = "10"
            }))
            using (var store3 = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisionsAsync(store1);

                var count = 100;
                for (int i = 0; i < count; i++)
                {
                    using (var session = store1.OpenSession())
                    {
                        session.Store(new User() { Age = i }, $"Users/{i}");
                        session.SaveChanges();
                    }
                }

                // create revisions
                for (int i = 0; i < count; i++)
                {
                    using (var session = store1.OpenSession())
                    {
                        var user = session.Load<User>($"Users/{i}");
                        Assert.NotNull(user);
                        user.Age = i + 1;
                        session.SaveChanges();
                    }
                }

                await SetupReplicationAsync(store1, store2);

                var res = WaitForValue(() =>
                {
                    using (var session = store2.OpenSession())
                    {
                        return session.Query<User>().Count();
                    }
                }, count, timeout: 60_000, interval: 333);

                Assert.Equal(count, res);

                var expectedRevisionsCount = count * 2;
                res = await WaitForValueAsync(async () =>
                {
                    var stats = await GetDatabaseStatisticsAsync(store2);
                    return (int)stats.CountOfRevisionDocuments;
                }, expectedRevisionsCount, timeout: 30_000, interval: 333);

                Assert.Equal(expectedRevisionsCount, res);

                await SetupReplicationAsync(store2, store3);

                res = WaitForValue(() =>
                {
                    using (var session = store3.OpenSession())
                    {
                        return session.Query<User>().Count();
                    }
                }, count, timeout: 60_000, interval: 333);

                Assert.Equal(count, res);

                res = await WaitForValueAsync(async () =>
                {
                    var stats = await GetDatabaseStatisticsAsync(store3);
                    return (int)stats.CountOfRevisionDocuments;
                }, expectedRevisionsCount, timeout: 30_000, interval: 333);

                Assert.Equal(expectedRevisionsCount, res);
            }
        }
    }
}