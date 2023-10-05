using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Server;
using Raven.Server.Commercial;
using Xunit;

namespace LicenseTests.Fixtures
{
    public interface ILicenseLimitsTestsFixture
    {
        RavenServer Server { get; init; }
        DocumentStore Store { get; init; }
        string CommunityLicenseString { get; init; }
    }

    public interface ILicenseLimitsTestsFixture<TOperation> : ILicenseLimitsTestsFixture
    {
        public TOperation PutOperation { get; init; }
    }

    public interface ILicenseLimitsTestsFixture<TOperation, TResult> : ILicenseLimitsTestsFixture<TOperation>
    {
        public TResult PutResult { get; set; }
    }

    public abstract class LicenseLimitsTestFixtureBuilderBase<T> where T : LicenseLimitsTestFixtureBuilderBase<T>, new()
    {
        internal RavenServer _server;
        internal DocumentStore _store;
        internal const string CommunityLicenseStringConst = """
                                                            {
                                                                    "Id": "7d8ac926-51c0-41e6-9ded-61b82d08f019",
                                                                    "Name": "lev.skuditsky@ravendb.net",
                                                                    "Keys": [
                                                                        "ZdG+zCu5mQ7gL7zo1swziqnNp",
                                                                        "yXL4UYst/UJyKQNKm2mZ5hp0+",
                                                                        "E1nsVeuP31GSiYu/RMmbSJzCh",
                                                                        "6qqq7TsRwZiqP62/1l2nxANx6",
                                                                        "f1F8pgaJsw70cydUMza1RHnU4",
                                                                        "o3zKAqya+VCpdiXqv7j6wsmxS",
                                                                        "lgfRKcUPjZQVD+9gBqredAAYE",
                                                                        "DNi4wBQYHKEkDCgsMDQ4PEBES",
                                                                        "ExQ1FhcYGRobHB0enwIfAJ8CI",
                                                                        "ACfAiEgnwIjAJ8DMkACnwMzQC",
                                                                        "2fAzRAJJ8DNUAknwM2QAGfAzd",
                                                                        "ADJ8DOUAYnwM4QDyfAzpAeJ8D",
                                                                        "O0ADnwM8QA+fAz1AAZ8DPkAFn",
                                                                        "wM/QAGfA0BABZ8CJACfAiUAnw",
                                                                        "ImAJ8CJwCfAigAnwIpAJ8CKgC",
                                                                        "fAisAnwIsAJ8CLQCfAi4AnwIv",
                                                                        "AJ8CMABDBkQDYlpY"
                                                                    ]
                                                            }
                                                            """;


        private void SetCommunityLicense()
        {
            Task.Run(() => LicenseLimitsTestsBase.SwitchToCommunityLicense(_server, CommunityLicenseStringConst));

            RavenTestBase.WaitForValue(() => _server.ServerStore.LicenseManager.LicenseStatus.Type, LicenseType.Community);
            Assert.Equal(LicenseType.Community, _server.ServerStore.LicenseManager.LicenseStatus.Type);
        }

        public T WithCommunityLicense()
        {
            SetCommunityLicense();
            return (T)this;
        }

        public abstract ILicenseLimitsTestsFixture Build();

        public static T Init(LicenseLimitsTests test)
        {
            var server = test.GetNewServer();
            return new T
            {
                _server = server,
                _store = test.GetDocumentStore(new RavenTestBase.Options
                {
                    Server = server,
                    RunInMemory = false,
                    ModifyDatabaseRecord = record => record.DocumentsCompression = new DocumentsCompressionConfiguration
                    {
                        CompressRevisions = false, CompressAllCollections = false
                    }
                })
            };
        }

        // public static T InitSharded(LicenseLimitsTests test, RavenTestBase.Options options = null)
        // {
        //     var server = test.GetNewServer();
        //
        //     options ??= test.GetOptionsForCluster(
        //         server,
        //         shards: 2,
        //         shardReplicationFactor: 1,
        //         orchestratorReplicationFactor: 1);
        //
        //     var existingDelegate = options.ModifyDatabaseRecord;
        //     options.ModifyDatabaseRecord = record =>
        //     {
        //         existingDelegate?.Invoke(record);
        //         record.DocumentsCompression = new DocumentsCompressionConfiguration
        //         {
        //             CompressRevisions = false,
        //             CompressAllCollections = false
        //         };
        //     };
        //
        //     options.RunInMemory = false;
        //
        //     return new T
        //     {
        //         _server = server,
        //         _store = test.Sharding.GetDocumentStore(options)
        //     };
        // }
    }
}
