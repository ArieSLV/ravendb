using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Server;
using Raven.Server.Commercial;
using Xunit;

namespace LicenseTests;

public class LicenseTestsFixtureBuilder<TOperation>
{
    private readonly LicenseLimitsTests _test;
    private RavenServer _server;
    private DocumentStore _store;
    private string _communityLicenseString = """
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
    private TOperation _putOperation;
    private TOperation _updateOperation;
    private long _putOperationTaskId;

    private LicenseTestsFixtureBuilder(LicenseLimitsTests test)
    {
        _test = test;
    }

    public static LicenseTestsFixtureBuilder<TOperation> Init(LicenseLimitsTests test)
    {
        var server = test.GetNewServer();
        return new LicenseTestsFixtureBuilder<TOperation>(test)
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

    public Fixture<TOperation> Build()
    {
        return new Fixture<TOperation>
        {
            Store = _store,
            PutOperation = _putOperation,
            UpdateOperation = _updateOperation,
            Server = _server,
            CommunityLicenseString = _communityLicenseString,
        };
    }



    public LicenseTestsFixtureBuilder<TOperation> WithPutOperation(Func<TOperation> action)
    {
        _putOperation = action();
        return this;
    }

    public LicenseTestsFixtureBuilder<TOperation> WithPutOperation(Func<DocumentStore, TOperation> action)
    {
        _putOperation = action(_store);
        return this;
    }

    public LicenseTestsFixtureBuilder<TOperation> WithPutOperation(Func<DocumentStore, RavenServer, TOperation> action)
    {
        _putOperation = action(_store, _server);
        return this;
    }

    public LicenseTestsFixtureBuilder<TOperation> WithCommunityLicense()
    {
        Task.Run(()=> LicenseLimitsTestsBase.SwitchToCommunityLicense(_server, _communityLicenseString));

        RavenTestBase.WaitForValue(() => _server.ServerStore.LicenseManager.LicenseStatus.Type, LicenseType.Community);
        Assert.Equal(LicenseType.Community, _server.ServerStore.LicenseManager.LicenseStatus.Type);

        return this;
    }

    public class Fixture<TOp>
    {
        public RavenServer Server { get; init; }
        public DocumentStore Store { get; init; }
        public TOp PutOperation { get; init; }
        public string CommunityLicenseString { get; init; }
        public TOp UpdateOperation { get; init; }
        public long TaskId { get; set; }
    }
}
