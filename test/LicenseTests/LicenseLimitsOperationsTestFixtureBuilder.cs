using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Server;
using Raven.Server.Commercial;
using Tests.Infrastructure;
using Xunit;

namespace LicenseTests;

public class LicenseLimitsTestFixtureBuilderBase(LicenseLimitsTests test)
{
    private protected readonly LicenseLimitsTests Test = test;
    private protected RavenServer Server;
    private protected DocumentStore Store;

    protected const string CommunityLicenseStringConst = """
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

    protected void SetCommunityLicense()
    {
        Task.Run(()=> LicenseLimitsTestsBase.SwitchToCommunityLicense(Server, CommunityLicenseStringConst));

        RavenTestBase.WaitForValue(() => Server.ServerStore.LicenseManager.LicenseStatus.Type, LicenseType.Community);
        Assert.Equal(LicenseType.Community, Server.ServerStore.LicenseManager.LicenseStatus.Type);
    }

}

public class LicenseLimitsSubscriptionsTestFixtureBuilder(LicenseLimitsTests test) : LicenseLimitsTestFixtureBuilderBase(test)
{
    private SubscriptionCreationOptions _subscriptionCreationOptions;

    public static LicenseLimitsSubscriptionsTestFixtureBuilder Init(LicenseLimitsTests test)
    {
        var server = test.GetNewServer();
        return new LicenseLimitsSubscriptionsTestFixtureBuilder(test)
        {
            Server = server,
            Store = test.GetDocumentStore(new RavenTestBase.Options
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

    public LicenseLimitsSubscriptionsTestFixtureBuilder WithSubscription(Func<SubscriptionCreationOptions> options)
    {
        _subscriptionCreationOptions = options();
        return this;
    }

    public LicenseLimitsSubscriptionsTestFixtureBuilder WithCommunityLicense()
    {
        SetCommunityLicense();
        return this;
    }

    public Fixture Build()
    {
        return new Fixture
        {
            Store = Store,
            SubscriptionCreationOptions = _subscriptionCreationOptions,
            Server = Server,
            CommunityLicenseString = CommunityLicenseStringConst,
        };
    }

    public class Fixture
    {
        public RavenServer Server { get; init; }
        public DocumentStore Store { get; init; }
        public string CommunityLicenseString { get; init; }
        public SubscriptionCreationOptions SubscriptionCreationOptions { get; init; }
    }
}


public class LicenseLimitsOperationsTestFixtureBuilder<TOperation>(LicenseLimitsTests test) : LicenseLimitsTestFixtureBuilderBase(test)
{
    private TOperation _putOperation;

    public static LicenseLimitsOperationsTestFixtureBuilder<TOperation> Init(LicenseLimitsTests test)
    {
        var server = test.GetNewServer();
        return new LicenseLimitsOperationsTestFixtureBuilder<TOperation>(test)
        {
            Server = server,
            Store = test.GetDocumentStore(new RavenTestBase.Options
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

    public static LicenseLimitsOperationsTestFixtureBuilder<TOperation> InitSharded(LicenseLimitsTests test)
    {
        var server = test.GetNewServer();
        return new LicenseLimitsOperationsTestFixtureBuilder<TOperation>(test)
        {
            Server = server,
            Store = test.Sharding.GetDocumentStore(new RavenTestBase.Options
            {
                Server = server,
                DatabaseMode = RavenDatabaseMode.Sharded,
                RunInMemory = false,
                ModifyDatabaseRecord = record => record.DocumentsCompression = new DocumentsCompressionConfiguration
                {
                    CompressRevisions = false, CompressAllCollections = false
                }
            })
        };
    }





    public LicenseLimitsOperationsTestFixtureBuilder<TOperation> WithPutOperation(Func<TOperation> action)
    {
        _putOperation = action();
        return this;
    }

    public LicenseLimitsOperationsTestFixtureBuilder<TOperation> WithPutOperation(Func<DocumentStore, TOperation> action)
    {
        _putOperation = action(Store);
        return this;
    }

    public LicenseLimitsOperationsTestFixtureBuilder<TOperation> WithPutOperation(Func<DocumentStore, RavenServer, TOperation> action)
    {
        _putOperation = action(Store, Server);
        return this;
    }



    public LicenseLimitsOperationsTestFixtureBuilder<TOperation> WithCommunityLicense()
    {
        SetCommunityLicense();
        return this;
    }

    public Fixture<TOperation> Build()
    {
        return new Fixture<TOperation>
        {
            Store = Store,
            PutOperation = _putOperation,
            Server = Server,
            CommunityLicenseString = CommunityLicenseStringConst,
        };
    }

    public class Fixture<T>
    {
        public RavenServer Server { get; init; }
        public DocumentStore Store { get; init; }
        public T PutOperation { get; init; }
        public string CommunityLicenseString { get; init; }
    }
}
