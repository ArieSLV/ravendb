using System;
using Raven.Client.Documents;
using Raven.Client.Documents.Subscriptions;
using Raven.Server;

namespace LicenseTests.Fixtures;

public class LicenseLimitsSubscriptionsTestFixtureBuilder : LicenseLimitsTestFixtureBuilderBase<LicenseLimitsSubscriptionsTestFixtureBuilder>
{
    private SubscriptionCreationOptions _subscriptionCreationOptions;
    private SubscriptionUpdateOptions _subscriptionUpdateOptions;

    public LicenseLimitsSubscriptionsTestFixtureBuilder WithCreateOptions(Func<SubscriptionCreationOptions> options)
    {
        _subscriptionCreationOptions = options();
        return this;
    }

    public LicenseLimitsSubscriptionsTestFixtureBuilder WithUpdatedOptions(Func<SubscriptionUpdateOptions> options)
    {
        _subscriptionUpdateOptions = options();
        return this;
    }

    public override Fixture Build()
    {
        return new Fixture
        {
            Store = _store,
            SubscriptionCreationOptions = _subscriptionCreationOptions,
            SubscriptionUpdateOption = _subscriptionUpdateOptions,
            Server = _server
        };
    }

    public class Fixture : ILicenseLimitsTestsFixture
    {
        public RavenServer Server { get; init; }
        public DocumentStore Store { get; init; }
        public string CommunityLicenseString { get; init; }
        public SubscriptionCreationOptions SubscriptionCreationOptions { get; set; }
        public SubscriptionUpdateOptions SubscriptionUpdateOption { get; set; }
        public long SubscriptionId { get; set; }
    }
}
