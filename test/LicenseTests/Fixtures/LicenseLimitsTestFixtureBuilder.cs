using Raven.Client.Documents;
using Raven.Server;

namespace LicenseTests.Fixtures;

public class LicenseLimitsTestFixtureBuilder : LicenseLimitsTestFixtureBuilderBase<LicenseLimitsTestFixtureBuilder>
{
    public override Fixture Build() =>
        new()
        {
            Store = _store,
            Server = _server
        };

    public class Fixture : ILicenseLimitsTestsFixture
    {
        public RavenServer Server { get; init; }
        public DocumentStore Store { get; init; }
        public string CommunityLicenseString { get; init; }
    }
}
