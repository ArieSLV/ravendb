using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Server;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Xunit;

namespace LicenseTests.Fixtures
{
    public interface ILicenseLimitsTestsFixture
    {
        RavenServer Server { get; init; }
        DocumentStore Store { get; init; }
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

        private void SetLicenseWithFeatureDisabled()
        {
            Task.Run(() => LicenseLimitsTestsBase.SwitchToLicenseWithFeatureDisabled(_server));

            RavenTestBase.WaitForValue(() => _server.ServerStore.LicenseManager.LicenseStatus.Type, LicenseType.Community);
            Assert.Equal(LicenseType.Community, _server.ServerStore.LicenseManager.LicenseStatus.Type);
        }

        public T WithLicenseFeatureDisabled()
        {
            SetLicenseWithFeatureDisabled();
            return (T)this;
        }

        public abstract ILicenseLimitsTestsFixture Build();

        public static T Init(LicenseLimitsTests test, bool licenseWithFeatureEnabled = true)
        {
            RavenServer server;
            if (licenseWithFeatureEnabled == false)
            {
                var options = new TestBase.ServerCreationOptions
                {
                    CustomSettings = new Dictionary<string, string>
                    {
                        {
                            RavenConfiguration.GetKey(x => x.Licensing.License),
                            Environment.GetEnvironmentVariable(LicenseLimitsTestsBase.EnvironmentVariableForLicenceKeyWithAllFeaturesDisabled)
                        }
                    }
                };
                server = test.GetNewServer(options);
            }
            else
            {
                server = test.GetNewServer();
            }

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

        public static T InitSharded(LicenseLimitsTests test, RavenTestBase.Options options)
        {
            var existingDelegate = options.ModifyDatabaseRecord;
            options.ModifyDatabaseRecord = record =>
            {
                existingDelegate?.Invoke(record);
                record.DocumentsCompression = new DocumentsCompressionConfiguration
                {
                    CompressRevisions = false,
                    CompressAllCollections = false
                };
            };

            options.RunInMemory = false;

            return new T
            {
                _server = options.Server,
                _store = test.Sharding.GetDocumentStore(options)
            };
        }
    }
}
