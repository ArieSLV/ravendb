using System;
using Raven.Client.Documents;
using Raven.Server;

namespace LicenseTests.Fixtures
{
    public class LicenseLimitsOperationsTestFixtureBuilder<TOperation> : LicenseLimitsTestFixtureBuilderBase<LicenseLimitsOperationsTestFixtureBuilder<TOperation>>
    {
        private TOperation _putOperation;

        public LicenseLimitsOperationsTestFixtureBuilder<TOperation> WithPutOperation(Func<TOperation> action)
        {
            _putOperation = action();
            return this;
        }

        public override Fixture<TOperation> Build()
        {
            return new Fixture<TOperation>
            {
                Store = _store,
                PutOperation = _putOperation,
                Server = _server,
            };
        }

        public class Fixture<T> : ILicenseLimitsTestsFixture<T>
        {
            public RavenServer Server { get; init; }
            public DocumentStore Store { get; init; }
            public T PutOperation { get; init; }
        }
    }

    public class LicenseLimitsOperationsTestFixtureBuilder<TOperation, TResult> : LicenseLimitsTestFixtureBuilderBase<LicenseLimitsOperationsTestFixtureBuilder<TOperation, TResult>>
    {
        private TOperation _putOperation;

        public LicenseLimitsOperationsTestFixtureBuilder<TOperation, TResult> WithPutOperation(Func<TOperation> action)
        {
            _putOperation = action();
            return this;
        }

        public LicenseLimitsOperationsTestFixtureBuilder<TOperation, TResult> WithPutOperation(Func<DocumentStore, TOperation> action)
        {
            _putOperation = action(_store);
            return this;
        }

        public LicenseLimitsOperationsTestFixtureBuilder<TOperation, TResult> WithPutOperation(Func<DocumentStore, RavenServer, TOperation> action)
        {
            _putOperation = action(_store, _server);
            return this;
        }

        public override Fixture<TOperation, TResult> Build()
        {
            return new Fixture<TOperation, TResult>
            {
                Store = _store,
                PutOperation = _putOperation,
                Server = _server,
            };
        }

        public class Fixture<TO, TR> : ILicenseLimitsTestsFixture<TO, TR>
        {
            public RavenServer Server { get; init; }
            public DocumentStore Store { get; init; }
            public TO PutOperation { get; init; }
            public TR PutResult { get; set; }
        }
    }
}
