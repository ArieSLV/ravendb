﻿import { DatabaseSharedInfo, NonShardedDatabaseInfo, ShardedDatabaseInfo } from "components/models/databases";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { MockedValue } from "test/mocks/services/AutoMockService";
import { createValue } from "../utils";
import { globalDispatch } from "components/storeCompat";
import { databaseActions } from "components/common/shell/databaseSliceActions";

export class MockDatabaseManager {
    with_Cluster(dto?: MockedValue<NonShardedDatabaseInfo>) {
        const value = this.createValue(dto, DatabasesStubs.nonShardedClusterDatabase().toDto());

        globalDispatch(databaseActions.databasesLoaded([value]));
        return value;
    }

    with_Sharded(dto?: MockedValue<ShardedDatabaseInfo>) {
        const value = this.createValue(dto, DatabasesStubs.shardedDatabase().toDto());
        globalDispatch(databaseActions.databasesLoaded([value]));
        return value;
    }

    with_Single(dto?: MockedValue<DatabaseSharedInfo>) {
        const value = this.createValue(dto, DatabasesStubs.nonShardedSingleNodeDatabase().toDto());
        globalDispatch(databaseActions.databasesLoaded([value]));
        return value;
    }

    withDatabases(dbs: DatabaseSharedInfo[]) {
        globalDispatch(databaseActions.databasesLoaded(dbs));
    }

    withActiveDatabase_NonSharded_Cluster(db?: MockedValue<ShardedDatabaseInfo>) {
        const value = this.createValue(db, DatabasesStubs.nonShardedClusterDatabase().toDto());
        return this.withActiveDatabase(value);
    }

    withActiveDatabase_NonSharded_SingleNode(db?: MockedValue<ShardedDatabaseInfo>) {
        const value = this.createValue(db, DatabasesStubs.nonShardedSingleNodeDatabase().toDto());
        return this.withActiveDatabase(value);
    }

    withActiveDatabase_Sharded(db?: MockedValue<ShardedDatabaseInfo>) {
        const value = this.createValue(db, DatabasesStubs.shardedDatabase().toDto());
        return this.withActiveDatabase(value);
    }

    withActiveDatabase(db?: MockedValue<DatabaseSharedInfo>) {
        const value = this.createValue(db, DatabasesStubs.nonShardedSingleNodeDatabase().toDto());

        globalDispatch(databaseActions.activeDatabaseChanged(value.name));
        globalDispatch(databaseActions.databasesLoaded([value]));

        return value;
    }

    protected createValue<T>(value: MockedValue<T>, defaultValue: T): T {
        return createValue(value, defaultValue);
    }
}
