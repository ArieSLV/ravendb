﻿import { createAsyncThunk, createEntityAdapter, createSlice, EntityState, PayloadAction } from "@reduxjs/toolkit";
import { DatabaseFilterCriteria, DatabaseLocalInfo, DatabaseSharedInfo } from "components/models/databases";
import genUtils from "common/generalUtils";
import { AppAsyncThunk, AppDispatch, AppThunk, RootState } from "components/store";
import createDatabase from "viewmodels/resources/createDatabase";
import app from "durandal/app";
import deleteDatabaseConfirm from "viewmodels/resources/deleteDatabaseConfirm";
import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import disableDatabaseToggleConfirm from "viewmodels/resources/disableDatabaseToggleConfirm";
import viewHelpers from "common/helpers/view/viewHelpers";
import changesContext from "common/changesContext";
import compactDatabaseDialog from "viewmodels/resources/compactDatabaseDialog";
import databasesManager from "common/shell/databasesManager";
import { locationAwareLoadableData, perNodeTagLoadStatus } from "components/models/common";
import { services } from "hooks/useServices";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { databaseLocationComparator } from "components/utils/common";
import disableIndexingToggleConfirm from "viewmodels/resources/disableIndexingToggleConfirm";
import notificationCenter from "common/notifications/notificationCenter";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import assertUnreachable from "components/utils/assertUnreachable";

interface DatabasesState {
    /**
     * global database information - sharded between shards/nodes - i.e. name, encryption, etc
     */
    databases: EntityState<DatabaseSharedInfo>; // global database information
    /**
     * holds database info specific for given shard/node (document count, errors, etc)
     */
    localDatabaseDetailedInfo: EntityState<DatabaseLocalInfo>;
    /**
     * Data loading status per each node
     */
    localDatabaseDetailedLoadStatus: EntityState<perNodeTagLoadStatus>;
    activeDatabase: string;

    searchCriteria: DatabaseFilterCriteria;
}

const databasesAdapter = createEntityAdapter<DatabaseSharedInfo>({
    selectId: (x) => x.name,
    sortComparer: (a, b) => genUtils.sortAlphaNumeric(a.name, b.name),
});

const selectDatabaseInfoId = (dbName: string, location: databaseLocationSpecifier) =>
    dbName + "_$$$_" + genUtils.formatLocation(location);

const localDatabaseInfoAdapter = createEntityAdapter<DatabaseLocalInfo>({
    selectId: (x) => selectDatabaseInfoId(x.name, x.location),
});

const localDatabaseDetailedLoadStatusAdapter = createEntityAdapter<perNodeTagLoadStatus>({
    selectId: (x) => x.nodeTag,
});

const databasesSelectors = databasesAdapter.getSelectors();
const localDatabaseDetailedLoadStatusSelectors = localDatabaseDetailedLoadStatusAdapter.getSelectors();
const localDatabaseDetailedInfoSelectors = localDatabaseInfoAdapter.getSelectors();

const initialState: DatabasesState = {
    databases: databasesAdapter.getInitialState(),
    localDatabaseDetailedInfo: localDatabaseInfoAdapter.getInitialState(),
    localDatabaseDetailedLoadStatus: localDatabaseDetailedLoadStatusAdapter.getInitialState(),
    activeDatabase: null,
    searchCriteria: {
        searchText: "",
    },
};

const sliceName = "databases";

export const selectAllDatabases = (store: RootState) => databasesSelectors.selectAll(store.databases.databases);

export const selectDatabaseSearchCriteria = (store: RootState) => store.databases.searchCriteria;

export const selectDatabasesSummary = (store: RootState) => {
    const allDatabases = selectAllDatabases(store);

    let loading = 0;
    let error = 0;
    let offline = 0;
    let disabled = 0;
    let online = 0;

    allDatabases.forEach((db) => {
        const perNodeState = selectDatabaseState(db.name)(store);
        const state = DatabaseUtils.getDatabaseState(db, perNodeState);
        switch (state) {
            case "Loading":
                loading++;
                break;
            case "Error":
                error++;
                break;
            case "Offline":
                offline++;
                break;
            case "Disabled":
                disabled++;
                break;
            case "Online":
                online++;
                break;
            default:
                assertUnreachable(state);
        }
    });

    return {
        count: allDatabases.length,
        loading,
        error,
        offline,
        disabled,
        online,
    };
};

export const selectFilteredDatabases = (store: RootState): DatabaseSharedInfo[] => {
    const criteria = selectDatabaseSearchCriteria(store);
    const databases = selectAllDatabases(store);

    if (criteria.searchText) {
        return databases.filter((x) => x.name.toLowerCase().includes(criteria.searchText.toLowerCase()));
    }

    return databases;
};

export const selectActiveDatabase = (store: RootState) => store.databases.activeDatabase;

export function selectDatabaseByName(name: string) {
    return (store: RootState) => databasesSelectors.selectById(store.databases.databases, name);
}

export function selectDatabaseState(name: string) {
    return (store: RootState) => {
        const db = selectDatabaseByName(name)(store);

        const locations = DatabaseUtils.getLocations(db);

        return locations.map((location): locationAwareLoadableData<DatabaseLocalInfo> => {
            const loadState = localDatabaseDetailedLoadStatusSelectors.selectById(
                store.databases.localDatabaseDetailedLoadStatus,
                location.nodeTag
            ) || {
                status: "idle",
                nodeTag: location.nodeTag,
            };

            switch (loadState.status) {
                case "idle":
                case "loading":
                    return {
                        status: loadState.status,
                        location,
                    };
                case "failure":
                    return {
                        status: "failure",
                        location,
                    };
                case "success": {
                    const data = localDatabaseDetailedInfoSelectors.selectById(
                        store.databases.localDatabaseDetailedInfo,
                        selectDatabaseInfoId(name, location)
                    );

                    if (!data) {
                        // where was change and we don't have yet new data
                        // TODO: start fetching?
                        return {
                            location,
                            status: "idle",
                        };
                    }

                    return {
                        location,
                        status: "success",
                        data,
                    };
                }
            }
        });
    };
}

export const databasesSlice = createSlice({
    initialState,
    name: sliceName,
    reducers: {
        disabledIndexing: (state, action: PayloadAction<string>) => {
            state.localDatabaseDetailedInfo.ids.forEach((id) => {
                const entity = state.localDatabaseDetailedInfo.entities[id];
                if (entity.name === action.payload) {
                    entity.indexingStatus = "Disabled";
                }
            });
        },
        enabledIndexing: (state, action: PayloadAction<string>) => {
            state.localDatabaseDetailedInfo.ids.forEach((id) => {
                const entity = state.localDatabaseDetailedInfo.entities[id];
                if (entity.name === action.payload) {
                    entity.indexingStatus = "Running";
                }
            });
        },
        pausedIndexing: {
            reducer: (state, action: PayloadAction<{ databaseName: string; location: databaseLocationSpecifier }>) => {
                state.localDatabaseDetailedInfo.ids.forEach((id) => {
                    const entity = state.localDatabaseDetailedInfo.entities[id];
                    if (
                        entity.name === action.payload.databaseName &&
                        databaseLocationComparator(entity.location, action.payload.location)
                    ) {
                        entity.indexingStatus = "Paused";
                    }
                });
            },
            prepare: (databaseName: string, location: databaseLocationSpecifier) => {
                return {
                    payload: { databaseName, location },
                };
            },
        },
        resumedIndexing: {
            reducer: (state, action: PayloadAction<{ databaseName: string; location: databaseLocationSpecifier }>) => {
                state.localDatabaseDetailedInfo.ids.forEach((id) => {
                    const entity = state.localDatabaseDetailedInfo.entities[id];
                    if (
                        entity.name === action.payload.databaseName &&
                        databaseLocationComparator(entity.location, action.payload.location)
                    ) {
                        entity.indexingStatus = "Running";
                    }
                });
            },
            prepare: (databaseName: string, location: databaseLocationSpecifier) => {
                return {
                    payload: { databaseName, location },
                };
            },
        },
        activeDatabaseChanged: (state, action: PayloadAction<string>) => {
            state.activeDatabase = action.payload;
        },
        databasesLoaded: (state, action: PayloadAction<DatabaseSharedInfo[]>) => {
            //TODO: update in shallow mode?
            databasesAdapter.setAll(state.databases, action.payload);
        },
        filterTextSet: (state, action: PayloadAction<string>) => {
            state.searchCriteria.searchText = action.payload;
        },
        initDetails: {
            reducer: (state, action: PayloadAction<{ nodeTags: string[] }>) => {
                localDatabaseDetailedLoadStatusAdapter.setAll(
                    state.localDatabaseDetailedLoadStatus,
                    action.payload.nodeTags.map((tag) => ({
                        nodeTag: tag,
                        status: "idle",
                    }))
                );
            },
            prepare: (nodeTags: string[]) => {
                return {
                    payload: {
                        nodeTags,
                    },
                };
            },
        },
    },
    extraReducers: (builder) => {
        builder.addCase(fetchDatabases.fulfilled, (state, action) => {
            const nodeTag = action.meta.arg;

            state.localDatabaseDetailedLoadStatus.entities[nodeTag].status = "success";

            action.payload.Databases.forEach((db) => {
                const newEntity: DatabaseLocalInfo = {
                    name: DatabaseUtils.shardGroupKey(db.Name),
                    location: {
                        nodeTag,
                        shardNumber: DatabaseUtils.shardNumber(db.Name),
                    },
                    alerts: db.Alerts,
                    loadError: db.LoadError,
                    documentsCount: db.DocumentsCount,
                    indexingStatus: db.IndexingStatus,
                    indexingErrors: db.IndexingErrors,
                    performanceHints: db.PerformanceHints,
                    upTime: db.UpTime ? genUtils.timeSpanAsAgo(db.UpTime, false) : null, // we format here to avoid constant updates of UI
                    backupInfo: db.BackupInfo,
                    totalSize: db.TotalSize,
                    tempBuffersSize: db.TempBuffersSize,
                };

                const existingInfo = localDatabaseDetailedInfoSelectors.selectById(
                    state.localDatabaseDetailedInfo,
                    localDatabaseInfoAdapter.selectId(newEntity)
                );
                if (!existingInfo || JSON.stringify(existingInfo) !== JSON.stringify(newEntity)) {
                    localDatabaseInfoAdapter.setOne(state.localDatabaseDetailedInfo, newEntity);
                }
            });
        });

        builder.addCase(fetchDatabases.rejected, (state, action) => {
            localDatabaseDetailedLoadStatusAdapter.setOne(state.localDatabaseDetailedLoadStatus, {
                nodeTag: action.meta.arg,
                status: "failure",
            });
        });
    },
});

export const { databasesLoaded, activeDatabaseChanged, initDetails, filterTextSet } = databasesSlice.actions;

export const loadDatabaseDetails = (nodeTags: string[]) => async (dispatch: AppDispatch, getState: () => RootState) => {
    dispatch(initDetails(nodeTags));

    const tasks = nodeTags.map((nodeTag) => dispatch(fetchDatabases(nodeTag)));

    await Promise.all(tasks);
};

export const reloadDatabaseDetails = async (dispatch: AppDispatch, getState: () => RootState) => {
    //TODO: read from redux!
    const nodeTags =
        clusterTopologyManager.default
            .topology()
            ?.nodes()
            ?.map((x) => x.tag()) ?? [];

    const tasks = nodeTags.map((nodeTag) => dispatch(fetchDatabases(nodeTag)));

    await Promise.all(tasks);
};

export const throttledReloadDatabaseDetails = _.throttle(reloadDatabaseDetails, 8000);

const fetchDatabases = createAsyncThunk(sliceName + "/fetchDatabases", async (nodeTag: string) => {
    return await services.databasesService.getDatabasesState(nodeTag);
});

export const openCreateDatabaseDialog = () => () => {
    const createDbView = new createDatabase("newDatabase");
    app.showBootstrapDialog(createDbView);
};

export const openCreateDatabaseFromRestoreDialog = () => () => {
    const createDbView = new createDatabase("restore");
    app.showBootstrapDialog(createDbView);
};

export const confirmToggleIndexing =
    (db: DatabaseSharedInfo, disable: boolean): AppAsyncThunk<{ can: boolean }> =>
    async () => {
        const confirmDeleteViewModel = new disableIndexingToggleConfirm(disable);
        app.showBootstrapDialog(confirmDeleteViewModel);
        return confirmDeleteViewModel.result;
    };

export const toggleIndexing =
    (db: DatabaseSharedInfo, disable: boolean): AppAsyncThunk =>
    async (dispatch, getState, getServices) => {
        const { indexesService } = getServices();

        if (disable) {
            await indexesService.disableAllIndexes(db);
            dispatch(databasesSlice.actions.disabledIndexing(db.name));
        } else {
            await indexesService.enableAllIndexes(db);
            dispatch(databasesSlice.actions.enabledIndexing(db.name));
        }
    };

export const openNotificationCenterForDatabase =
    (db: DatabaseSharedInfo): AppThunk =>
    (dispatch, getState) => {
        const activeDatabase = selectActiveDatabase(getState());
        if (activeDatabase !== db.name) {
            const dbRaw = databasesManager.default.getDatabaseByName(db.name);
            if (dbRaw) {
                databasesManager.default.activate(dbRaw);
            }
        }

        notificationCenter.instance.showNotifications.toggle();
    };

export const confirmTogglePauseIndexing =
    (db: DatabaseSharedInfo, pause: boolean): AppAsyncThunk<{ can: boolean; locations: databaseLocationSpecifier[] }> =>
    async () => {
        //TODO: context selector!
        const msg = pause ? "pause indexing?" : "resume indexing?";
        const result = await viewHelpers.confirmationMessage("Are you sure?", `Do you want to ` + msg);

        return {
            can: result.can,
            locations: DatabaseUtils.getLocations(db),
        };
    };

export const togglePauseIndexing =
    (db: DatabaseSharedInfo, pause: boolean, locations: databaseLocationSpecifier[]): AppAsyncThunk =>
    async (dispatch, getState, getServices) => {
        const { indexesService } = getServices();

        if (pause) {
            const tasks = locations.map(async (l) => {
                await indexesService.pauseAllIndexes(db, l);
                dispatch(databasesSlice.actions.pausedIndexing(db.name, l));
            });
            await Promise.all(tasks);
        } else {
            const tasks = locations.map(async (l) => {
                await indexesService.resumeAllIndexes(db, l);
                dispatch(databasesSlice.actions.resumedIndexing(db.name, l));
            });
            await Promise.all(tasks);
        }
    };

//TODO: report success after database deletion? - what about other actions?
export const confirmDeleteDatabases =
    (
        toDelete: DatabaseSharedInfo[]
    ): AppAsyncThunk<{ can: boolean; keepFiles?: boolean; databases?: DatabaseSharedInfo[] }> =>
    async (): Promise<{ can: boolean; keepFiles?: boolean; databases?: DatabaseSharedInfo[] }> => {
        const selectedDatabasesWithoutLock = toDelete.filter((x) => x.lockMode === "Unlock");
        if (selectedDatabasesWithoutLock.length === 0) {
            return {
                can: false,
            };
        }

        const confirmDeleteViewModel = new deleteDatabaseConfirm(selectedDatabasesWithoutLock);
        app.showBootstrapDialog(confirmDeleteViewModel);
        const baseResult = await confirmDeleteViewModel.result;
        return {
            ...baseResult,
            databases: selectedDatabasesWithoutLock,
        };
    };

export const deleteDatabases =
    (toDelete: DatabaseSharedInfo[], keepFiles: boolean): AppAsyncThunk<updateDatabaseConfigurationsResult> =>
    async (dispatch, getState, getServices) => {
        const { databasesService } = getServices();
        /* TODO:
           const dbsList = toDelete.map(x => {
               //TODO: x.isBeingDeleted(true);
               const asDatabase = x.asDatabase();

               // disconnect here to avoid race condition between database deleted message
               // and websocket disconnection
               //TODO: changesContext.default.disconnectIfCurrent(asDatabase, "DatabaseDeleted");
               return asDatabase;
           });*/

        return databasesService.deleteDatabase(
            toDelete.map((x) => x.name),
            !keepFiles
        );
    };

export const compactDatabase = (database: DatabaseSharedInfo) => () => {
    const db = databasesManager.default.getDatabaseByName(database.name);
    if (db) {
        changesContext.default.disconnectIfCurrent(db, "DatabaseDisabled");
    }
    app.showBootstrapDialog(new compactDatabaseDialog(database));
};

export const changeDatabasesLockMode =
    (databases: DatabaseSharedInfo[], lockMode: DatabaseLockMode): AppAsyncThunk =>
    async (dispatch, getState, getServices) => {
        const { databasesService } = getServices();

        await databasesService.setLockMode(databases, lockMode);
    };

export const confirmToggleDatabases =
    (databases: DatabaseSharedInfo[], enable: boolean): AppAsyncThunk<boolean> =>
    async () => {
        const confirmation = new disableDatabaseToggleConfirm(databases, !enable);
        app.showBootstrapDialog(confirmation);

        const result = await confirmation.result;
        return result.can;
    };

export const toggleDatabases =
    (databases: DatabaseSharedInfo[], enable: boolean): AppAsyncThunk =>
    async (dispatch, getState, getServices) => {
        const { databasesService } = getServices();
        // TODO: lazy update UI
        await databasesService.toggle(databases, enable);
    };

/* TODO
    private onDatabaseDisabled(result: disableDatabaseResult) {
        const dbs = this.databases().sortedDatabases();
        const matchedDatabase = dbs.find(rs => rs.name === result.Name);

        if (matchedDatabase) {
            matchedDatabase.disabled(result.Disabled);

            // If Enabling a database (that is selected from the top) than we want it to be Online(Loaded)
            if (matchedDatabase.isCurrentlyActiveDatabase() && !matchedDatabase.disabled()) {
                new loadDatabaseCommand(matchedDatabase.asDatabase())
                    .execute();
            }
        }
    }
 */

export const confirmSetLockMode = (): AppAsyncThunk<boolean> => async () => {
    const result = await viewHelpers.confirmationMessage("Are you sure?", `Do you want to change lock mode?`);

    return result.can;
};