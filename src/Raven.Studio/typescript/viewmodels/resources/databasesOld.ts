import app = require("durandal/app");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import accessManager = require("common/shell/accessManager");
import createDatabase = require("viewmodels/resources/createDatabase");
type databaseState = "errored" | "disabled" | "online" | "offline" | "remote";
type filterState = databaseState | 'local' | 'all';

type OLD_DATABASES_INFO = {
    sortedDatabases: KnockoutObservableArray<any>;
    getByName: (name: string) => any;
    updateDatabase: (a: any) => void;
}

class databases extends viewModelBase {
    view = require("views/resources/databases.html")
    
    
    /*
    
    databases = ko.observable<OLD_DATABASES_INFO>(); //TODO: databasesInfo
    clusterManager = clusterTopologyManager.default;
    
    formatBytes = generalUtils.formatBytesToSize;

    filters = {
        searchText: ko.observable<string>(),
        requestedState: ko.observable<filterState>('all')
    };

    selectionState: KnockoutComputed<checkbox>;
    selectedDatabases = ko.observableArray<string>([]);
    selectedDatabasesWithoutLock: KnockoutComputed<databaseInfo[]>;

    lockModeCommon: KnockoutComputed<string>;
    
    databasesByState: KnockoutComputed<Record<databaseState, number>>;

    spinners = {
        globalToggleDisable: ko.observable<boolean>(false),
        localLockChanges: ko.observableArray<string>([]),
    };

    private static compactView = ko.observable<boolean>(false);
    compactView = databases.compactView;
    
    statsSubscription: changeSubscription;

    accessManager = accessManager.default.databasesView;
    
    databaseNameWidth = ko.observable<number>(350);

    environmentClass = (source: KnockoutObservable<Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment>) => 
        database.createEnvironmentColorComputed("label", source);
   
    databaseToCompact: string;
    popupRestoreDialog: boolean;
    
    notificationCenter = notificationCenter.instance;
    
    constructor() {
        super();
        
        this.initObservables();
    }

    private initObservables() {
        const filters = this.filters;

        filters.searchText.throttle(200).subscribe(() => this.filterDatabases());
        filters.requestedState.subscribe(() => this.filterDatabases());

        this.selectionState = ko.pureComputed<checkbox>(() => {
            const databases = this.databases().sortedDatabases().filter(x => !x.filteredOut());
            const selectedCount = this.selectedDatabases().length;
            if (databases.length && selectedCount === databases.length)
                return "checked";
            if (selectedCount > 0)
                return "some_checked";
            return "unchecked";
        });

        this.lockModeCommon = ko.pureComputed(() => {
            const selectedDatabases = this.getSelectedDatabases();
            if (selectedDatabases.length === 0)
                return "None";

            const firstLockMode = selectedDatabases[0].lockMode();
            for (let i = 1; i < selectedDatabases.length; i++) {
                if (selectedDatabases[i].lockMode() !== firstLockMode) {
                    return "Mixed";
                }
            }
            return firstLockMode;
        });
        
        this.databasesByState = ko.pureComputed(() => {
            const databases = this.databases().sortedDatabases();
            
            const result: Record<databaseState, number> = {
                errored: 0,
                disabled: 0,
                offline: 0,
                online: 0,
                remote: 0
            };

            for (const database of databases) {
                if (database.hasLoadError()) {
                    result.errored++;
                //TODO } else if (!this.isLocalDatabase(database.name)) {
                //TODO:     result.remote++;
                } else if (database.disabled()) {
                    result.disabled++;
                } else if (database.online()) {
                    result.online++;
                } else {
                    result.offline++;
                }
            }
            
            return result;
        });
        
        this.selectedDatabasesWithoutLock = ko.pureComputed(() => {
            const selected = this.getSelectedDatabases();
            return selected.filter(x => x.lockMode() === "Unlock");
        });
    }

    // Override canActivate: we can always load this page, regardless of any system db prompt.
    canActivate(): any {
        return true;
    }

    activate(args: any): JQueryPromise<Raven.Client.ServerWide.Operations.DatabasesInfo> {
        super.activate(args);

        // When coming here from Storage Report View
        if (args && args.compact) {
            this.databaseToCompact = args.compact;
        }

        // When coming here from Backups View, user wants to restore a database
        if (args && args.restore) {
            this.popupRestoreDialog = true;
        }
        
        // we can't use createNotifications here, as it is called after *database changes API* is connected, but user
        // can enter this view and never select database

        this.addNotification(this.changesContext.serverNotifications().watchAllDatabaseChanges((e: Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged) => this.onDatabaseChange(e)));
        this.addNotification(this.changesContext.serverNotifications().watchReconnect(() => this.fetchDatabases()));
        
        this.registerDisposable(this.changesContext.databaseNotifications.subscribe((dbNotifications) => this.onDatabaseChanged(dbNotifications)));
        
        return this.fetchDatabases();
    }
    
    private onDatabaseChanged(dbChanges: databaseNotificationCenterClient) {
        if (dbChanges) {

            const database = dbChanges.getDatabase();

            const throttledUpdate = _.throttle(() => {
                this.updateDatabaseInfo(database.name);
            }, 10000);
            
            this.statsSubscription = dbChanges.watchAllDatabaseStatsChanged(stats => {
                const matchedDatabase = this.databases().sortedDatabases().find(x => x.name === database.name);
                if (matchedDatabase) {
                    matchedDatabase.documentsCount(stats.CountOfDocuments);
                    matchedDatabase.indexesCount(stats.CountOfIndexes);
                }
                
                // schedule update of other properties
                throttledUpdate();
            });
        } else {
            if (this.statsSubscription) {
                this.statsSubscription.off();
                this.statsSubscription = null;
            }
        }
    }

    attached() {
        super.attached();
        this.updateHelpLink("Z8DC3Q");
        this.updateUrl(appUrl.forDatabases());
    }
    
    compositionComplete() {
        super.compositionComplete();
        
        this.initTooltips();
        this.setupDisableReasons();
       
        if (this.databaseToCompact) {
            const dbInfo = this.databases().getByName(this.databaseToCompact);
            this.compactDatabase(dbInfo);
        }
        
        if (this.popupRestoreDialog) {
            this.newDatabaseFromBackup();
            this.popupRestoreDialog = false;
        }
    }
    
    deactivate() {
        super.deactivate();
        
        if (this.statsSubscription) {
            this.statsSubscription.off();
            this.statsSubscription = null;
        }
        
        // make we all propovers are hidden
        $('[data-toggle="more-nodes-tooltip"]').popover('hide');
    }

    private fetchDatabases(): JQueryPromise<Raven.Client.ServerWide.Operations.DatabasesInfo> {
        return new getDatabasesCommand()
            .execute()
            //TODO: .done(info => this.databases(new databasesInfo(info)));
    }

    private onDatabaseChange(e: Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged) {
        switch (e.ChangeType) {
            case "Load":
            case "Put":
            case "Update":
                this.updateDatabaseInfo(e.DatabaseName);
                break;

            case "RemoveNode":
            case "Delete":
                // since we don't know if database was removed from current node, let's fetch databaseInfo first
                this.updateDatabaseInfo(e.DatabaseName)
                    .fail((xhr: JQueryXHR) => {
                        if (xhr.status === 404) {
                            // database was removed from all nodes

                            const db = this.databases().sortedDatabases().find(rs => rs.name === e.DatabaseName);
                            if (db) {
                                this.removeDatabase(db);
                            }
                        }
                    });
                break;
        }
    }

    updateDatabaseInfo(databaseName: string) {
        return new getDatabaseCommand(databaseName)
            .execute()
            .done((result: Raven.Client.ServerWide.Operations.DatabaseInfo) => {
                this.databases().updateDatabase(result);
                this.filterDatabases();
                this.initTooltips();
                this.setupDisableReasons();
            });
    }
    

    private static getLocalStorageKeyForDbNameWidth() {
        return storageKeyProvider.storageKeyFor("databaseNameWidth");
    }
    
    private loadDatabaseNamesSize() {
        return localStorage.getObject(databases.getLocalStorageKeyForDbNameWidth()) || 350;
    }
    
    private saveDatabaseNamesSize(value: number) {
        localStorage.setObject(databases.getLocalStorageKeyForDbNameWidth(), value);
    }
    
    private initTooltips() {
        // eslint-disable-next-line @typescript-eslint/no-this-alias
        /* TODO
        const self = this;

        const contentProvider = (dbInfo: databaseInfo) => {
            const nodesPart = dbInfo.nodes().map(node => {
                return `
                <a href="${this.createAllDocumentsUrlObservableForNode(dbInfo, node)()}" 
                    target="${node.tag() === this.clusterManager.localNodeTag() ? "" : "_blank"}" 
                    class="margin-left margin-right ${dbInfo.isBeingDeleted() ? "link-disabled" : ''}" 
                    title="${node.type()}">
                        <i class="${node.cssIcon()}"></i> <span>Node ${node.tag()}</span>
                    </a>
                `
            }).join(" ");
            
            return `<div class="more-nodes-tooltip">
                <div>
                    <i class="icon-dbgroup"></i>
                    <span>
                        Database Group for ${dbInfo.name}
                    </span>
                </div>
                <hr />
                <div class="flex-horizontal flex-wrap">
                    ${nodesPart}    
                </div>
            </div>`;
        };
        
        $('.databases [data-toggle="more-nodes-tooltip"]').each((idx, element) => {
            popoverUtils.longWithHover($(element), {
                content: () => { 
                    const context = ko.dataFor(element);
                    return contentProvider(context);
                },
                placement: "top",
                container: "body"
            });
        });

        $('.databases [data-toggle="size-tooltip"]').tooltip({
            container: "body",
            html: true,
            placement: "right",
            title: function () {
                const $data = ko.dataFor(this) as databaseInfo;
                return `<div class="text-left padding padding-sm">
                    Data: <strong>${self.formatBytes($data.totalSize())}</strong><br />
                Temp: <strong>${self.formatBytes($data.totalTempBuffersSize())}</strong><br />
                    Total: <strong>${self.formatBytes($data.totalSize() + $data.totalTempBuffersSize())}</strong>
                </div>`
            }
        });
        
         *
    }
    */

    private filterDatabases(): void {
        /* TODO
        const filters = this.filters;
        let searchText = filters.searchText();
        const hasSearchText = !!searchText;

        if (hasSearchText) {
            searchText = searchText.toLowerCase();
        }

        const matchesFilters = (db: databaseInfo): boolean => {
            const state = filters.requestedState();
            const nodeTag = this.clusterManager.localNodeTag();
            
            const matchesOnline = state === 'online' && db.online();
            const matchesDisabled = state === 'disabled' && db.disabled();
            const matchesErrored = state === 'errored' && db.hasLoadError();
            const matchesOffline = state === 'offline' && (!db.online() && !db.disabled() && !db.hasLoadError() && db.isLocal(nodeTag));
            
            const matchesLocal = state === 'local' && db.isLocal(nodeTag);
            const matchesRemote = state === 'remote' && !db.isLocal(nodeTag);
            const matchesAll = state === 'all';
            
            const matchesText = !hasSearchText || db.name.toLowerCase().indexOf(searchText) >= 0;
            
            return matchesText &&
                (matchesOnline || matchesDisabled || matchesErrored || matchesOffline || matchesLocal || matchesRemote || matchesAll);
        };

        const databases = this.databases();
        databases.sortedDatabases().forEach(db => {
            const matches = matchesFilters(db);
            db.filteredOut(!matches);

            if (!matches) {
                this.selectedDatabases.remove(db.name);
            }
        });
         */
    }

    /*
    createManageDbGroupUrlObsevable(dbInfo: databaseInfo): KnockoutComputed<string> {
        return ko.pureComputed(() => {
            const isLocal = true; //TODO: this.isLocalDatabase(dbInfo.name);
            const link = appUrl.forManageDatabaseGroup(dbInfo);
            if (isLocal) {
                return link;
            } else {
                //TODO: return databases.toExternalUrl(dbInfo, link);
            }
        });
    }

    createAllDocumentsUrlObservable(dbInfo: databaseInfo): KnockoutComputed<string> {
        return ko.pureComputed(() => {
            const isLocal = true; //TOD: this.isLocalDatabase(dbInfo.name);
            const link = appUrl.forDocuments(null, dbInfo);
            if (isLocal) {
                return link;
            } else {
                //TODO: return databases.toExternalUrl(dbInfo, link);
            }
        });
    }*/

    
    /*
    createAllDocumentsUrlObservableForNode(dbInfo: databaseInfo, node: databaseGroupNode) {
        return ko.pureComputed(() => {
            const currentNodeTag = this.clusterManager.localNodeTag();
            const nodeTag = node.tag();
            const link = appUrl.forDocuments(null, dbInfo);
            if (currentNodeTag === nodeTag) {
                return link;
            } else {
                return appUrl.toExternalUrl(node.serverUrl(), link);
            }
        });
    }*/
    

    /*
    indexErrorsUrl(dbInfo: databaseInfo): string {
        return appUrl.forIndexErrors(dbInfo);
    }

    storageReportUrl(dbInfo: databaseInfo): string {
        return appUrl.forStatusStorageReport(dbInfo);
    }

    indexesUrl(dbInfo: databaseInfo): string {
        return appUrl.forIndexes(dbInfo);
    } 

    backupsViewUrl(dbInfo: databaseInfo): string {
        return appUrl.forBackups(dbInfo);
    }

    periodicBackupUrl(dbInfo: databaseInfo): string {
        return appUrl.forEditPeriodicBackupTask(dbInfo);
    }

    manageDatabaseGroupUrl(dbInfo: databaseInfo): string {
        return appUrl.forManageDatabaseGroup(dbInfo);
    }

    private getSelectedDatabases() {
        const selected = this.selectedDatabases();
        return this.databases().sortedDatabases().filter(x => _.includes(selected, x.name));
    }
    

    /*
    deleteDatabase(db: databaseInfo) {
        this.deleteDatabases([db]);
    }

    deleteSelectedDatabases() {
        const withoutLock = this.selectedDatabasesWithoutLock();
        if (withoutLock.length) {
            this.deleteDatabases(withoutLock);
        }
    }

    private removeDatabase(dbInfo: databaseInfo) {
        this.databases().sortedDatabases.remove(dbInfo);
        this.selectedDatabases.remove(dbInfo.name);
        messagePublisher.reportSuccess(`Database ${dbInfo.name} was successfully deleted`);
    }

    enableSelectedDatabases() {
        this.toggleSelectedDatabases(true);
    }

    disableSelectedDatabases() {
        this.toggleSelectedDatabases(false);
    }

    private toggleSelectedDatabases(enableAll: boolean) {
        const selectedDatabases = this.getSelectedDatabases().map(x => x.asDatabase());

        if (_.every(selectedDatabases, x => x.disabled() !== enableAll)) {
            return;
        }

        if (selectedDatabases.length > 0) {
            const disableDatabaseToggleViewModel = new disableDatabaseToggleConfirm(selectedDatabases, !enableAll);

            disableDatabaseToggleViewModel.result.done(result => {
                if (result.can) {
                    this.spinners.globalToggleDisable(true);
  
                    new toggleDatabaseCommand(selectedDatabases, !enableAll)
                        .execute()
                        .done(disableResult => {
                            disableResult.Status.forEach(x => this.onDatabaseDisabled(x));
                        })
                        .always(() => this.spinners.globalToggleDisable(false));
                }
            });

            app.showBootstrapDialog(disableDatabaseToggleViewModel);
        }
    }

    toggleDatabase(rsInfo: databaseInfo) {
        const disable = !rsInfo.disabled();

        const rs = rsInfo.asDatabase();
        const disableDatabaseToggleViewModel = new disableDatabaseToggleConfirm([rs], disable);

        disableDatabaseToggleViewModel.result.done(result => {
            if (result.can) {
                rsInfo.inProgressAction(disable ? "Disabling..." : "Enabling...");

                new toggleDatabaseCommand([rs], disable)
                    .execute()
                    .done(disableResult => {
                        disableResult.Status.forEach(x => this.onDatabaseDisabled(x));
                    })
                    .always(() => rsInfo.inProgressAction(null));
            }
        });

        app.showBootstrapDialog(disableDatabaseToggleViewModel);
    }

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

    toggleDisableDatabaseIndexing(db: databaseInfo) {
        const enableIndexing = db.indexingDisabled();
        const message = enableIndexing ? "Enable" : "Disable";

        eventsCollector.default.reportEvent("databases", "toggle-indexing");

        this.confirmationMessage("Are you sure?", message + " indexing?")
            .done(result => {
                if (result.can) {
                    db.inProgressAction(enableIndexing ? "Enabling..." : "Disabling...");

                    new toggleDisableIndexingCommand(enableIndexing, { name: db.name })
                        .execute()
                        .done(() => {
                            db.indexingDisabled(!enableIndexing);
                            db.indexingPaused(false);
                        })
                        .always(() => db.inProgressAction(null));
                }
            });
    }
    
    compactDatabase(db: databaseInfo) {
        eventsCollector.default.reportEvent("databases", "compact");
        this.changesContext.disconnectIfCurrent(db.asDatabase(), "DatabaseDisabled");
        app.showBootstrapDialog(new compactDatabaseDialog(db));
    }

    togglePauseDatabaseIndexing(db: databaseInfo) {
        eventsCollector.default.reportEvent("databases", "pause-indexing");
        
        const pauseIndexing = db.indexingPaused();
        const message = pauseIndexing ? "Resume" : "Pause";

        this.confirmationMessage("Are you sure?", message + " indexing?")
            .done(result => {
                if (result.can) {
                    db.inProgressAction(pauseIndexing ? "Resuming..." : "Pausing...");

                    new togglePauseIndexingCommand(pauseIndexing, db.asDatabase())
                        .execute()
                        .done(() => db.indexingPaused(!pauseIndexing))
                        .always(() => db.inProgressAction(null));
                }
            });
    }

    
    newDatabase() {
        const createDbView = new createDatabase("newDatabase");
        app.showBootstrapDialog(createDbView)
            .done(shardsDefined => {
                if (shardsDefined) {
                    // For now, refresh data for this view
                    // Can't rely on ws - see issue https://issues.hibernatingrhinos.com/issue/RavenDB-16177 // TODO
                    this.fetchDatabases();
                    this.databasesManager.refreshDatabases();
                }
            });
    }
    
    newDatabaseFromBackup() {
        eventsCollector.default.reportEvent("databases", "new-from-backup");
        
        const createDbView = new createDatabase("restore");
        app.showBootstrapDialog(createDbView);
    }
    
    newDatabaseFromLegacyDatafiles() {
        eventsCollector.default.reportEvent("databases", "new-from-legacy");
        
        const createDbView = new createDatabase("legacyMigration");
        app.showBootstrapDialog(createDbView);
    }

    databasePanelClicked(dbInfo: databaseInfo, event: JQueryEventObject) {
        if (generalUtils.canConsumeDelegatedEvent(event)) {
            this.activateDatabase(dbInfo);
            return false;
        }
        
        return true;
    }
    
    activateDatabase(dbInfo: databaseInfo) {
        const db = this.databasesManager.getDatabaseByName(dbInfo.name);
        if (!db || db.disabled() || !db.relevant())
            return true;

        this.databasesManager.activate(db);

        this.updateDatabaseInfo(db.name);
        
        return true; // don't prevent default action as we have links inside links
    }
    
    
    openNotificationCenter(dbInfo: databaseInfo) {
        if (!this.activeDatabase() || this.activeDatabase().name !== dbInfo.name) {
            this.activateDatabase(dbInfo);
        }

        this.notificationCenter.showNotifications.toggle();
    }
    

    unlockSelectedDatabases() {
        this.setLockModeSelectedDatabases("Unlock", "allow deletes");
    }

    lockSelectedDatabases() {
        this.setLockModeSelectedDatabases("PreventDeletesIgnore", "prevent deletes");
    }

    lockErrorSelectedDatabases() {
        this.setLockModeSelectedDatabases("PreventDeletesError", "prevent deletes (with error)");
    }

    private setLockModeSelectedDatabases(lockMode: Raven.Client.ServerWide.DatabaseLockMode, lockModeStrForTitle: string) {
        if (this.lockModeCommon() === lockMode)
            return;

        this.confirmationMessage("Are you sure?", `Do you want to <strong>${generalUtils.escapeHtml(lockModeStrForTitle)}</strong> of selected databases?`, {
            html: true
        })
            .done(can => {
                if (can) {
                    eventsCollector.default.reportEvent("databases", "set-lock-mode-selected", lockMode);
                    
                    const databases = this.getSelectedDatabases();

                    if (databases.length) {
                        this.spinners.globalToggleDisable(true);

                        new saveDatabaseLockModeCommand(databases.map(x => x.asDatabase()), lockMode)
                            .execute()
                            .done(() => databases.forEach(i => i.lockMode(lockMode)))
                            .always(() => this.spinners.globalToggleDisable(false));
                    }
                }
            });
    }

    isAdminAccessByDbName(dbName: string) {
        return accessManager.default.isAdminByDbName(dbName);
    }
    
     */
}

export = databases;


/* TODO

class databasesInfo {

    sortedDatabases = ko.observableArray<databaseInfo>();

    databasesCount: KnockoutComputed<number>;

    constructor(dto: Raven.Client.ServerWide.Operations.DatabasesInfo) {

        const databases = dto.Databases.map(db => new databaseInfo(db));

        const dbs = [...databases];
        dbs.sort((a, b) => generalUtils.sortAlphaNumeric(a.name, b.name));

        this.sortedDatabases(dbs);

        this.initObservables();
    }

    getByName(name: string) {
        return this.sortedDatabases().find(x => x.name.toLowerCase() === name.toLowerCase());
    }

    updateDatabase(newDatabaseInfo: Raven.Client.ServerWide.Operations.DatabaseInfo) {
        const databaseToUpdate = this.getByName(newDatabaseInfo.Name);

        if (databaseToUpdate) {
            databaseToUpdate.update(newDatabaseInfo);
        } else { // new database - create instance of it
            const dto = newDatabaseInfo as Raven.Client.ServerWide.Operations.DatabaseInfo;
            const databaseToAdd = new databaseInfo(dto);
            this.sortedDatabases.push(databaseToAdd);
            this.sortedDatabases.sort((a, b) => generalUtils.sortAlphaNumeric(a.name, b.name));
        }
    }

    private initObservables() {
        this.databasesCount = ko.pureComputed(() => this
            .sortedDatabases()
            .filter(r => r instanceof databaseInfo)
            .length);
    }
}


//TODO: consider removing 
class databaseInfo {

    private static dayAsSeconds = 60 * 60 * 24;

    name: string;

    uptime = ko.observable<string>();
    totalSize = ko.observable<number>();
    totalTempBuffersSize = ko.observable<number>();
    bundles = ko.observableArray<string>();
    backupStatus = ko.observable<string>();
    lastBackupText = ko.observable<string>();
    lastFullOrIncrementalBackup = ko.observable<string>();
    dynamicDatabaseDistribution = ko.observable<boolean>();

    loadError = ko.observable<string>();

    isEncrypted = ko.observable<boolean>();
    isAdmin = ko.observable<boolean>();
    disabled = ko.observable<boolean>();
    lockMode = ko.observable<Raven.Client.ServerWide.DatabaseLockMode>();

    filteredOut = ko.observable<boolean>(false);
    isBeingDeleted = ko.observable<boolean>(false);

    indexingErrors = ko.observable<number>();
    alerts = ko.observable<number>();
    performanceHints = ko.observable<number>();

    environment = ko.observable<Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment>();
    
    online: KnockoutComputed<boolean>;
    isLoading: KnockoutComputed<boolean>;
    hasLoadError: KnockoutComputed<boolean>;
    canNavigateToDatabase: KnockoutComputed<boolean>;
    isCurrentlyActiveDatabase: KnockoutComputed<boolean>;
    
    databaseAccessText = ko.observable<string>();
    databaseAccessColor = ko.observable<string>();
    databaseAccessClass = ko.observable<string>();

    inProgressAction = ko.observable<string>();

    rejectClients = ko.observable<boolean>();
    indexingStatus = ko.observable<Raven.Client.Documents.Indexes.IndexRunningStatus>();
    indexingDisabled = ko.observable<boolean>();
    indexingPaused = ko.observable<boolean>();
    documentsCount = ko.observable<number>();
    indexesCount = ko.observable<number>();

    deletionInProgress = ko.observableArray<string>([]);

    constructor(dto: Raven.Client.ServerWide.Operations.DatabaseInfo) {
        this.initializeObservables();

        this.update(dto);
    }

    get qualifier() {
        return "db";
    }

    get fullTypeName() {
        return "database";
    }

    asDatabase(): database {
        const casted = databasesManager.default.getDatabaseByName(this.name);
        if (!casted) {
            throw new Error("Unable to find database: " + this.name + " in database manager");
        }
        return casted;
    }

    static extractQualifierAndNameFromNotification(input: string): { qualifier: string, name: string } {
        return { qualifier: input.substr(0, 2), name: input.substr(3) };
    }

    private computeBackupStatus(backupInfo: Raven.Client.ServerWide.Operations.BackupInfo) {
        if (!backupInfo || !backupInfo.LastBackup) {
            this.lastBackupText("Never backed up");
            return "text-danger";
        }

        const dateInUtc = moment.utc(backupInfo.LastBackup);
        const diff = moment().utc().diff(dateInUtc);
        const durationInSeconds = moment.duration(diff).asSeconds();

        this.lastBackupText(`Backed up ${this.lastFullOrIncrementalBackup()}`);
        return durationInSeconds > databaseInfo.dayAsSeconds ? "text-warning" : "text-success";
    }
    
    private initializeObservables() {
        this.hasLoadError = ko.pureComputed(() => !!this.loadError());

        this.online = ko.pureComputed(() => {
            return !!this.uptime();
        });

        this.canNavigateToDatabase = ko.pureComputed(() => {
            const enabled = !this.disabled();
            const hasLoadError = this.hasLoadError();
            return enabled && !hasLoadError;
        });

        this.isCurrentlyActiveDatabase = ko.pureComputed(() => {
            const currentDatabase = activeDatabaseTracker.default.database();

            if (!currentDatabase) {
                return false;
            }

            return currentDatabase.name === this.name;
        });

        this.isLoading = ko.pureComputed(() => {
            return this.isCurrentlyActiveDatabase() &&
                !this.online() &&
                !this.disabled();
        });
    }

    update(dto: Raven.Client.ServerWide.Operations.DatabaseInfo): void {
        this.name = dto.Name;
        this.lockMode(dto.LockMode);
        this.disabled(dto.Disabled);
        this.isAdmin(dto.IsAdmin);
        this.isEncrypted(dto.IsEncrypted);
        this.totalSize(dto.TotalSize ? dto.TotalSize.SizeInBytes : 0);
        this.totalTempBuffersSize(dto.TempBuffersSize ? dto.TempBuffersSize.SizeInBytes : 0);
        this.indexingErrors(dto.IndexingErrors);
        this.alerts(dto.Alerts);
        this.performanceHints(dto.PerformanceHints);
        this.loadError(dto.LoadError);
        this.uptime(generalUtils.timeSpanAsAgo(dto.UpTime, false));
        this.dynamicDatabaseDistribution(dto.DynamicNodesDistribution);
        
        this.environment(dto.Environment);

        if (dto.BackupInfo && dto.BackupInfo.LastBackup) {
            this.lastFullOrIncrementalBackup(moment.utc(dto.BackupInfo.LastBackup).local().fromNow());
        }
            
        this.backupStatus(this.computeBackupStatus(dto.BackupInfo));

        this.rejectClients(dto.RejectClients);
        this.indexingStatus(dto.IndexingStatus);
        this.indexingDisabled(dto.IndexingStatus === "Disabled");
        this.indexingPaused(dto.IndexingStatus === "Paused");
        this.documentsCount(dto.DocumentsCount);
        this.indexesCount(dto.IndexesCount);
        this.deletionInProgress(dto.DeletionInProgress ? Object.keys(dto.DeletionInProgress) : []);
        this.databaseAccessText(accessManager.default.getDatabaseAccessLevelTextByDbName(this.name));
        this.databaseAccessColor(accessManager.default.getAccessColorByDbName(this.name));
        this.databaseAccessClass(accessManager.default.getAccessIconByDbName(this.name))
    }
}

export = databaseInfo;

 */