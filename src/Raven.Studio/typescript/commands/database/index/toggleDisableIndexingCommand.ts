﻿import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class toggleDisableIndexingCommand extends commandBase {

    private readonly start: boolean;

    private readonly databaseName: string;

    constructor(start: boolean, databaseName: string) {
        super();
        this.databaseName = databaseName;
        this.start = start;
    }

    execute(): JQueryPromise<void> {
        const args = {
            enable: this.start
        }

        const url = endpoints.global.adminDatabases.adminDatabasesIndexing + this.urlEncodeArgs(args);

        const payload: Raven.Client.ServerWide.Operations.ToggleDatabasesStateOperation.Parameters = {
            DatabaseNames: [this.databaseName]
        };

        //TODO: report messages!
        return this.post(url, JSON.stringify(payload));
    }
}

export = toggleDisableIndexingCommand;
