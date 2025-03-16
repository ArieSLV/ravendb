using System;
using System.Collections.Concurrent;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client.ServerWide;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.AI.Embeddings;

#pragma warning disable SKEXP0001

namespace Raven.Server.Documents.AI;

public class AiIntegrationsController : IDisposable
{
    private readonly ConcurrentDictionary<AiConnectionStringIdentifier, ITextEmbeddingGenerationService> _embeddingsGenerationServiceByConnectionStringIdentifier;

    private readonly ConcurrentDictionary<EmbeddingsGenerationTaskIdentifier, AiConnectionStringIdentifier> _connectionStringsByTaskIdentifiers;
    private readonly ConcurrentDictionary<EmbeddingsGenerationTaskIdentifier, EmbeddingsGenerationConfiguration> _embeddingsGenerationConfigurationByTaskIdentifiers;

    private readonly object _atomicDatabaseRecordChangeLock = new();
    
    public DocumentDatabase Database { get; }

    public AiIntegrationsController(DocumentDatabase database)
    {
        Database = database;
        _embeddingsGenerationServiceByConnectionStringIdentifier = new();
        _embeddingsGenerationConfigurationByTaskIdentifiers = new();
        _connectionStringsByTaskIdentifiers = new();

        var storage = new EmbeddingsStorage(database);
        var cacher = new QueryEmbeddingsCacher(database, database.DatabaseShutdown);

        Embeddings = new EmbeddingsController(this, storage, cacher);
    }

    public EmbeddingsController Embeddings { get; private set; }

    public bool TryGetEmbeddingsGenerationConfiguration(EmbeddingsGenerationTaskIdentifier taskIdentifier, out EmbeddingsGenerationConfiguration configuration)
    {
        return _embeddingsGenerationConfigurationByTaskIdentifiers.TryGetValue(taskIdentifier, out configuration);
    }

    public bool TryGetConnectionStringIdByEmbeddingsGenerationTask(EmbeddingsGenerationTaskIdentifier taskIdentifier, out AiConnectionStringIdentifier connectionString)
    {
        return _connectionStringsByTaskIdentifiers.TryGetValue(taskIdentifier, out connectionString);
    }

    public void HandleDatabaseRecordChange(DatabaseRecord record)
    {
        if (record == null)
            return;

        lock (_atomicDatabaseRecordChangeLock)
        {
            var embeddingsGenerationTaskIdsToRetain = new HashSet<EmbeddingsGenerationTaskIdentifier>();

            // Updating existing configurations, connection strings and workers for active Embeddings Generation tasks
            foreach (var newEmbeddingGenerationConfiguration in record.EmbeddingsGenerations.Where(configuration => configuration.Disabled == false))
            {
                var taskId = new EmbeddingsGenerationTaskIdentifier(newEmbeddingGenerationConfiguration.Identifier);
                embeddingsGenerationTaskIdsToRetain.Add(taskId);

                var isConnectionStringChanged = false;
                if (_embeddingsGenerationConfigurationByTaskIdentifiers.TryGetValue(taskId, out var oldEmbeddingGenerationConfiguration))
                    isConnectionStringChanged = oldEmbeddingGenerationConfiguration.Connection.Compare(newEmbeddingGenerationConfiguration.Connection) != AiSettingsCompareDifferences.None;

                _embeddingsGenerationConfigurationByTaskIdentifiers.AddOrUpdate(taskId, newEmbeddingGenerationConfiguration, (_, _) => newEmbeddingGenerationConfiguration);

                if (record.AiConnectionStrings?.TryGetValue(newEmbeddingGenerationConfiguration.ConnectionStringName, out var connectionString) != true)
                    continue;

                var connectionStringIdentifier = new AiConnectionStringIdentifier(connectionString.Identifier);
                _connectionStringsByTaskIdentifiers.AddOrUpdate(taskId, connectionStringIdentifier, (_, _) => connectionStringIdentifier);

                if (isConnectionStringChanged == false)
                    continue;

                var newService = AiHelper.CreateService(connectionString);
                _embeddingsGenerationServiceByConnectionStringIdentifier.AddOrUpdate(connectionStringIdentifier, newService, (_, _) => newService);
                Embeddings.UpdateBatchingWorkerForConnectionStringId(connectionString);
            }

            // Removing configurations for inactive Embeddings Generation tasks
            foreach ((EmbeddingsGenerationTaskIdentifier taskId, _) in _embeddingsGenerationConfigurationByTaskIdentifiers)
            {
                if (embeddingsGenerationTaskIdsToRetain.Contains(taskId))
                    continue;

                _embeddingsGenerationConfigurationByTaskIdentifiers.TryRemove(taskId, out _);
            }

            // Removing connection strings and workers for inactive Embeddings Generation tasks
            foreach ((EmbeddingsGenerationTaskIdentifier taskId, _) in _connectionStringsByTaskIdentifiers)
            {
                if (embeddingsGenerationTaskIdsToRetain.Contains(taskId))
                    continue;

                if (_connectionStringsByTaskIdentifiers.TryRemove(taskId, out var connectionStringIdToRemove) == false)
                    continue;

                _embeddingsGenerationServiceByConnectionStringIdentifier.TryRemove(connectionStringIdToRemove, out _);
                Embeddings.RemoveBatchingWorkerForConnectionStringId(connectionStringIdToRemove);
            }

            // Switching the QueryEmbeddingsCacher on or off based on the existence of active Embeddings Generation tasks
            if (Embeddings.QueryEmbeddingsCacher.IsRunning)
            {
                if (embeddingsGenerationTaskIdsToRetain.Count == 0)
                    Embeddings.QueryEmbeddingsCacher.Stop();
            }
            else
            {
                if (embeddingsGenerationTaskIdsToRetain.Count > 0)
                    Embeddings.QueryEmbeddingsCacher.Start();
            }
        }
    }

    public void Dispose()
    {
        Embeddings.QueryEmbeddingsCacher.Dispose();
    }

    public bool TryGetServiceByConnectionString(AiConnectionStringIdentifier connectionStringIdentifier, out ITextEmbeddingGenerationService service)
    {
        return _embeddingsGenerationServiceByConnectionStringIdentifier.TryGetValue(connectionStringIdentifier, out service);
    }

    public void Initialize(DatabaseRecord record)
    {
        HandleDatabaseRecordChange(record);
    }
}
