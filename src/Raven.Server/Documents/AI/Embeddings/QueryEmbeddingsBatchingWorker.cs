using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.SemanticKernel;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.AI.Embeddings
{
    public class QueryEmbeddingsBatchingWorker : IDisposable
    {
        private readonly string _databaseName;
        private readonly AiConfiguration _configuration;
#pragma warning disable SKEXP0001
        private readonly (AiConnectionString ConnectionString, ITextEmbeddingGenerationService Instance) _service;
#pragma warning restore SKEXP0001
        private readonly SemaphoreSlim _concurrencyLimiter;
        private readonly RavenLogger _logger;
        private readonly CancellationToken _shutdown;

        private readonly ConcurrentQueue<QueryEmbeddingsBatchRequest> _requestQueue = new();
        private readonly AutoResetEvent _workAvailableSignal = new(false);

        private readonly Task[] _workerTasks;

        // Flag that indicates the service is being disposed
        private volatile bool _workerShuttingDown;
        private int _activeOperations;

        public QueryEmbeddingsBatchingWorker(string databaseName,
            AiConfiguration configuration,
#pragma warning disable SKEXP0001
            (AiConnectionString ConnectionString, ITextEmbeddingGenerationService Instance) service,
#pragma warning restore SKEXP0001
            SemaphoreSlim concurrencyLimiter,
            RavenLogger logger,
            CancellationToken shutdown)
        {
            _databaseName = databaseName;
            _configuration = configuration;
            _service = service;
            _concurrencyLimiter = concurrencyLimiter;
            _logger = logger;
            _shutdown = shutdown;

            // Initialize worker tasks
            int workerCount = Math.Max(1, configuration.QueryEmbeddingsMaxConcurrentBatches);
            _workerTasks = new Task[workerCount];

            if (logger.IsInfoEnabled)
                logger.Info($"Initializing {nameof(QueryEmbeddingsBatchingWorker)} for connection '{service.ConnectionString.Name}' in database '{databaseName}' with {workerCount} workers");
        }

        public void Start()
        {
            for (int i = 0; i < _workerTasks.Length; i++)
                _workerTasks[i] = Task.Run(WorkerLoopAsync, _shutdown);
        }

        public Task<ReadOnlyMemory<float>[]> EnqueueRequestAsync(IList<string> values, CancellationToken cancellationToken)
        {
            var request = new QueryEmbeddingsBatchRequest(values, cancellationToken);

            if (_workerShuttingDown)
                return request.CancelWithShutdownMessage();

            _requestQueue.Enqueue(request);

            // Signal workers that new work is available
            _workAvailableSignal.Set();

            return request.TaskCompletionSource.Task;
        }

        private async Task WorkerLoopAsync()
        {
            while (_workerShuttingDown == false && _shutdown.IsCancellationRequested == false)
            {
                try
                {
                    WaitHandle.WaitAny([_workAvailableSignal, _shutdown.WaitHandle]);

                    if (_shutdown.IsCancellationRequested || _workerShuttingDown)
                        break;

                    if (_requestQueue.IsEmpty)
                        continue;

                    await ProcessBatchAsync();
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    if (_logger.IsErrorEnabled)
                        _logger.Error($"Error in query embeddings batching worker for connection string '{_service.ConnectionString.Name}' in database '{_databaseName}'", ex);

                    await Task.Delay(100, _shutdown); // Sleep briefly before continuing
                }
            }
        }

        private async Task ProcessBatchAsync()
        {
            Interlocked.Increment(ref _activeOperations);

            Stopwatch stopwatch = null;
            var concurrencySlotAcquired = false;
            QueryEmbeddingsBatchRequest[] requestsArray = null;
            int count = 0;

            try
            {
                if (_workerShuttingDown)
                    return;

                // Acquire concurrency slot
                await _concurrencyLimiter.WaitAsync(_shutdown);
                concurrencySlotAcquired = true;

                // Collect requests for this batch
                requestsArray = new QueryEmbeddingsBatchRequest[_configuration.QueryEmbeddingsMaxBatchSize];

                while (count < _configuration.QueryEmbeddingsMaxBatchSize &&
                       _requestQueue.TryDequeue(out var request)) // todo: take into account model's token limit
                {
                    if (request.TaskCompletionSource.Task.IsCanceled)
                        continue;

                    requestsArray[count++] = request;
                }

                if (count == 0)
                    return;

                // If there are more requests in the queue, signal another worker immediately
                if (_requestQueue.IsEmpty == false)
                    _workAvailableSignal.Set();

                // Recheck shutdown before expensive operation
                _shutdown.ThrowIfCancellationRequested();

                // Process the batch
                stopwatch = Stopwatch.StartNew();
                await FlushBatchAsync(requestsArray, count);
                ForTestingPurposes?.AfterBatchFlushed?.Invoke();
            }
            catch (OperationCanceledException)
            {
                CancelActiveRequestsWithShutdownMessage(requestsArray, count);
                throw;
            }
            catch (Exception ex)
            {
                PropagateExceptionToActiveRequests(requestsArray, count, ex);

                if (_logger.IsErrorEnabled)
                    _logger.Error($"Error in batch processing for connection string '{_service.ConnectionString.Name}' in database '{_databaseName}'", ex);
            }
            finally
            {
                if (stopwatch != null)
                {
                    stopwatch.Stop();
                    if (_logger.IsDebugEnabled && count > 0)
                        _logger.Debug($"Batch processing completed for connection '{_service.ConnectionString.Identifier}' in {stopwatch.ElapsedMilliseconds}ms, processed {count} requests");
                }

                // Release concurrency slot
                if (concurrencySlotAcquired)
                    _concurrencyLimiter.Release();

                // Decrement active operations counter
                Interlocked.Decrement(ref _activeOperations);
            }
        }

        private async Task FlushBatchAsync(QueryEmbeddingsBatchRequest[] requestsArray, int count)
        {
            var totalValueCount = 0;
            for (var i = 0; i < count; i++)
                totalValueCount += requestsArray[i].Values.Count;

            var allTextValues = new string[totalValueCount];

            // It's a tracking structure for the range of values for each request
            var valueRanges = new (int StartIndex, int Count)[count];

            // Fill the array with all values and remember the ranges
            var currentIndex = 0;
            for (int i = 0; i < count; i++)
            {
                var values = requestsArray[i].Values;
                valueRanges[i] = (currentIndex, values.Count);

                foreach (var value in values)
                    allTextValues[currentIndex++] = value;
            }

            if (_logger.IsDebugEnabled)
                _logger.Debug($"Processing batch of {totalValueCount} values from {count} requests for connection '{_service.ConnectionString.Name}'");

            // Final check before calling service
            _shutdown.ThrowIfCancellationRequested();

            // Generate embeddings
            IList<ReadOnlyMemory<float>> allEmbeddings = null;
            try
            {
#pragma warning disable SKEXP0001
                allEmbeddings = await AiHelper.GenerateEmbeddingsAsync(_service.Instance, allTextValues);
#pragma warning restore SKEXP0001
            }
            catch (HttpOperationException httpOperationException) when (httpOperationException.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
            {
                var rateLimitException = new EmbeddingGenerationException(
                    $"Failed to generate embeddings due to rate limits. Consider decreasing the number of elements processed in a single batch " +
                    $"('{RavenConfiguration.GetKey(x => x.Ai.QueryEmbeddingsMaxBatchSize)}') or increasing the " +
                    $"limits on your model deployment.", httpOperationException);

                PropagateExceptionToActiveRequestsAndRethrow(requestsArray, count, rateLimitException);
            }
            catch (Exception ex)
            {
                PropagateExceptionToActiveRequestsAndRethrow(requestsArray, count, ex);
            }

            // Verify we got the expected number of embeddings
            if (allEmbeddings.Count != totalValueCount)
            {
                var ex = new InvalidOperationException($"Failed to generate embeddings: expected {totalValueCount} embeddings, but got {allEmbeddings.Count}");
                PropagateExceptionToActiveRequestsAndRethrow(requestsArray, count, ex);
            }

            // Distribute results back to the requests - this needs to be done regardless of cancellation
            // If the request was canceled, we still need to create and return the result to cache the embeddings
            try
            {
                for (int i = 0; i < count; i++)
                {
                    var request = requestsArray[i];
                    (int startIndex, int itemsCount) = valueRanges[i];

                    var requestEmbeddings = new ReadOnlyMemory<float>[itemsCount];

                    for (int j = 0; j < itemsCount; j++)
                        requestEmbeddings[j] = allEmbeddings[startIndex + j];

                    // Return the list of embeddings to the caller - even if the request was canceled
                    // If request was already canceled, TrySetResult will silently fail, but we don't care
                    // because we just want to ensure embeddings are processed for caching
                    request.TaskCompletionSource.TrySetResult(requestEmbeddings);
                }
            }
            catch (Exception ex)
            {
                PropagateExceptionToActiveRequestsAndRethrow(requestsArray, count, ex);
            }
        }

        [DoesNotReturn]
        private static void PropagateExceptionToActiveRequestsAndRethrow(QueryEmbeddingsBatchRequest[] requestsArray, int count, Exception ex)
        {
            PropagateExceptionToActiveRequests(requestsArray, count, ex);
            throw ex;
        }

        public AiSettingsCompareDifferences Compare(AiConnectionString connectionString) =>
            _service.ConnectionString.Compare(connectionString);

        private static void PropagateExceptionToActiveRequests(QueryEmbeddingsBatchRequest[] requests, int count, Exception ex)
        {
            if (requests == null || count < 1)
                return;

            for (int i = 0; i < count; i++)
            {
                var request = requests[i];
                if (request.WasCanceled == false && request.TaskCompletionSource.Task.IsCompleted == false)
                    request.TaskCompletionSource.TrySetException(ex);
            }
        }

        private static void CancelActiveRequestsWithShutdownMessage(QueryEmbeddingsBatchRequest[] requests, int count)
        {
            if (requests == null || count < 1)
                return;

            for (int i = 0; i < count; i++)
                if (requests[i].WasCanceled == false)
                    requests[i].CancelWithShutdownMessage();
        }

        public async Task PrepareForServiceDisposalAsync()
        {
            _workerShuttingDown = true;

            // Cancel all queued items
            while (_requestQueue.TryDequeue(out var request))
                await request.CancelWithShutdownMessage();

            // Signal any waiting workers to check the shutdown flag
            _workAvailableSignal.Set();

            var timeout = TimeSpan.FromSeconds(10); // Timeout for waiting operations to complete
            var deadline = DateTime.UtcNow.Add(timeout);

            // Wait for any ongoing operations to complete, with timeout
            while (Interlocked.CompareExchange(ref _activeOperations, 0, 0) > 0)
            {
                // Check if timeout has elapsed
                if (DateTime.UtcNow > deadline)
                {
                    if (_logger.IsWarnEnabled)
                        _logger.Warn($"Timed out waiting for {_activeOperations} operations to complete during worker disposal.");

                    break;
                }

                await Task.Delay(100, _shutdown);
            }

            // Wait for worker tasks to complete with timeout
            try
            {
                await Task.WhenAny(
                    Task.WhenAll(_workerTasks.Where(t => t != null)),
                    Task.Delay(timeout)
                );
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                if (_logger.IsErrorEnabled)
                    _logger.Error("Error waiting for worker tasks to complete", ex);
            }
        }

        public void Dispose()
        {
            // Ensure we're shutting down
            _workerShuttingDown = true;

            // Signal to wake up any waiting workers
            _workAvailableSignal.Set();

            // Dispose the signal
            _workAvailableSignal.Dispose();
        }

        internal TestingStuff ForTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (ForTestingPurposes != null)
                return ForTestingPurposes;

            return ForTestingPurposes = new TestingStuff();
        }

        internal sealed class TestingStuff
        {
            internal Action AfterBatchFlushed;
        }
    }
}
