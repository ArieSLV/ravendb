using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Server.Documents.AI.Embeddings;

public sealed class QueryEmbeddingsBatchRequest : IDisposable
{
    public IList<string> Values { get; }
    public TaskCompletionSource<ReadOnlyMemory<float>[]> TaskCompletionSource { get; }
    private readonly CancellationTokenRegistration _tokenRegistration;

    public bool WasCanceled { get; private set; }

    public QueryEmbeddingsBatchRequest(IList<string> values, CancellationToken callerToken)
    {
        Values = values;
        TaskCompletionSource = new TaskCompletionSource<ReadOnlyMemory<float>[]>(TaskCreationOptions.RunContinuationsAsynchronously);

        if (callerToken.CanBeCanceled)
            _tokenRegistration = callerToken.Register(() => {
                WasCanceled = true;
                TaskCompletionSource.TrySetCanceled(callerToken);
            });
    }

    public Task<ReadOnlyMemory<float>[]> CancelWithShutdownMessage()
    {
        TaskCompletionSource.TrySetException(new OperationCanceledException(QueryEmbeddingsBatchingService.ShutdownMessage));
        return TaskCompletionSource.Task;
    }

    public void Dispose()
    {
        _tokenRegistration.Dispose();
    }
}
