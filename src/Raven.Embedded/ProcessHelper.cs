#nullable enable
using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Extensions;

namespace Raven.Embedded
{
    internal static class ProcessHelper
    {
        internal static ProcessOutput ReadOutput(this Process process, Action<string>? onOutputLine = null) => new(process, onOutputLine);

        internal static async Task<bool> WaitForCompletionAsync(Task task, TimeSpan timeout)
        {
            if (timeout == Timeout.InfiniteTimeSpan || timeout == TimeSpan.MaxValue)
                return await task.WaitWithTimeout(Timeout.InfiniteTimeSpan).ConfigureAwait(false);

            var elapsed = Stopwatch.StartNew();
            while (task.IsCompleted == false)
            {
                var remaining = timeout - elapsed.Elapsed;
                if (remaining <= TimeSpan.Zero)
                    return task.IsCompleted;

                // WaitWithTimeout uses shared timers which may fire early. The stopwatch owns the deadline.
                if (await task.WaitWithTimeout(remaining).ConfigureAwait(false))
                    return true;
            }
            return true;
        }

        internal sealed class ProcessOutput
        {
            private readonly string _executable;
            private readonly string _workingDirectory;
            private readonly object _locker = new();
            private readonly TaskCompletionSource<Exception> _readFailure = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private StringBuilder? _standardOutput = new();
            private StringBuilder? _standardError = new();
            private Action<string>? _onOutputLine;

            // Completion concerns the two readers only. The caller owns process exit and cleanup.
            internal Task Completion { get; }
            internal Task<Exception> ReadFailure => _readFailure.Task;

            internal string StandardOutput
            {
                get
                {
                    lock (_locker)
                        return _standardOutput?.ToString() ?? string.Empty;
                }
            }

            internal ProcessOutput(Process process, Action<string>? onOutputLine)
            {
                _executable = process.StartInfo.FileName;
                _workingDirectory = string.IsNullOrEmpty(process.StartInfo.WorkingDirectory) ? Directory.GetCurrentDirectory() : process.StartInfo.WorkingDirectory;
                _onOutputLine = onOutputLine;

                var stdout = process.StandardOutput;
                var stderr = process.StandardError;

                // Start both readers immediately and keep draining after server readiness.
                var stdoutTask = ReadStream(stdout, isStandardOutput: true);
                var stderrTask = ReadStream(stderr, isStandardOutput: false);
                Completion = Task.WhenAll(stdoutTask, stderrTask);
                _ = Completion.IgnoreUnobservedExceptions();
            }

            internal void StopCapturing()
            {
                lock (_locker)
                {
                    _standardOutput = null;
                    _standardError = null;
                    _onOutputLine = null;
                }
            }

            internal async Task<bool> DrainAsync(TimeSpan timeout)
            {
                try
                {
                    if (await WaitForCompletionAsync(Completion, timeout).ConfigureAwait(false) == false)
                        return false;

                    await Completion.ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Reader failures are included in AppendDiagnostics; they must not replace the initiating failure.
                }

                return Completion.IsCompleted;
            }

            internal void AppendDiagnostics(StringBuilder message, int? exitCode)
            {
                // Server arguments can contain license keys and certificate passwords.
                // Report the configured executable; reconstructing PATH lookup can misidentify the launched image.
                message.AppendLine($"Executable: '{_executable}'");
                message.AppendLine($"Working directory: '{_workingDirectory}'");
                if (exitCode.HasValue)
                    message.AppendLine($"Exit code: {exitCode.Value} (0x{exitCode.Value:X8})");

                lock (_locker)
                {
                    message.AppendLine("Standard output:");
                    AppendOutput(message, _standardOutput);
                    message.AppendLine("Standard error:");
                    AppendOutput(message, _standardError);
                }

                if (ReadFailure.IsCompleted)
                {
                    message.AppendLine("Failed to read process output:");
                    message.AppendLine(ReadFailure.Result.ToString());
                }
            }

            private static void AppendOutput(StringBuilder message, StringBuilder? output)
            {
                message.AppendLine(output == null
                    ? "<capture stopped after startup>"
                    : output.Length == 0
                        ? "<empty>"
                        : output.ToString());
            }

            private async Task ReadStream(StreamReader stream, bool isStandardOutput)
            {
                try
                {
                    using (stream)
                    {
                        string? line;
                        while ((line = await stream.ReadLineAsync().ConfigureAwait(false)) != null)
                        {
                            lock (_locker)
                            {
                                if (isStandardOutput)
                                {
                                    _standardOutput?.AppendLine(line);
                                    _onOutputLine?.Invoke(line);
                                }
                                else
                                    _standardError?.AppendLine(line);
                            }
                        }
                    }
                }
                catch (Exception error)
                {
                    _readFailure.TrySetResult(error);
                    throw;
                }
            }
        }
    }
}
