#if NET8_0_OR_GREATER
using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Embedded;
using Xunit;

namespace EmbeddedTests
{
    public class ProcessOutputTests : EmbeddedTestBase
    {
        [Fact]
        public async Task Should_drain_standard_error_while_reading_standard_output()
        {
            using var process = StartShell("echo stderr-start 1>&2 & (for /l %i in (1,1,4096) do @echo stderr-content 1>&2) & echo stderr-end 1>&2 & echo stdout-end & exit /b 42",
                "echo stderr-start >&2; i=0; while [ $i -lt 4096 ]; do echo stderr-content >&2; i=$((i+1)); done; echo stderr-end >&2; echo stdout-end; exit 42");
            try
            {
                var output = process.ReadOutput(_ => { });
                // An unread stderr pipe blocks the child before it can finish stdout or exit.
                // https://learn.microsoft.com/en-us/dotnet/api/system.diagnostics.processstartinfo.redirectstandarderror#remarks
                Assert.True(await ProcessHelper.WaitForCompletionAsync(output.Completion, TimeSpan.FromSeconds(10)), "Both redirected pipes must be drained concurrently.");
                await output.Completion;
                var message = new StringBuilder();
                output.AppendDiagnostics(message, process.HasExited ? process.ExitCode : null);
                Assert.Contains("stderr-start", message.ToString());
                Assert.Contains("stderr-end", message.ToString());
                Assert.Contains("stdout-end", message.ToString());
                Assert.True(process.WaitForExit(10_000));
                Assert.Equal(42, process.ExitCode);
            }
            finally
            {
                StopProcess(process);
            }
        }

        [Fact]
        public async Task Should_release_startup_capture_and_continue_draining()
        {
            using var process = StartShell("echo ready & set /p release= & (for /l %i in (1,1,4096) do @(echo stdout-after-ready & echo stderr-after-ready 1>&2))",
                "echo ready; read release; i=0; while [ $i -lt 4096 ]; do echo stdout-after-ready; echo stderr-after-ready >&2; i=$((i+1)); done");
            try
            {
                var ready = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                int callbackCount = 0;
                var output = process.ReadOutput(_ =>
                {
                    Interlocked.Increment(ref callbackCount);
                    ready.TrySetResult(true);
                });
                await ready.Task.WaitAsync(TimeSpan.FromSeconds(10));
                output.StopCapturing();
                await process.StandardInput.WriteLineAsync("continue");
                await output.Completion.WaitAsync(TimeSpan.FromSeconds(10));
                var message = new StringBuilder();
                output.AppendDiagnostics(message, process.HasExited ? process.ExitCode : null);
                Assert.Contains("<capture stopped after startup>", message.ToString());
                Assert.DoesNotContain("stdout-after-ready", message.ToString());
                Assert.DoesNotContain("stderr-after-ready", message.ToString());
                Assert.Equal(1, callbackCount);
                Assert.True(process.WaitForExit(10_000));
                Assert.Equal(0, process.ExitCode);
            }
            finally
            {
                StopProcess(process);
            }
        }

        [Fact]
        public async Task Should_observe_reader_failure_while_the_process_is_still_running()
        {
            using var process = StartShell("echo ready & set /p release=", "echo ready; read release");
            try
            {
                var output = process.ReadOutput(_ => throw new IOException("output-reader-failure"));
                IOException error = Assert.IsType<IOException>(await output.ReadFailure.WaitAsync(TimeSpan.FromSeconds(10)));
                Assert.Equal("output-reader-failure", error.Message);
                Assert.False(process.HasExited);
                var message = new StringBuilder();
                output.AppendDiagnostics(message, process.HasExited ? process.ExitCode : null);
                Assert.Contains("output-reader-failure", message.ToString());
            }
            finally
            {
                StopProcess(process);
            }
        }

        [Fact]
        public async Task Should_wait_for_the_full_configured_timeout()
        {
            var pending = new TaskCompletionSource<bool>();
            var duration = TimeSpan.FromSeconds(1);
            var firstWait = ProcessHelper.WaitForCompletionAsync(pending.Task, duration);
            await Task.Delay(200); // Join the shared timer partway through its period.
            var elapsed = Stopwatch.StartNew();
            Assert.False(await ProcessHelper.WaitForCompletionAsync(pending.Task, duration));
            Assert.True(elapsed.Elapsed >= duration, $"The deadline fired after {elapsed.Elapsed}, before {duration}.");
            await firstWait;
        }

        [Fact]
        public async Task Should_report_a_nonzero_exit_with_empty_output()
        {
            using var process = StartShell("exit /b 51", "exit 51");
            try
            {
                var output = process.ReadOutput();
                await output.Completion.WaitAsync(TimeSpan.FromSeconds(10));
                Assert.True(process.WaitForExit(10_000));
                var message = new StringBuilder();
                output.AppendDiagnostics(message, process.ExitCode);

                Assert.Contains("Exit code: 51 (0x00000033)", message.ToString());
                Assert.Contains("Standard output:" + Environment.NewLine + "<empty>", message.ToString());
                Assert.Contains("Standard error:" + Environment.NewLine + "<empty>", message.ToString());
            }
            finally
            {
                StopProcess(process);
            }
        }

        [Fact]
        public async Task Should_bound_failure_collection_while_both_pipes_remain_open()
        {
            using var process = StartShell("echo ready & set /p release=", "echo ready; read release");
            try
            {
                var ready = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                var output = process.ReadOutput(_ => ready.TrySetResult(true));
                await ready.Task.WaitAsync(TimeSpan.FromSeconds(10));

                Assert.False(await output.DrainAsync(TimeSpan.FromMilliseconds(50)));
                Assert.Contains("ready", output.StandardOutput);
                Assert.False(process.HasExited);

                output.StopCapturing();
                await process.StandardInput.WriteLineAsync("continue");
                Assert.True(await output.DrainAsync(TimeSpan.FromSeconds(10)));
            }
            finally
            {
                StopProcess(process);
            }
        }

        [PosixFact]
        public async Task Should_finish_draining_at_eof_while_the_child_is_running()
        {
            using var process = StartShell(null, "echo stdout-marker; echo stderr-marker >&2; exec 1>&- 2>&-; read release");
            try
            {
                var output = process.ReadOutput();
                Assert.True(await output.DrainAsync(TimeSpan.FromSeconds(1)), "Drain waits for readers, not process exit.");
                Assert.False(process.HasExited);
                Assert.Contains("stdout-marker", output.StandardOutput);
            }
            finally
            {
                StopProcess(process);
            }
        }

        [PosixFact]
        public async Task Should_report_server_exit_without_waiting_for_inherited_pipes()
        {
            // A controlled inherited-pipe case; this does not establish the customer's process tree.
            ServerOptions options = CreateScriptServer("sleep 5 &\necho server-exit-marker >&2\nexit 23\n");
            options.MaxServerStartupTimeDuration = TimeSpan.FromSeconds(10);
            var elapsed = Stopwatch.StartNew();
            InvalidOperationException error = await GetStartupFailure(options);

            Assert.True(elapsed.Elapsed < TimeSpan.FromSeconds(2), "An exited server must use the bounded failure drain, not wait for inherited pipes indefinitely.");
            Assert.Contains("exited before startup completed", error.Message);
            Assert.Contains("Exit code: 23", error.Message);
            Assert.Contains("server-exit-marker", error.Message);
            Assert.Contains("output may be incomplete", error.Message);
        }

        [PosixFact]
        public async Task Should_collect_trailing_output_after_server_exit_before_taking_the_snapshot()
        {
            ServerOptions options = CreateScriptServer("(sleep 0.2; printf trailing-stdout; printf trailing-stderr >&2) &\necho server-exit-marker\nexit 23\n");
            options.ProcessKillTimeout = TimeSpan.FromSeconds(5);
            var elapsed = Stopwatch.StartNew();
            InvalidOperationException error = await GetStartupFailure(options);

            Assert.Contains("trailing-stdout", error.Message);
            Assert.Contains("trailing-stderr", error.Message);
            Assert.Contains("Exit code: 23 (0x00000017)", error.Message);
            Assert.DoesNotContain("output may be incomplete", error.Message);
            Assert.True(elapsed.Elapsed < TimeSpan.FromSeconds(3), "Completed readers must not incur a fixed failure-drain delay.");
        }

        [PosixFact]
        public async Task Should_fail_startup_on_a_zero_exit_without_readiness()
        {
            ServerOptions options = CreateScriptServer("printf stdout-before-exit\nprintf stderr-before-exit >&2\nexit 0\n");
            InvalidOperationException error = await GetStartupFailure(options);

            Assert.Contains("before startup completed", error.Message);
            Assert.Contains("Exit code: 0 (0x00000000)", error.Message);
            Assert.Contains("stdout-before-exit", error.Message);
            Assert.Contains("stderr-before-exit", error.Message);
        }

        [PosixFact]
        public async Task Should_preserve_output_and_the_full_server_startup_timeout()
        {
            ServerOptions options = CreateScriptServer("echo stdout-before-timeout\necho stderr-before-timeout >&2\nexec sleep 30\n");
            options.MaxServerStartupTimeDuration = TimeSpan.FromSeconds(1);
            var elapsed = Stopwatch.StartNew();
            InvalidOperationException error = await GetStartupFailure(options);

            // The component deadline test separately excludes graceful shutdown/drain time from its measurement.
            Assert.True(elapsed.Elapsed >= options.MaxServerStartupTimeDuration);
            Assert.Contains("did not complete within", error.Message);
            Assert.Contains("stdout-before-timeout", error.Message);
            Assert.Contains("stderr-before-timeout", error.Message);
            Assert.DoesNotContain("Exit code:", error.Message);
        }

        [PosixFact]
        public async Task Should_shutdown_gracefully_and_keep_the_initiating_startup_failure()
        {
            ServerOptions options = CreateScriptServer("echo stdout-before-timeout\necho stderr-before-timeout >&2\nread command\necho \"received: $command\" >&2\nprintf trailing-stdout\nprintf trailing-stderr >&2\nexit 0\n");
            options.MaxServerStartupTimeDuration = TimeSpan.FromMilliseconds(200);
            options.GracefulShutdownTimeout = TimeSpan.FromSeconds(5);
            options.ProcessKillTimeout = TimeSpan.FromSeconds(5);
            InvalidOperationException error = await GetStartupFailure(options);

            Assert.Contains("did not complete within", error.InnerException.Message);
            Assert.Contains("received: shutdown no-confirmation", error.Message);
            Assert.Contains("trailing-stdout", error.Message);
            Assert.Contains("trailing-stderr", error.Message);
            Assert.DoesNotContain("Exit code:", error.Message);
        }

        [PosixFact]
        public async Task Should_keep_draining_the_server_after_readiness()
        {
            string release = NewDataPath() + ".release";
            // This producer deliberately exceeds pipe capacity; a default server overflow route is not established.
            ServerOptions options = CreateScriptServer($"echo 'Server available on: http://127.0.0.1:54321/'\nwhile [ ! -f '{release}' ]; do sleep 0.01; done\ni=0; while [ $i -lt 8192 ]; do echo stdout-after-ready; echo stderr-after-ready >&2; i=$((i+1)); done\nexit 23\n");
            using var embedded = new EmbeddedServer();
            var exited = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            embedded.ServerProcessExited += (_, _) => exited.TrySetResult(true);
            try
            {
                embedded.StartServer(options);
                Assert.Equal("http://127.0.0.1:54321/", (await embedded.GetServerUriAsync()).AbsoluteUri);
                Directory.CreateDirectory(Path.GetDirectoryName(release));
                File.WriteAllText(release, "continue");
                await exited.Task.WaitAsync(TimeSpan.FromSeconds(10));
            }
            finally
            {
                File.Delete(release);
            }
        }

        [PosixFact]
        public async Task Should_preserve_large_stderr_during_discovery_and_startup()
        {
            ServerOptions options = CreateScriptServer("echo stderr-start >&2\ni=0; while [ $i -lt 8192 ]; do echo stderr-content >&2; i=$((i+1)); done\necho stderr-end >&2\nprintf stdout-end\nexit 42\n");
            options.DotNetPath = Path.Combine(options.ServerDirectory, "Raven.Server");
            options.FrameworkVersion = "8.0.x";
            InvalidOperationException discoveryError = await Assert.ThrowsAsync<InvalidOperationException>(() => RuntimeFrameworkVersionMatcher.MatchAsync(options).WaitAsync(TimeSpan.FromSeconds(10)));

            Assert.Contains("stderr-start", discoveryError.Message);
            Assert.Contains("stderr-end", discoveryError.Message);
            Assert.Contains("stdout-end", discoveryError.Message);
            Assert.Contains("Exit code: 42", discoveryError.Message);

            options.FrameworkVersion = null;
            InvalidOperationException startupError = await GetStartupFailure(options);
            Assert.Contains("stderr-start", startupError.Message);
            Assert.Contains("stderr-end", startupError.Message);
            Assert.Contains("stdout-end", startupError.Message);
            Assert.Contains("Exit code: 42", startupError.Message);
        }

        [PosixFact]
        public async Task Should_wait_for_discovery_output_without_a_startup_or_failure_drain_deadline()
        {
            ServerOptions options = CreateScriptServer("echo '.NET runtimes installed:'\necho '  Microsoft.NETCore.App 8.0.30 [/controlled/runtime]'\nexec 1>&-\nsleep 0.2\necho stderr-after-stdout >&2\nsleep 0.2\nexit 0\n");
            options.DotNetPath = Path.Combine(options.ServerDirectory, "Raven.Server");
            options.FrameworkVersion = "8.0.x";
            options.MaxServerStartupTimeDuration = TimeSpan.FromMilliseconds(50);
            options.ProcessKillTimeout = TimeSpan.FromMilliseconds(25);

            Assert.Equal("8.0.30", await RuntimeFrameworkVersionMatcher.MatchAsync(options).WaitAsync(TimeSpan.FromSeconds(10)));
        }

        [PosixFact]
        public async Task Should_preserve_malformed_inventory_and_its_parse_error()
        {
            ServerOptions options = CreateScriptServer("echo '.NET runtimes installed:'\necho '  Microsoft.NETCore.App invalid-version [/controlled/runtime]'\necho stderr-context >&2\nexit 42\n");
            options.DotNetPath = Path.Combine(options.ServerDirectory, "Raven.Server");
            options.FrameworkVersion = "8.0.x";
            InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(() => RuntimeFrameworkVersionMatcher.MatchAsync(options).WaitAsync(TimeSpan.FromSeconds(10)));

            Assert.Contains("Cannot parse 'invalid' to a number", error.InnerException.Message);
            Assert.Contains("Microsoft.NETCore.App invalid-version", error.Message);
            Assert.Contains("stderr-context", error.Message);
            Assert.Contains("Exit code: 42", error.Message);
        }

        [PosixFact]
        public async Task Should_preserve_the_exit_code_when_streams_end_before_exit_is_observed()
        {
            ServerOptions options = CreateScriptServer("echo stdout-marker-7\necho stderr-marker-7 >&2\nexit 7\n");

            // Repeated ordinary exits exercise the Unix ordering between pipe EOF and exit notification.
            for (int i = 0; i < 200; i++)
            {
                InvalidOperationException error = await GetStartupFailure(options);
                Assert.Contains("Exit code: 7 (0x00000007)", error.Message);
                Assert.Contains("stdout-marker-7", error.Message);
                Assert.Contains("stderr-marker-7", error.Message);
            }
        }

        [PosixFact]
        public async Task Should_report_output_end_without_a_cleanup_exit_code_while_the_server_is_running()
        {
            ServerOptions options = CreateScriptServer("echo stdout-marker-c\necho stderr-marker-c >&2\nexec 1>&- 2>&-\nexec sleep 30\n");
            options.ProcessKillTimeout = TimeSpan.FromSeconds(30);
            var elapsed = Stopwatch.StartNew();
            InvalidOperationException error = await GetStartupFailure(options);

            Assert.True(elapsed.Elapsed >= TimeSpan.FromSeconds(5), "A live process with closed streams must receive the full five-second exit-observation interval.");
            Assert.True(elapsed.Elapsed < TimeSpan.FromSeconds(10), "Exit observation must not wait for the thirty-second process kill timeout.");
            Assert.Contains("output ended before startup completed", error.InnerException.Message);
            Assert.DoesNotContain("Exit code:", error.Message);
            Assert.Contains("stdout-marker-c", error.Message);
            Assert.Contains("stderr-marker-c", error.Message);
        }

        private static async Task<InvalidOperationException> GetStartupFailure(ServerOptions options)
        {
            var embedded = new EmbeddedServer();
            try
            {
                embedded.StartServer(options);
                return await Assert.ThrowsAsync<InvalidOperationException>(() => embedded.GetServerUriAsync().WaitAsync(TimeSpan.FromSeconds(15)));
            }
            finally
            {
                // Startup is explicitly observed above; Dispose preserves its historical AggregateException.
                try
                {
                    embedded.Dispose();
                }
                catch (AggregateException)
                {
                }
            }
        }
        private ServerOptions CreateScriptServer(string script)
        {
            if (OperatingSystem.IsWindows())
                throw new PlatformNotSupportedException();
            var directory = NewDataPath();
            Directory.CreateDirectory(directory);
            var executable = Path.Combine(directory, "Raven.Server");
            File.WriteAllText(executable, "#!/bin/sh\n" + script);
            File.SetUnixFileMode(executable, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
            return new ServerOptions
            {
                ServerDirectory = directory,
                DataDirectory = directory,
                LogsPath = directory,
                MaxServerStartupTimeDuration = TimeSpan.FromSeconds(10),
                GracefulShutdownTimeout = TimeSpan.FromMilliseconds(50),
                ProcessKillTimeout = TimeSpan.FromMilliseconds(50)
            };
        }

        private static Process StartShell(string windowsCommand, string posixCommand)
        {
            var options = new ProcessStartInfo
            {
                FileName = OperatingSystem.IsWindows() ? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.System), "cmd.exe") : "/bin/sh",
                UseShellExecute = false,
                CreateNoWindow = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                RedirectStandardInput = true
            };

            if (OperatingSystem.IsWindows())
            {
                options.ArgumentList.Add("/d");
                options.ArgumentList.Add("/s");
                options.ArgumentList.Add("/c");
            }
            else
            {
                options.ArgumentList.Add("-c");
            }

            options.ArgumentList.Add(OperatingSystem.IsWindows() ? windowsCommand : posixCommand);
            return Process.Start(options);
        }

        private static void StopProcess(Process process)
        {
            if (process.HasExited != false)
                return;

            process.Kill(entireProcessTree: true);
            Assert.True(process.WaitForExit(10_000));
        }

        private sealed class PosixFactAttribute : FactAttribute
        {
            public PosixFactAttribute()
            {
                if (OperatingSystem.IsWindows())
                    Skip = "This startup scenario uses a Unix shell as the child executable.";
            }
        }
    }
}
#endif
