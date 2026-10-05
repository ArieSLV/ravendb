using System;
using System.Collections.Generic;
using System.Linq;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.IO;
using System.Threading.Tasks;
using Raven.Embedded;
using Xunit;

namespace EmbeddedTests
{
    public class RuntimeFrameworkVersionMatcherTests : EmbeddedTestBase
    {
        [Fact]
        public async Task MatchTest1()
        {
            var options = new ServerOptions();

            var defaultFrameworkVersion = ServerOptions.Default.FrameworkVersion;
            Assert.True(defaultFrameworkVersion.EndsWith(RuntimeFrameworkVersionMatcher.GreaterOrEqual.ToString()));

            var expectedVersion = new Version(defaultFrameworkVersion.Substring(0, defaultFrameworkVersion.Length - 1));
            var actualVersion = new Version(await RuntimeFrameworkVersionMatcher.MatchAsync(options));

            Assert.True(actualVersion.CompareTo(expectedVersion) >= 0);

            options.FrameworkVersion = null;
            Assert.Null(await RuntimeFrameworkVersionMatcher.MatchAsync(options));

            options = new ServerOptions();

            var frameworkVersion = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion(options.FrameworkVersion)
            {
                Patch = null
            };

            options.FrameworkVersion = frameworkVersion.ToString();
            var match = await RuntimeFrameworkVersionMatcher.MatchAsync(options);
            Assert.NotNull(match);
            var matchFrameworkVersion = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion(match);
            Assert.True(matchFrameworkVersion.Major.HasValue);
            Assert.True(matchFrameworkVersion.Minor.HasValue);
            Assert.True(matchFrameworkVersion.Patch.HasValue);

            Assert.True(frameworkVersion.Match(matchFrameworkVersion));

            options = new ServerOptions
            {
                DotNetPath = Path.GetTempFileName(),
                FrameworkVersion = frameworkVersion.ToString()
            };

            var e = await Assert.ThrowsAsync<InvalidOperationException>(() => RuntimeFrameworkVersionMatcher.MatchAsync(options));
            Assert.Contains("Unable to execute dotnet to retrieve list of installed runtimes", e.Message);
        }

        [Fact]
        public void MatchTest2()
        {
            var runtimes = GetRuntimes();

            var runtime = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("3.1.1");
            Assert.Equal("3.1.1", RuntimeFrameworkVersionMatcher.Match(runtime, runtimes));

            runtime = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("2.1.11");
            Assert.Equal("2.1.11", RuntimeFrameworkVersionMatcher.Match(runtime, runtimes));

            runtime = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("3.1.x");
            Assert.Equal("3.1.3", RuntimeFrameworkVersionMatcher.Match(runtime, runtimes));

            runtime = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("3.x");
            Assert.Equal("3.2.3", RuntimeFrameworkVersionMatcher.Match(runtime, runtimes));

            runtime = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("3.x.x");
            Assert.Equal("3.2.3", RuntimeFrameworkVersionMatcher.Match(runtime, runtimes));

            runtime = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("5.0.x");
            Assert.Equal("5.0.4", RuntimeFrameworkVersionMatcher.Match(runtime, runtimes));

            runtime = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("x");
            Assert.Equal("5.0.4", RuntimeFrameworkVersionMatcher.Match(runtime, runtimes));

            runtime = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("5.0.x-rc.2.20475.17");
            Assert.Equal("5.0.0-rc.2.20475.17", RuntimeFrameworkVersionMatcher.Match(runtime, runtimes));

            runtime = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("6.x");
            var e = Assert.Throws<InvalidOperationException>(() => RuntimeFrameworkVersionMatcher.Match(runtime, runtimes));
            Assert.Contains("Could not find a matching runtime for '6.x.x'", e.Message);
        }

        [Fact]
        public void MatchTest3()
        {
            var runtime = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("3.1.0-rc");
            Assert.Equal("3.1.0-rc", runtime.ToString());

            runtime = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("5.0.0-rc.2.20475.17");
            Assert.Equal("5.0.0-rc.2.20475.17", runtime.ToString());
        }

        [Fact]
        public void MatchTest4()
        {
            var runtimes = GetRuntimes();

            var runtime = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("3.1.1+");
            Assert.Equal("3.1.1+", runtime.ToString());
            Assert.Equal("3.1.3", RuntimeFrameworkVersionMatcher.Match(runtime, runtimes));

            runtime = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("3.1.4+");
            Assert.Equal("3.1.4+", runtime.ToString());
            var e = Assert.Throws<InvalidOperationException>(() => RuntimeFrameworkVersionMatcher.Match(runtime, runtimes));
            Assert.Contains("Could not find a matching runtime for '3.1.4+'. Available runtimes:", e.Message);

            e = Assert.Throws<InvalidOperationException>(() => new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("6.0.0+-preview.6.21352.12"));
            Assert.Equal("Cannot set 'Patch' with value '0+' because '+' is not allowed when Suffix ('preview.6.21352.12') is set.", e.Message);

            e = Assert.Throws<InvalidOperationException>(() => new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("6+"));
            Assert.Equal("Cannot set 'Major' with value '6+' because '+' is not allowed.", e.Message);

            e = Assert.Throws<InvalidOperationException>(() => new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("3.1+"));
            Assert.Equal("Cannot set 'Minor' with value '1+' because '+' is not allowed.", e.Message);
        }

#if NETCOREAPP
        [Fact]
        public void Should_report_runtime_discovery_failure_when_dotnet_host_is_incomplete()
        {
            ServerOptions options = CopyServerAndCreateOptions();
            options.DotNetPath = CopyDotNetHost();

            InvalidOperationException error = GetStartupFailure(options);

            Assert.DoesNotContain("Could not find a matching runtime", error.Message);
            Assert.Contains("Unable to discover installed .NET runtimes", error.Message);
            Assert.Contains(options.DotNetPath, error.Message);
            Assert.Contains("--info", error.Message);
            Assert.Contains("Working directory:", error.Message);
            Assert.Contains(RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "-2147450749 (0x80008083)" : "131 (0x00000083)", error.Message);
            Assert.Contains("Standard output:", error.Message);
            Assert.Contains("Standard error:", error.Message);
            Assert.Contains("host", error.Message);
            Assert.Contains("fxr", error.Message);
        }

        [Fact]
        public void Should_preserve_a_genuine_runtime_version_mismatch()
        {
            ServerOptions options = CopyServerAndCreateOptions();
            options.FrameworkVersion = "999.0.0+";

            InvalidOperationException error = GetStartupFailure(options);
            Assert.Contains("Could not find a matching runtime for '999.0.0+'. Available runtimes:", error.Message);
            Assert.Contains(Environment.NewLine + "- ", error.Message);
        }
#if NET8_0_OR_GREATER
        [Fact]
        public async Task Should_use_available_runtimes_when_the_sdk_cannot_start()
        {
            var options = CopyServerAndCreateOptions();
            options.DotNetPath = CopyDotNetHost();
            var privateRoot = Path.GetDirectoryName(options.DotNetPath);
            var runtimeDirectory = RuntimeEnvironment.GetRuntimeDirectory().TrimEnd(Path.DirectorySeparatorChar);
            var installation = Directory.GetParent(runtimeDirectory).Parent.Parent.FullName;
            var runtimeVersion = Path.GetFileName(runtimeDirectory);
            CopyDirectory(runtimeDirectory, Path.Combine(privateRoot, "shared", "Microsoft.NETCore.App", runtimeVersion));
            CopyDirectory(Path.Combine(installation, "shared", "Microsoft.AspNetCore.App", runtimeVersion), Path.Combine(privateRoot, "shared", "Microsoft.AspNetCore.App", runtimeVersion));

            var hostDirectory = Directory.GetDirectories(Path.Combine(installation, "host", "fxr"))[0];
            CopyDirectory(hostDirectory, Path.Combine(privateRoot, "host", "fxr", Path.GetFileName(hostDirectory)));
            var sdkDirectory = Directory.GetDirectories(Path.Combine(installation, "sdk"))
                .OrderByDescending(x => new Version(Path.GetFileName(x).Split('-')[0]))
                .First(x => Path.GetFileName(x).StartsWith(Environment.Version.Major + "."));
            var sdkCopy = Path.Combine(privateRoot, "sdk", Path.GetFileName(sdkDirectory));
            Directory.CreateDirectory(sdkCopy);
            File.Copy(Path.Combine(sdkDirectory, "dotnet.dll"), Path.Combine(sdkCopy, "dotnet.dll"));
            // Only the SDK's runtime is missing. The application's installed runtime remains usable.
            File.WriteAllText(Path.Combine(sdkCopy, "dotnet.runtimeconfig.json"), "{\"runtimeOptions\":{\"framework\":{\"name\":\"Microsoft.NETCore.App\",\"version\":\"999.0.0\"}}}");
            options.FrameworkVersion = runtimeVersion + "+";

            using (var probe = Process.Start(new ProcessStartInfo(options.DotNetPath, "--info")
                   { UseShellExecute = false, CreateNoWindow = true, RedirectStandardOutput = true, RedirectStandardError = true }))
            {
                var stdout = probe.StandardOutput.ReadToEndAsync();
                var stderr = probe.StandardError.ReadToEndAsync();
                Assert.True(probe.WaitForExit(30_000));
                Assert.NotEqual(0, probe.ExitCode);
                Assert.Contains("Microsoft.NETCore.App " + runtimeVersion, await stdout);
                Assert.Contains("999.0.0", await stderr);
            }

            using var embedded = new EmbeddedServer();
            embedded.StartServer(options);
            Assert.NotNull(await embedded.GetServerUriAsync());
            Assert.True(await embedded.GetServerProcessIdAsync() > 0);
        }

        private static void CopyDirectory(string source, string destination)
        {
            Directory.CreateDirectory(destination);
            foreach (var file in Directory.GetFiles(source))
                File.Copy(file, Path.Combine(destination, Path.GetFileName(file)));
            foreach (var directory in Directory.GetDirectories(source))
                CopyDirectory(directory, Path.Combine(destination, Path.GetFileName(directory)));
        }
#endif

        [Fact]
        public async Task Should_rethrow_the_observed_startup_failure_when_disposed()
        {
            ServerOptions options = CopyServerAndCreateOptions();
            options.DotNetPath = CopyDotNetHost();
            var embedded = new EmbeddedServer();
            embedded.StartServer(options);
            InvalidOperationException startupError = await Assert.ThrowsAsync<InvalidOperationException>(() => embedded.GetServerUriAsync());

            AggregateException disposalError = Assert.Throws<AggregateException>(() => embedded.Dispose());
            Assert.Same(startupError, disposalError.InnerException);
        }

        private static InvalidOperationException GetStartupFailure(ServerOptions options)
        {
            AggregateException error = Assert.Throws<AggregateException>(() =>
            {
                using var embedded = new EmbeddedServer();
                embedded.StartServer(options);
            });
            return Assert.IsType<InvalidOperationException>(error.InnerException);
        }
        private string CopyDotNetHost()
        {
            string runtimeDirectory = RuntimeEnvironment.GetRuntimeDirectory();
            string installation = Directory.GetParent(runtimeDirectory.TrimEnd(Path.DirectorySeparatorChar)).Parent.Parent.FullName;
            string executable = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "dotnet.exe" : "dotnet";
            string directory = Path.Combine(NewDataPath(), "incomplete dotnet installation");
            Directory.CreateDirectory(directory);
            string copy = Path.Combine(directory, executable);
            File.Copy(Path.Combine(installation, executable), copy);
            return copy;
        }

#endif
        private static List<RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion> GetRuntimes()
        {
            return new()
            {
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("2.1.3"),
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("2.1.4"),
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("2.1.11"),
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("2.2.0"),
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("2.2.1"),
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("3.1.0"),
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("3.1.1"),
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("3.1.2"),
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("3.1.3"),
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("3.2.3"),
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("5.0.0-rc.2.20475.17"),
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("5.0.3"),
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("5.0.4"),
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("6.0.0-preview.6.21352.12"),
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("6.0.0-rc.1.21451.13")
            };
        }
    }
}
