using System;
using System.IO;
using Raven.Server.Documents.Replication.ReplicationItems;

namespace Raven.Server.Documents.Replication
{
    // Temporary investigation helper for collecting a narrow, ordered replication trace
    // without relying on the full RavenDB server logs.
    public static class ReplicationInvestigationTrace
    {
        private static readonly object Locker = new();

        private static string _path;
        private static string[] _interestingPrefixes = Array.Empty<string>();
        private static long _sequence;

        public static bool Enabled => string.IsNullOrEmpty(_path) == false;

        public static string CurrentPath => _path;

        public static string ReadablePath => ReplicationInvestigationReadableTrace.CurrentPath;

        public static void Configure(string path, params string[] interestingPrefixes)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException("Trace path cannot be null or empty.", nameof(path));

            var directory = Path.GetDirectoryName(path);
            if (string.IsNullOrEmpty(directory) == false)
                Directory.CreateDirectory(directory);

            lock (Locker)
            {
                File.WriteAllText(path, string.Empty);
                _path = path;
                _interestingPrefixes = interestingPrefixes ?? Array.Empty<string>();
                _sequence = 0;
            }

            ReplicationInvestigationReadableTrace.Configure(GetReadablePath(path), interestingPrefixes);
        }

        public static void Reset()
        {
            lock (Locker)
            {
                _path = null;
                _interestingPrefixes = Array.Empty<string>();
                _sequence = 0;
            }

            ReplicationInvestigationReadableTrace.Reset();
        }

        public static bool ShouldTrace(string documentId = null)
        {
            if (Enabled == false)
                return false;

            if (documentId == null || _interestingPrefixes.Length == 0)
                return true;

            foreach (var prefix in _interestingPrefixes)
            {
                if (string.IsNullOrEmpty(prefix))
                    continue;

                if (documentId.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            return false;
        }

        public static void Write(string eventType, string message, string documentId = null)
        {
            if (ShouldTrace(documentId) == false)
                return;

            lock (Locker)
            {
                var path = _path;
                if (string.IsNullOrEmpty(path))
                    return;

                var sequence = ++_sequence;
                var timestampUtc = DateTime.UtcNow;
                var processId = Environment.ProcessId;
                var threadId = Environment.CurrentManagedThreadId;
                var sanitizedMessage = Sanitize(message);
                var line =
                    $"{sequence:D6}|{timestampUtc:O}|pid={processId}|tid={threadId}|{eventType}|{sanitizedMessage}";

                File.AppendAllText(path, line + Environment.NewLine);
                ReplicationInvestigationReadableTrace.Write(sequence, timestampUtc, processId, threadId, eventType, sanitizedMessage, documentId);
            }
        }

        public static string TryGetItemId(ReplicationBatchItem item)
        {
            return item switch
            {
                DocumentReplicationItem doc => doc.Id,
                CounterReplicationItem counter => counter.Id,
                _ => null
            };
        }

        private static string Sanitize(string message)
        {
            return message?
                .Replace(Environment.NewLine, " ", StringComparison.Ordinal)
                .Replace('\r', ' ')
                .Replace('\n', ' ');
        }

        private static string GetReadablePath(string machineReadablePath)
        {
            var directory = Path.GetDirectoryName(machineReadablePath);
            var fileName = Path.GetFileNameWithoutExtension(machineReadablePath);
            var extension = Path.GetExtension(machineReadablePath);
            var readableFileName = fileName + ".readable" + extension;

            return string.IsNullOrEmpty(directory)
                ? readableFileName
                : Path.Combine(directory, readableFileName);
        }
    }
}
