using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Raven.Server.Documents.Replication
{
    // Temporary investigation helper for collecting a human-readable replication trace
    // alongside the machine-readable one.
    public static class ReplicationInvestigationReadableTrace
    {
        private static readonly object Locker = new();

        private static string _path;
        private static string[] _interestingPrefixes = Array.Empty<string>();

        public static bool Enabled => string.IsNullOrEmpty(_path) == false;

        public static string CurrentPath => _path;

        public static void Configure(string path, params string[] interestingPrefixes)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException("Readable trace path cannot be null or empty.", nameof(path));

            var directory = Path.GetDirectoryName(path);
            if (string.IsNullOrEmpty(directory) == false)
                Directory.CreateDirectory(directory);

            lock (Locker)
            {
                File.WriteAllText(path, string.Empty);
                _path = path;
                _interestingPrefixes = interestingPrefixes ?? Array.Empty<string>();
            }
        }

        public static void Reset()
        {
            lock (Locker)
            {
                _path = null;
                _interestingPrefixes = Array.Empty<string>();
            }
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

        public static void Write(long sequence, DateTime timestampUtc, int processId, int threadId, string eventType, string message, string documentId = null)
        {
            if (ShouldTrace(documentId) == false)
                return;

            var parsed = ParseMessage(message);
            var effectiveDocumentId = string.IsNullOrEmpty(documentId) == false ? documentId : parsed.DocumentId;
            if (ShouldWriteEntry(eventType, effectiveDocumentId, parsed) == false)
                return;

            lock (Locker)
            {
                var path = _path;
                if (string.IsNullOrEmpty(path))
                    return;

                File.AppendAllText(path, FormatEntry(sequence, timestampUtc, processId, threadId, eventType, parsed, effectiveDocumentId));
            }
        }

        private static string FormatEntry(long sequence, DateTime timestampUtc, int processId, int threadId, string eventType, ParsedMessage parsed, string documentId)
        {
            var builder = new StringBuilder();
            builder.Append('#').Append(sequence.ToString("D6"))
                .Append(" | ").Append(timestampUtc.ToString("O"))
                .Append(" | pid=").Append(processId)
                .Append(" tid=").Append(threadId)
                .AppendLine();
            builder.Append("event: ").Append(eventType).Append(" - ").AppendLine(DescribeEvent(eventType));

            if (string.IsNullOrEmpty(documentId) == false)
                builder.Append("document: ").AppendLine(documentId);

            if (string.IsNullOrEmpty(parsed.Subject) == false)
                builder.Append("subject: ").AppendLine(parsed.Subject);

            var importantFields = GetImportantFields(eventType, parsed.Fields);
            if (importantFields.Count > 0)
            {
                builder.AppendLine("details:");
                foreach (var field in importantFields)
                    AppendField(builder, field.Key, field.Value);
            }

            builder.AppendLine();
            return builder.ToString();
        }

        private static bool ShouldWriteEntry(string eventType, string documentId, ParsedMessage parsed)
        {
            switch (eventType)
            {
                case "FAULT_RULE_ARMED":
                case "FAULT_RULE_MATCHED":
                case "FAULT_SKIP_AND_ADVANCE":
                case "FAULT_RULE_COMPLETED":
                case "FAULT_RULE_DISPOSED":
                case "PULL_SINK_REMOTE_TOPOLOGY":
                case "PULL_SINK_TCP_INFO":
                case "OUTGOING_PULL_HANDSHAKE":
                case "OUTGOING_MRE_WAIT":
                case "OUTGOING_MRE_RELEASED":
                case "SENDER_JUMP_AHEAD":
                case "SENDER_BATCH_REPLY":
                    return true;

                case "SENDER_BATCH_SEND":
                    return ContainsHighlightedBatchItems(parsed);

                case "SENDER_ALREADY_MERGED_SKIP":
                    return IsHighlightedDocument(documentId);

                case "INCOMING_PULL_PREPROCESS":
                case "INCOMING_DB_CV_MERGE":
                    return IsHighlightedDocument(documentId);

                default:
                    return false;
            }
        }

        // The human-readable log intentionally focuses on the small set of documents
        // that tell the story of the scenario, while the machine-readable log remains exhaustive.
        private static bool IsHighlightedDocument(string documentId)
        {
            if (string.IsNullOrEmpty(documentId))
                return false;

            return documentId.EndsWith("/seed", StringComparison.OrdinalIgnoreCase) ||
                   documentId.EndsWith("/1", StringComparison.OrdinalIgnoreCase) ||
                   documentId.EndsWith("/2", StringComparison.OrdinalIgnoreCase) ||
                   documentId.EndsWith("/3", StringComparison.OrdinalIgnoreCase) ||
                   documentId.EndsWith("/10", StringComparison.OrdinalIgnoreCase) ||
                   documentId.IndexOf("after-gap", StringComparison.OrdinalIgnoreCase) >= 0;
        }

        private static List<KeyValuePair<string, string>> GetImportantFields(string eventType, List<KeyValuePair<string, string>> fields)
        {
            string[] fieldNames = eventType switch
            {
                "FAULT_RULE_ARMED" => new[] { "db", "ruleId", "label", "source", "target", "minEtagExclusive", "expected" },
                "FAULT_RULE_MATCHED" => new[] { "db", "ruleId", "label", "source", "target", "minEtagExclusive", "etag", "matched", "expected" },
                "FAULT_SKIP_AND_ADVANCE" => new[] { "db", "minEtagExclusive", "etag", "cv", "decision" },
                "FAULT_RULE_COMPLETED" => new[] { "db", "ruleId", "label", "source", "target", "minEtagExclusive", "matched", "expected" },
                "FAULT_RULE_DISPOSED" => new[] { "db", "ruleId", "label", "source", "target", "minEtagExclusive", "matched", "expected" },
                "FAULT_HEARTBEAT_SUPPRESSION_ARMED" => new[] { "db", "label", "source", "target" },
                "FAULT_HEARTBEAT_SUPPRESSION_DISPOSED" => new[] { "db", "label", "source", "target" },
                "FAULT_HEARTBEAT_SUPPRESSED" => new[] { "db", "source", "target", "changeVector" },
                "PULL_SINK_REMOTE_TOPOLOGY" => new[] { "db", "sinkTask", "hubName", "bootstrapUrl", "remoteUrls" },
                "PULL_SINK_TCP_INFO" => new[] { "db", "sinkTask", "hubName", "requestUrl", "tcpUrl", "tcpNode" },
                "OUTGOING_PULL_HANDSHAKE" => new[] { "db", "pathsToSend", "destinationAcceptable" },
                "OUTGOING_MRE_WAIT" => new[] { "db" },
                "OUTGOING_MRE_RELEASED" => new[] { "db" },
                "SENDER_JUMP_AHEAD" => new[] { "db", "oldLastEtag", "newLastEtag", "destCv" },
                "SENDER_BATCH_SEND" => new[] { "db", "items", "lastEtag" },
                "SENDER_BATCH_REPLY" => new[] { "db", "replyType", "lastAcceptedCv" },
                "SENDER_ALREADY_MERGED_SKIP" => new[] { "db", "etag", "itemCv", "destCv" },
                "INCOMING_PULL_PREPROCESS" => new[] { "db", "originalCv", "rewrittenIncomingCv", "mergeCv", "dbCvBefore", "isHub", "isSink" },
                "INCOMING_DB_CV_MERGE" => new[] { "db", "incomingCv", "mergeCv", "dbCvBefore", "dbCvAfter" },
                _ => Array.Empty<string>()
            };

            var result = new List<KeyValuePair<string, string>>(fieldNames.Length);
            foreach (var fieldName in fieldNames)
            {
                foreach (var field in fields)
                {
                    if (field.Key != fieldName)
                        continue;

                    result.Add(new KeyValuePair<string, string>(field.Key, SummarizeField(field.Key, field.Value)));
                    break;
                }
            }

            return result;
        }

        private static void AppendField(StringBuilder builder, string key, string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                builder.Append("  ").Append(key).AppendLine(":");
                return;
            }

            if (ShouldRenderAsList(key, value))
            {
                builder.Append("  ").Append(key).AppendLine(":");
                foreach (var item in SplitList(value))
                    builder.Append("    - ").AppendLine(item);
                return;
            }

            builder.Append("  ").Append(key).Append(": ").AppendLine(value);
        }

        private static bool ShouldRenderAsList(string key, string value)
        {
            if (value.Contains(",") == false)
                return false;

            return key is "pathsToSend" or "destinationAcceptable" or "remoteUrls";
        }

        private static IEnumerable<string> SplitList(string value)
        {
            foreach (var part in value.Split(',', StringSplitOptions.RemoveEmptyEntries))
            {
                var trimmed = part.Trim();
                if (trimmed.Length > 0)
                    yield return trimmed;
            }
        }

        private static string DescribeEvent(string eventType)
        {
            return eventType switch
            {
                "FAULT_RULE_ARMED" => "replication fault rule was armed for a specific source to target path",
                "FAULT_RULE_MATCHED" => "replication fault rule matched a document on the outgoing path",
                "FAULT_SKIP_AND_ADVANCE" => "sender skipped the matching document while still advancing replication progress",
                "FAULT_RULE_COMPLETED" => "replication fault rule consumed all expected documents",
                "FAULT_RULE_DISPOSED" => "replication fault rule was disposed before or after completion",
                "FAULT_HEARTBEAT_SUPPRESSION_ARMED" => "heartbeat database change vector suppression was armed for a source to target path",
                "FAULT_HEARTBEAT_SUPPRESSION_DISPOSED" => "heartbeat database change vector suppression was removed for a source to target path",
                "FAULT_HEARTBEAT_SUPPRESSED" => "sender omitted the database change vector from a heartbeat on the selected path",
                "OUTGOING_MRE_WAIT" => "replication loop is blocked by the debug gate",
                "OUTGOING_MRE_RELEASED" => "replication loop was released by the debug gate",
                "OUTGOING_PULL_HANDSHAKE" => "pull replication handshake exchanged the allowed path filters",
                "PULL_SINK_REMOTE_TOPOLOGY" => "sink resolved the hub topology for the pull task",
                "PULL_SINK_TCP_INFO" => "sink resolved the TCP endpoint it will connect to",
                "SENDER_JUMP_AHEAD" => "sender advanced its last etag from the destination database change vector",
                "SENDER_BATCH_SEND" => "sender transmitted a batch to the destination",
                "SENDER_BATCH_REPLY" => "sender received the destination reply for the batch",
                "SENDER_ALREADY_MERGED_SKIP" => "sender skipped an item because the destination change vector claims it already has it",
                "INCOMING_PULL_PREPROCESS" => "incoming pull replication rewrote the incoming change vector",
                "INCOMING_DB_CV_MERGE" => "incoming replication merged the item change vector into the database change vector",
                _ => "replication trace event"
            };
        }

        private static string SummarizeField(string key, string value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return value;

            if (key.IndexOf("Cv", StringComparison.OrdinalIgnoreCase) >= 0 || key.IndexOf("changeVector", StringComparison.OrdinalIgnoreCase) >= 0)
                return SummarizeChangeVector(value);

            if (key == "items")
                return SummarizeItems(value);

            return value;
        }

        private static string SummarizeChangeVector(string value)
        {
            var entries = value.Split(',', StringSplitOptions.RemoveEmptyEntries);
            if (entries.Length == 0)
                return value;

            var compact = new List<string>(entries.Length);
            foreach (var entry in entries)
            {
                var trimmed = entry.Trim();
                var colonIndex = trimmed.IndexOf(':');
                var dashIndex = trimmed.IndexOf('-', colonIndex + 1);
                if (colonIndex <= 0 || dashIndex <= colonIndex)
                {
                    compact.Add(trimmed);
                    continue;
                }

                compact.Add(trimmed.Substring(0, dashIndex));
            }

            return string.Join(", ", compact);
        }

        private static string SummarizeItems(string value)
        {
            var items = SplitList(value).ToList();
            if (items.Count == 0)
                return value;

            if (items.Count <= 5)
                return string.Join(", ", items);

            var highlights = new List<string>();
            AddIfPresent(highlights, items, item => item.StartsWith("internal/1@", StringComparison.OrdinalIgnoreCase));
            AddIfPresent(highlights, items, item => item.StartsWith("internal/2@", StringComparison.OrdinalIgnoreCase));
            AddIfPresent(highlights, items, item => item.StartsWith("internal/10@", StringComparison.OrdinalIgnoreCase));
            AddIfPresent(highlights, items, item => item.StartsWith("internal/20@", StringComparison.OrdinalIgnoreCase));
            AddIfPresent(highlights, items, item => item.StartsWith("tickets/2@", StringComparison.OrdinalIgnoreCase));
            AddIfPresent(highlights, items, item => item.StartsWith("tickets/3@", StringComparison.OrdinalIgnoreCase));

            if (highlights.Count == 0)
            {
                highlights.Add(items[0]);
                highlights.Add(items[^1]);
            }

            return $"{items.Count} items | highlights: {string.Join(", ", highlights.Distinct(StringComparer.OrdinalIgnoreCase))}";
        }

        private static bool ContainsHighlightedBatchItems(ParsedMessage parsed)
        {
            var items = GetFieldValue(parsed.Fields, "items");
            if (string.IsNullOrWhiteSpace(items))
                return false;

            foreach (var item in SplitList(items))
            {
                var atIndex = item.IndexOf('@');
                var documentId = atIndex > 0 ? item.Substring(0, atIndex) : item;
                if (IsHighlightedDocument(documentId))
                    return true;
            }

            return false;
        }

        private static string GetFieldValue(List<KeyValuePair<string, string>> fields, string fieldName)
        {
            foreach (var field in fields)
            {
                if (field.Key == fieldName)
                    return field.Value;
            }

            return null;
        }

        private static void AddIfPresent(List<string> highlights, List<string> items, Func<string, bool> predicate)
        {
            foreach (var item in items)
            {
                if (predicate(item))
                {
                    highlights.Add(item);
                    return;
                }
            }
        }

        private static ParsedMessage ParseMessage(string message)
        {
            var sanitized = Sanitize(message);
            if (string.IsNullOrWhiteSpace(sanitized))
                return new ParsedMessage();

            var fields = new List<KeyValuePair<string, string>>();
            var firstFieldStart = FindFieldStart(sanitized, 0);
            if (firstFieldStart < 0)
                return new ParsedMessage { Subject = sanitized };

            var subject = sanitized.Substring(0, firstFieldStart).Trim();
            var index = firstFieldStart;

            while (index >= 0 && index < sanitized.Length)
            {
                var equalsIndex = sanitized.IndexOf('=', index);
                if (equalsIndex < 0)
                    break;

                var key = sanitized.Substring(index, equalsIndex - index).Trim();
                var valueStart = equalsIndex + 1;
                var nextFieldStart = FindFieldStart(sanitized, valueStart);
                var value = nextFieldStart >= 0
                    ? sanitized.Substring(valueStart, nextFieldStart - valueStart).Trim()
                    : sanitized.Substring(valueStart).Trim();

                fields.Add(new KeyValuePair<string, string>(key, value));

                if (nextFieldStart < 0)
                    break;

                index = nextFieldStart;
            }

            string documentId = null;
            foreach (var field in fields)
            {
                if (field.Key is "id" or "itemId")
                {
                    documentId = field.Value;
                    break;
                }
            }

            return new ParsedMessage
            {
                Subject = string.IsNullOrWhiteSpace(subject) ? null : subject,
                Fields = fields,
                DocumentId = documentId
            };
        }

        private static int FindFieldStart(string text, int searchStart)
        {
            for (int i = searchStart; i < text.Length; i++)
            {
                if (i > 0 && text[i - 1] != ' ')
                    continue;

                if (char.IsLetter(text[i]) == false)
                    continue;

                var j = i + 1;
                while (j < text.Length && (char.IsLetterOrDigit(text[j]) || text[j] == '_'))
                    j++;

                if (j < text.Length && text[j] == '=')
                    return i;
            }

            return -1;
        }

        private static string Sanitize(string message)
        {
            return message?
                .Replace(Environment.NewLine, " ", StringComparison.Ordinal)
                .Replace('\r', ' ')
                .Replace('\n', ' ');
        }

        private sealed class ParsedMessage
        {
            public string Subject { get; init; }

            public string DocumentId { get; init; }

            public List<KeyValuePair<string, string>> Fields { get; init; } = new();
        }
    }
}
