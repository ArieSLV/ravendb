using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Raven.Server.Documents.Replication
{
    public enum ReplicationFaultDecision
    {
        None,
        SkipAndAdvance
    }

    public sealed class ReplicationFaultEvent
    {
        public string EventType { get; init; }

        public string RuleId { get; init; }

        public string Label { get; init; }

        public string DatabaseName { get; init; }

        public string SourceNodeTag { get; init; }

        public string TargetNodeTag { get; init; }

        public string DocumentId { get; init; }

        public long? Etag { get; init; }

        public string ChangeVector { get; init; }

        public int ExpectedMatches { get; init; }

        public int MatchedMatches { get; init; }

        public ReplicationFaultDecision Decision { get; init; }
    }

    public sealed class ReplicationFaultRule
    {
        internal ReplicationFaultRule(
            string id,
            string label,
            string databaseName,
            string sourceNodeTag,
            string targetNodeTag,
            long minEtagExclusive,
            HashSet<string> remainingDocumentIds)
        {
            Id = id;
            Label = label;
            DatabaseName = databaseName;
            SourceNodeTag = sourceNodeTag;
            TargetNodeTag = targetNodeTag;
            MinEtagExclusive = minEtagExclusive;
            RemainingDocumentIds = remainingDocumentIds;
            ExpectedMatches = remainingDocumentIds.Count;
        }

        public string Id { get; }

        public string Label { get; }

        public string DatabaseName { get; }

        public string SourceNodeTag { get; }

        public string TargetNodeTag { get; }

        public long MinEtagExclusive { get; }

        public ReplicationFaultDecision Decision => ReplicationFaultDecision.SkipAndAdvance;

        public int ExpectedMatches { get; }

        public int MatchedMatches { get; internal set; }

        internal HashSet<string> RemainingDocumentIds { get; }

        internal TaskCompletionSource<object> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public sealed class ReplicationFaultRuleHandle : IDisposable
    {
        private readonly ReplicationFaultController _controller;
        private readonly ReplicationFaultRule _rule;
        private bool _disposed;

        internal ReplicationFaultRuleHandle(ReplicationFaultController controller, ReplicationFaultRule rule)
        {
            _controller = controller;
            _rule = rule;
        }

        public string RuleId => _rule.Id;

        public string Label => _rule.Label;

        public int ExpectedMatches => _rule.ExpectedMatches;

        public int MatchedMatches => _rule.MatchedMatches;

        public bool IsCompleted => _rule.Completion.Task.IsCompleted;

        public Task WaitForCompletionAsync()
        {
            return _rule.Completion.Task;
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
            _controller.DisposeRule(_rule);
        }
    }

    public sealed class ReplicationHeartbeatSuppressionHandle : IDisposable
    {
        private readonly ReplicationFaultController _controller;
        private readonly string _key;
        private readonly string _databaseName;
        private readonly string _sourceNodeTag;
        private readonly string _targetNodeTag;
        private readonly string _label;
        private bool _disposed;

        internal ReplicationHeartbeatSuppressionHandle(ReplicationFaultController controller, string key, string databaseName, string sourceNodeTag, string targetNodeTag, string label)
        {
            _controller = controller;
            _key = key;
            _databaseName = databaseName;
            _sourceNodeTag = sourceNodeTag;
            _targetNodeTag = targetNodeTag;
            _label = label;
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
            _controller.DisposeHeartbeatSuppression(_key, _databaseName, _sourceNodeTag, _targetNodeTag, _label);
        }
    }

    public sealed class ReplicationFaultController
    {
        private readonly ConcurrentDictionary<string, ReplicationFaultRule> _rules = new(StringComparer.OrdinalIgnoreCase);
        private readonly ConcurrentDictionary<string, int> _suppressedHeartbeatLinks = new(StringComparer.OrdinalIgnoreCase);

        public event Action<ReplicationFaultEvent> EventEmitted;

        public ReplicationFaultRuleHandle ArmSkipAndAdvance(
            string databaseName,
            string sourceNodeTag,
            string targetNodeTag,
            long minEtagExclusive,
            IEnumerable<string> documentIds,
            string label)
        {
            var remainingDocumentIds = documentIds
                .Where(id => string.IsNullOrWhiteSpace(id) == false)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var rule = new ReplicationFaultRule(
                Guid.NewGuid().ToString("N"),
                label,
                databaseName,
                sourceNodeTag,
                targetNodeTag,
                minEtagExclusive,
                remainingDocumentIds);

            _rules[rule.Id] = rule;
            Emit(rule, "FAULT_RULE_ARMED");
            return new ReplicationFaultRuleHandle(this, rule);
        }

        public ReplicationHeartbeatSuppressionHandle ArmHeartbeatSuppression(
            string databaseName,
            string sourceNodeTag,
            string targetNodeTag,
            string label)
        {
            var key = CreateHeartbeatKey(databaseName, sourceNodeTag, targetNodeTag);
            _suppressedHeartbeatLinks.AddOrUpdate(key, 1, (_, current) => current + 1);

            ReplicationInvestigationTrace.Write(
                "FAULT_HEARTBEAT_SUPPRESSION_ARMED",
                $"db={databaseName} label={label} source={sourceNodeTag} target={targetNodeTag}",
                null);

            return new ReplicationHeartbeatSuppressionHandle(this, key, databaseName, sourceNodeTag, targetNodeTag, label);
        }

        public bool ShouldSuppressHeartbeatDatabaseChangeVector(string databaseName, string sourceNodeTag, string targetNodeTag)
        {
            if (string.IsNullOrWhiteSpace(databaseName) || string.IsNullOrWhiteSpace(sourceNodeTag) || string.IsNullOrWhiteSpace(targetNodeTag))
                return false;

            return _suppressedHeartbeatLinks.ContainsKey(CreateHeartbeatKey(databaseName, sourceNodeTag, targetNodeTag));
        }

        public ReplicationFaultDecision TryGetDecision(
            string databaseName,
            string sourceNodeTag,
            string targetNodeTag,
            string documentId,
            long etag,
            string changeVector)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                return ReplicationFaultDecision.None;

            foreach (var rule in _rules.Values)
            {
                if (string.Equals(rule.DatabaseName, databaseName, StringComparison.OrdinalIgnoreCase) == false)
                    continue;

                if (string.Equals(rule.SourceNodeTag, sourceNodeTag, StringComparison.OrdinalIgnoreCase) == false)
                    continue;

                if (string.Equals(rule.TargetNodeTag, targetNodeTag, StringComparison.OrdinalIgnoreCase) == false)
                    continue;

                lock (rule)
                {
                    if (etag <= rule.MinEtagExclusive)
                        continue;

                    if (rule.RemainingDocumentIds.Remove(documentId) == false)
                        continue;

                    rule.MatchedMatches++;
                    Emit(rule, "FAULT_RULE_MATCHED", documentId, etag, changeVector);
                    Emit(rule, "FAULT_SKIP_AND_ADVANCE", documentId, etag, changeVector);

                    if (rule.RemainingDocumentIds.Count == 0)
                    {
                        _rules.TryRemove(rule.Id, out _);
                        Emit(rule, "FAULT_RULE_COMPLETED", documentId, etag, changeVector);
                        rule.Completion.TrySetResult(null);
                    }

                    return rule.Decision;
                }
            }

            return ReplicationFaultDecision.None;
        }

        internal void DisposeRule(ReplicationFaultRule rule)
        {
            if (_rules.TryRemove(rule.Id, out _) == false)
                return;

            Emit(rule, "FAULT_RULE_DISPOSED");
            rule.Completion.TrySetCanceled();
        }

        internal void DisposeHeartbeatSuppression(string key, string databaseName, string sourceNodeTag, string targetNodeTag, string label)
        {
            while (true)
            {
                if (_suppressedHeartbeatLinks.TryGetValue(key, out var current) == false)
                    break;

                if (current <= 1)
                {
                    if (_suppressedHeartbeatLinks.TryRemove(key, out _))
                        break;

                    continue;
                }

                if (_suppressedHeartbeatLinks.TryUpdate(key, current - 1, current))
                    break;
            }

            ReplicationInvestigationTrace.Write(
                "FAULT_HEARTBEAT_SUPPRESSION_DISPOSED",
                $"db={databaseName} label={label} source={sourceNodeTag} target={targetNodeTag}",
                null);
        }

        private static string CreateHeartbeatKey(string databaseName, string sourceNodeTag, string targetNodeTag)
        {
            return $"{databaseName}|{sourceNodeTag}|{targetNodeTag}";
        }

        private void Emit(
            ReplicationFaultRule rule,
            string eventType,
            string documentId = null,
            long? etag = null,
            string changeVector = null)
        {
            var faultEvent = new ReplicationFaultEvent
            {
                EventType = eventType,
                RuleId = rule.Id,
                Label = rule.Label,
                DatabaseName = rule.DatabaseName,
                SourceNodeTag = rule.SourceNodeTag,
                TargetNodeTag = rule.TargetNodeTag,
                DocumentId = documentId,
                Etag = etag,
                ChangeVector = changeVector,
                ExpectedMatches = rule.ExpectedMatches,
                MatchedMatches = rule.MatchedMatches,
                Decision = rule.Decision
            };

            ReplicationInvestigationTrace.Write(eventType,
                $"db={rule.DatabaseName} ruleId={rule.Id} label={rule.Label} source={rule.SourceNodeTag} target={rule.TargetNodeTag} minEtagExclusive={rule.MinEtagExclusive} id={documentId} etag={etag} matched={rule.MatchedMatches} expected={rule.ExpectedMatches} decision={rule.Decision} cv={changeVector}",
                documentId);

            EventEmitted?.Invoke(faultEvent);
        }
    }
}
