using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Commands
{
    public class GetBackupHistoryNodeCommand : RavenCommand<NodeBackupHistoryResult>
    {
        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/backup-history?url={node.Url}";
            
            return new HttpRequestMessage(HttpMethod.Get, url);
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = JsonDeserializationClient.NodeBackupHistoryResult(response);
        }
    }

    public class NodeBackupHistoryResult
    {
        public List<BackupHistoryEntry> Result;

    }




    public class BackupHistoryEntry
    {

        public string BackupName { get; set; }
        public BackupType? BackupType { get; set; }
        public DateTime CreatedAt { get; private set; }
        public string DatabaseName { get; set; }
        public long? DurationInMs { get; set; }
        public string Error { get; set; } 
        public bool IsCompletedSuccessfully { get; set; }
        public bool? IsFull { get; set; }
        public string NodeTag { get; set; }

        public BackupHistoryEntry()
        {
            CreatedAt = SystemTime.UtcNow;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(BackupName)] = BackupName,
                [nameof(BackupType)] = BackupType,
                [nameof(CreatedAt)] = CreatedAt,
                [nameof(DatabaseName)] = DatabaseName,
                [nameof(DurationInMs)] = DurationInMs,
                [nameof(Error)] = Error,
                [nameof(IsCompletedSuccessfully)] = IsCompletedSuccessfully,
                [nameof(IsFull)] = IsFull,
                [nameof(NodeTag)] = NodeTag,
            };
        }

        public static string Prefix => "backup-history/";

        public static string GenerateItemName(string databaseName, long? id)
        {
            return $"values/{databaseName}/{Prefix}{id}";
        }
    }

















    public class GetTcpInfoCommand : RavenCommand<TcpConnectionInfo>
    {
        private readonly string _tag;
        private readonly string _dbName;
        private readonly string _dbId;
        private readonly long _etag;
        private readonly bool _fromReplication;

        public GetTcpInfoCommand(string tag)
        {
            _tag = tag;
            Timeout = TimeSpan.FromSeconds(15);
        }

        public GetTcpInfoCommand(string tag, string dbName = null) : this(tag)
        {
            _dbName = dbName;
        }

        internal GetTcpInfoCommand(string tag, string dbName, string dbId, long etag) : this(tag, dbName)
        {
            _dbId = dbId;
            _etag = etag;
            _fromReplication = true;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            if (string.IsNullOrEmpty(_dbName))
            {
                url = $"{node.Url}/info/tcp?tag={_tag}";
            }
            else
            {
                url = $"{node.Url}/databases/{_dbName}/info/tcp?tag={_tag}";
                if (_fromReplication)
                {
                    url += $"&from-outgoing={_dbId}&etag={_etag}";
                }
            }
            RequestedNode = node;
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = JsonDeserializationClient.TcpConnectionInfo(response);
        }

        public ServerNode RequestedNode { get; private set; }

        public override bool IsReadRequest => true;   
    }
}
