﻿using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.ServerWide.Commands.Sharding;

namespace Raven.Server.ServerWide
{
    public class ShardingStore
    {
        private readonly ServerStore _server;
        public bool ManualMigration = false;

        public ShardingStore(ServerStore server)
        {
            _server = server;
        }
        
        public Task<(long Index, object Result)> StartBucketMigration(string database, int bucket, int fromShard, int toShard, string raftId = null)
        {
            var cmd = new StartBucketMigrationCommand(bucket, fromShard, toShard, database, raftId ?? RaftIdGenerator.NewId());
            return _server.SendToLeaderAsync(cmd);
        }

        public Task<(long Index, object Result)> SourceMigrationCompleted(string database, int bucket, long migrationIndex, string lastChangeVector, string raftId = null)
        {
            var cmd = new SourceMigrationSendCompletedCommand(bucket, migrationIndex, lastChangeVector, database, raftId ?? RaftIdGenerator.NewId());
            return _server.SendToLeaderAsync(cmd);
        }

        public Task<(long Index, object Result)> DestinationMigrationConfirm(string database, int bucket, long migrationIndex, string raftId = null)
        {
            var cmd = new DestinationMigrationConfirmCommand(bucket, migrationIndex, _server.NodeTag, database, raftId ?? RaftIdGenerator.NewId());
            return _server.SendToLeaderAsync(cmd);
        }

        public Task<(long Index, object Result)> SourceMigrationCleanup(string database, int bucket, long migrationIndex, string raftId = null)
        {
            var cmd = new SourceMigrationCleanupCommand(bucket, migrationIndex, _server.NodeTag, database, raftId ?? RaftIdGenerator.NewId());
            return _server.SendToLeaderAsync(cmd);
        }
    }
}