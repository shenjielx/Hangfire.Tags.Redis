using System;
using Hangfire.Common;
using Hangfire.Redis;
using Hangfire.States;
using Hangfire.Storage;
using StackExchange.Redis;

namespace Hangfire.Tags.Redis
{
    internal class AwaitingStateHandler : StateHandler
    {
        public AwaitingStateHandler(RedisStorageOptions options, IConnectionMultiplexer multiplexer) : base(options, multiplexer)
        {
        }

        public override void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            var tags = GetTags(context);
            var timestamp = JobHelper.ToTimestamp(DateTime.UtcNow);
            if (_useTransactions)
            {
                foreach (var item in tags)
                {
                    transaction.AddToSet(GetAwaitingKey(item), context.BackgroundJob.Id, timestamp);
                }
            }
            else
            {
                var pipeline = _database.CreateBatch();
                foreach (var item in tags)
                {
                    pipeline.SortedSetAddAsync(_prefix + GetAwaitingKey(item), context.BackgroundJob.Id, timestamp);
                }
                pipeline.Execute();
            }
        }

        public override void Unapply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            var tags = GetTags(context);
            if (_useTransactions)
            {
                foreach (var item in tags)
                {
                    transaction.RemoveFromSet(GetAwaitingKey(item), context.BackgroundJob.Id);
                }
            }
            else
            {
                var pipeline = _database.CreateBatch();
                foreach (var item in tags)
                {
                    pipeline.SortedSetRemoveAsync(_prefix + GetAwaitingKey(item), context.BackgroundJob.Id);
                }
                pipeline.Execute();
            }
        }

        public override string StateName => AwaitingState.StateName;
    }
}
