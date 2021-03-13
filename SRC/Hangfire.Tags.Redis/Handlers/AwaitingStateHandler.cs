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

            foreach (var item in tags)
            {
                if (_useTransactions)
                {
                    transaction.AddToSet(GetAwaitingKey(item), context.BackgroundJob.Id, JobHelper.ToTimestamp(DateTime.UtcNow));
                }
                else
                {
                    _database.SortedSetAddAsync(_prefix + GetAwaitingKey(item), context.BackgroundJob.Id, JobHelper.ToTimestamp(DateTime.UtcNow));
                }
            }
        }

        public override void Unapply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            var tags = GetTags(context);
            foreach (var item in tags)
            {
                if (_useTransactions)
                {
                    transaction.RemoveFromSet(GetAwaitingKey(item), context.BackgroundJob.Id);
                }
                else
                {
                    _database.SortedSetRemoveAsync(_prefix + GetAwaitingKey(item), context.BackgroundJob.Id);
                }
            }
        }

        public override string StateName => AwaitingState.StateName;
    }
}
