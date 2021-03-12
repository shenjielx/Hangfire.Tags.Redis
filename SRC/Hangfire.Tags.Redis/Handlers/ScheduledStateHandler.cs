using Hangfire.Common;
using Hangfire.Redis;
using Hangfire.States;
using Hangfire.Storage;
using StackExchange.Redis;

namespace Hangfire.Tags.Redis
{
    internal class ScheduledStateHandler : StateHandler
    {
        public ScheduledStateHandler(RedisStorageOptions options, IConnectionMultiplexer multiplexer) : base(options, multiplexer)
        {
        }

        public override void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            var tags = GetTags(context);

            var scheduledState = context.NewState as ScheduledState;
            var timestamp = JobHelper.ToTimestamp(scheduledState.EnqueueAt);

            foreach (var item in tags)
            {
                if (_useTransactions)
                {
                    transaction.AddToSet(GetScheduledKey(item), context.BackgroundJob.Id, timestamp);
                }
                else
                {
                    AddToSet(GetScheduledKey(item), context.BackgroundJob.Id, timestamp);
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
                    transaction.RemoveFromSet(GetScheduledKey(item), context.BackgroundJob.Id);
                }
                else
                {
                    RemoveFromSet(GetScheduledKey(item), context.BackgroundJob.Id);
                }
            }
        }

        public override string StateName => ScheduledState.StateName;
    }
}
