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

            if (_useTransactions)
            {
                foreach (var item in tags)
                {
                    transaction.AddToSet(GetScheduledKey(item), context.BackgroundJob.Id, timestamp);
                }
            }
            else
            {
                var pipeline = _database.CreateBatch();
                foreach (var item in tags)
                {
                    AddToSet(pipeline, _prefix + GetScheduledKey(item), context.BackgroundJob.Id, timestamp);
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
                    transaction.RemoveFromSet(GetScheduledKey(item), context.BackgroundJob.Id);
                }
            }
            else
            {
                var pipeline = _database.CreateBatch();
                foreach (var item in tags)
                {
                    RemoveFromSet(pipeline, _prefix + GetScheduledKey(item), context.BackgroundJob.Id);
                }
                pipeline.Execute();
            }
        }

        public override string StateName => ScheduledState.StateName;
    }
}
