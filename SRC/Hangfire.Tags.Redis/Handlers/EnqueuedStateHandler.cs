using Hangfire.Common;
using Hangfire.Redis;
using Hangfire.States;
using Hangfire.Storage;
using StackExchange.Redis;

namespace Hangfire.Tags.Redis
{
    internal class EnqueuedStateHandler : StateHandler
    {
        public EnqueuedStateHandler(RedisStorageOptions options, IConnectionMultiplexer multiplexer) : base(options, multiplexer)
        {
        }

        public override void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            var tags = GetTags(context);

            var enqueuedState = context.NewState as EnqueuedState;
            var timestamp = JobHelper.ToTimestamp(enqueuedState.EnqueuedAt);

            if (_useTransactions)
            {
                foreach (var item in tags)
                {
                    transaction.AddToSet(_prefix + GetEnqueuedKey(item), context.BackgroundJob.Id, timestamp);
                }
            }
            else
            {
                var pipeline = _database.CreateBatch();
                foreach (var item in tags)
                {
                    AddToSet(pipeline, _prefix + GetEnqueuedKey(item), context.BackgroundJob.Id, timestamp);
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
                    transaction.RemoveFromSet(GetEnqueuedKey(item), context.BackgroundJob.Id);
                }
            }
            else
            {
                var pipeline = _database.CreateBatch();
                foreach (var item in tags)
                {
                    RemoveFromSet(pipeline, _prefix + GetEnqueuedKey(item), context.BackgroundJob.Id);
                }
                pipeline.Execute();
            }
        }

        public override string StateName => EnqueuedState.StateName;
    }
}
