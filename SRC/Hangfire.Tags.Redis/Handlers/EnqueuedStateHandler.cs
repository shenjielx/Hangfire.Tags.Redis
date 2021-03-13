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

            foreach (var item in tags)
            {
                if (_useTransactions)
                {
                    transaction.AddToSet(_prefix + GetEnqueuedKey(item), context.BackgroundJob.Id, timestamp);
                }
                else
                {
                    AddToSet(_prefix + GetEnqueuedKey(item), context.BackgroundJob.Id, timestamp);
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
                    transaction.RemoveFromSet(GetEnqueuedKey(item), context.BackgroundJob.Id);
                }
                else
                {
                    RemoveFromSet(_prefix + GetEnqueuedKey(item), context.BackgroundJob.Id);
                }
            }
        }

        public override string StateName => EnqueuedState.StateName;
    }
}
