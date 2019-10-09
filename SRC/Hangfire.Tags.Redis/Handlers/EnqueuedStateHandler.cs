using Hangfire.Common;
using Hangfire.Redis;
using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Tags.Redis
{
    internal class EnqueuedStateHandler : StateHandler
    {
        public EnqueuedStateHandler(RedisStorageOptions options) : base(options)
        {
        }

        public override void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            var tags = GetTags(context);

            var enqueuedState = context.NewState as EnqueuedState;
            var timestamp = JobHelper.ToTimestamp(enqueuedState.EnqueuedAt);

            foreach (var item in tags)
            {
                transaction.AddToSet(GetEnqueuedKey(item), context.BackgroundJob.Id, timestamp);
            }
        }

        public override void Unapply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            var tags = GetTags(context);
            foreach (var item in tags)
            {
                transaction.RemoveFromSet(GetEnqueuedKey(item), context.BackgroundJob.Id);
            }
        }

        public override string StateName => EnqueuedState.StateName;
    }
}
