using Hangfire.Common;
using Hangfire.Redis;
using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Tags.Redis
{
    internal class ScheduledStateHandler : StateHandler
    {
        public ScheduledStateHandler(RedisStorageOptions options) : base(options)
        {
        }

        public override void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            var tags = GetTags(context);

            var scheduledState = context.NewState as ScheduledState;
            var timestamp = JobHelper.ToTimestamp(scheduledState.EnqueueAt);

            foreach (var item in tags)
            {
                transaction.AddToSet(GetScheduledKey(item), context.BackgroundJob.Id, timestamp);
            }
        }

        public override void Unapply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            var tags = GetTags(context);
            foreach (var item in tags)
            {
                transaction.RemoveFromSet(GetScheduledKey(item), context.BackgroundJob.Id);
            }
        }

        public override string StateName => ScheduledState.StateName;
    }
}
