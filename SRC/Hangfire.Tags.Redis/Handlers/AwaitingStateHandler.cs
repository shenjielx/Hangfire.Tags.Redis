using System;
using Hangfire.Common;
using Hangfire.Redis;
using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Tags.Redis
{
    internal class AwaitingStateHandler : StateHandler
    {
        public AwaitingStateHandler(RedisStorageOptions options) : base(options)
        {
        }

        public override void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            var tags = GetTags(context);

            foreach (var item in tags)
            {
                transaction.AddToSet(GetAwaitingKey(item), context.BackgroundJob.Id, JobHelper.ToTimestamp(DateTime.UtcNow));
            }
        }

        public override void Unapply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            var tags = GetTags(context);
            foreach (var item in tags)
            {
                transaction.RemoveFromSet(GetAwaitingKey(item), context.BackgroundJob.Id);
            }
        }

        public override string StateName => AwaitingState.StateName;
    }
}
