using System;
using Hangfire.Common;
using Hangfire.Redis;
using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Tags.Redis
{
    internal class FailedStateHandler : StateHandler
    {
        public FailedStateHandler(RedisStorageOptions options) : base(options)
        {
        }

        public override void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            var tags = GetTags(context);

            var failedState = context.NewState as FailedState;
            var timestamp = JobHelper.ToTimestamp(failedState.FailedAt);

            foreach (var item in tags)
            {
                transaction.AddToSet(GetFailedKey(item), context.BackgroundJob.Id, timestamp);
                transaction.IncrementCounter(GetStatsFailedDateKey(item), TimeSpan.FromDays(30));
                transaction.IncrementCounter(GetStatsFailedHourKey(item), TimeSpan.FromDays(1));
            }
        }

        public override void Unapply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            var tags = GetTags(context);
            foreach (var item in tags)
            {
                transaction.RemoveFromSet(GetFailedKey(item), context.BackgroundJob.Id);
                transaction.DecrementCounter(GetStatsFailedDateKey(item));
                transaction.DecrementCounter(GetStatsFailedHourKey(item));
            }
        }

        public override string StateName => FailedState.StateName;
    }
}
