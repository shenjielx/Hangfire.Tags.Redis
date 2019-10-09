using Hangfire;
using Hangfire.Common;
using Hangfire.Redis;
using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Tags.Redis
{
    internal class ProcessingStateHandler : StateHandler
    {
        public ProcessingStateHandler(RedisStorageOptions options) : base(options)
        {
        }

        public override void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            var tags = GetTags(context);

            var processingState = context.NewState as ProcessingState;
            var timestamp = JobHelper.ToTimestamp(processingState.StartedAt);

            foreach (var item in tags)
            {
                transaction.AddToSet(GetProcessingKey(item), context.BackgroundJob.Id, timestamp);
            }
        }

        public override void Unapply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            var tags = GetTags(context);
            foreach (var item in tags)
            {
                transaction.RemoveFromSet(GetProcessingKey(item), context.BackgroundJob.Id);
            }
        }

        public override string StateName => ProcessingState.StateName;
    }
}
