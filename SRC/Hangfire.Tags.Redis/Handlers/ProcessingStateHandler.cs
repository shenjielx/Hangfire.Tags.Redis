using Hangfire;
using Hangfire.Common;
using Hangfire.Redis;
using Hangfire.States;
using Hangfire.Storage;
using StackExchange.Redis;

namespace Hangfire.Tags.Redis
{
    internal class ProcessingStateHandler : StateHandler
    {
        public ProcessingStateHandler(RedisStorageOptions options, IConnectionMultiplexer multiplexer) : base(options, multiplexer)
        {
        }

        public override void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            var tags = GetTags(context);

            var processingState = context.NewState as ProcessingState;
            var timestamp = JobHelper.ToTimestamp(processingState.StartedAt);

            foreach (var item in tags)
            {
                if (_useTransactions)
                {
                    transaction.AddToSet(GetProcessingKey(item), context.BackgroundJob.Id, timestamp);
                }
                else
                {
                    AddToSet(GetProcessingKey(item), context.BackgroundJob.Id, timestamp);
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
                    transaction.RemoveFromSet(GetProcessingKey(item), context.BackgroundJob.Id);
                }
                else
                {
                    RemoveFromSet(GetProcessingKey(item), context.BackgroundJob.Id);
                }
            }
        }

        public override string StateName => ProcessingState.StateName;
    }
}
