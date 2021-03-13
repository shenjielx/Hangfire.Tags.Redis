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

            if (_useTransactions)
            {
                foreach (var item in tags)
                {
                    transaction.AddToSet(GetProcessingKey(item), context.BackgroundJob.Id, timestamp);
                }
            }
            else
            {
                var pipeline = _database.CreateBatch();
                foreach (var item in tags)
                {
                    AddToSet(pipeline, _prefix + GetProcessingKey(item), context.BackgroundJob.Id, timestamp);
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
                    transaction.RemoveFromSet(GetProcessingKey(item), context.BackgroundJob.Id);
                }
            }
            else
            {
                var pipeline = _database.CreateBatch();
                foreach (var item in tags)
                {
                    RemoveFromSet(pipeline, _prefix + GetProcessingKey(item), context.BackgroundJob.Id);
                }
                pipeline.Execute();
            }
        }

        public override string StateName => ProcessingState.StateName;
    }
}
