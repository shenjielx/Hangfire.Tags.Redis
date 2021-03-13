using Hangfire.Redis;
using Hangfire.States;
using Hangfire.Storage;
using StackExchange.Redis;

namespace Hangfire.Tags.Redis
{
    internal class DeletedStateHandler : StateHandler
    {
        public DeletedStateHandler(RedisStorageOptions options, IConnectionMultiplexer multiplexer) : base(options, multiplexer)
        {
        }

        public override void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            var tags = GetTags(context);
            var storage = context.Storage as RedisStorage;

            if (_useTransactions)
            {
                foreach (var item in tags)
                {
                    transaction.InsertToList(GetDeletedKey(item), context.BackgroundJob.Id);
                    transaction.IncrementCounter(GetStatsDeletedKey(item));

                    if (storage != null && SucceededListSize > 0)
                    {
                        transaction.TrimList(GetDeletedKey(item), 0, SucceededListSize);
                    }
                }
            }
            else
            {
                var pipeline = _database.CreateBatch();
                foreach (var item in tags)
                {
                    InsertToList(pipeline, _prefix + GetDeletedKey(item), context.BackgroundJob.Id);
                    IncrementCounter(pipeline, _prefix + GetStatsDeletedKey(item));

                    if (storage != null && SucceededListSize > 0)
                    {
                        TrimList(pipeline, _prefix + GetDeletedKey(item), 0, SucceededListSize);
                    }
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
                    transaction.RemoveFromList(GetDeletedKey(item), context.BackgroundJob.Id);
                    transaction.DecrementCounter(GetStatsDeletedKey(item));
                }
            }
            else
            {
                var pipeline = _database.CreateBatch();
                foreach (var item in tags)
                {
                    RemoveFromList(pipeline, _prefix + GetDeletedKey(item), context.BackgroundJob.Id);
                    DecrementCounter(pipeline, _prefix + GetStatsDeletedKey(item));
                }
                pipeline.Execute();
            }
        }

        public override string StateName => DeletedState.StateName;
    }
}
