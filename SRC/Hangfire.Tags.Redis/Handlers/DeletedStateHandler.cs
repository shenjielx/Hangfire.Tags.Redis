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

            foreach (var item in tags)
            {
                if (_useTransactions)
                {
                    transaction.InsertToList(GetDeletedKey(item), context.BackgroundJob.Id);
                    transaction.IncrementCounter(GetStatsDeletedKey(item));

                    if (storage != null && SucceededListSize > 0)
                    {
                        transaction.TrimList(GetDeletedKey(item), 0, SucceededListSize);
                    }
                }
                else
                {
                    InsertToList(GetDeletedKey(item), context.BackgroundJob.Id);
                    IncrementCounter(GetStatsDeletedKey(item));

                    if (storage != null && SucceededListSize > 0)
                    {
                        TrimList(GetDeletedKey(item), 0, SucceededListSize);
                    }
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
                    transaction.RemoveFromList(GetDeletedKey(item), context.BackgroundJob.Id);
                    transaction.DecrementCounter(GetStatsDeletedKey(item));
                }
                else
                {
                    RemoveFromList(GetDeletedKey(item), context.BackgroundJob.Id);
                    DecrementCounter(GetStatsDeletedKey(item));
                }
            }
        }

        public override string StateName => DeletedState.StateName;
    }
}
