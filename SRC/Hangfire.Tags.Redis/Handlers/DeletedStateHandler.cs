using Hangfire.Redis;
using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Tags.Redis
{
    internal class DeletedStateHandler : StateHandler
    {
        public DeletedStateHandler(RedisStorageOptions options) : base(options)
        {
        }

        public override void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            var tags = GetTags(context);
            var storage = context.Storage as RedisStorage;

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

        public override void Unapply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            var tags = GetTags(context);
            foreach (var item in tags)
            {
                transaction.RemoveFromList(GetDeletedKey(item), context.BackgroundJob.Id);
                transaction.DecrementCounter(GetStatsDeletedKey(item));
            }
        }

        public override string StateName => DeletedState.StateName;
    }
}
