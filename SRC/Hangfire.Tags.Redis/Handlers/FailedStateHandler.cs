using System;
using Hangfire.Common;
using Hangfire.Redis;
using Hangfire.States;
using Hangfire.Storage;
using StackExchange.Redis;

namespace Hangfire.Tags.Redis
{
    internal class FailedStateHandler : StateHandler
    {
        public FailedStateHandler(RedisStorageOptions options, IConnectionMultiplexer multiplexer) : base(options, multiplexer)
        {
        }

        public override void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            var tags = GetTags(context);

            var failedState = context.NewState as FailedState;
            var timestamp = JobHelper.ToTimestamp(failedState.FailedAt);

            foreach (var item in tags)
            {
                var machineItem = $"{item}:{Environment.MachineName.ToLower()}";
                if (_useTransactions)
                {
                    transaction.AddToSet(GetFailedKey(item), context.BackgroundJob.Id, timestamp);
                    transaction.IncrementCounter(GetStatsFailedDateKey(item), TimeSpan.FromDays(30));
                    transaction.IncrementCounter(GetStatsFailedHourKey(item), TimeSpan.FromDays(7));
                    transaction.IncrementCounter(GetStatsFailedMinuteKey(item), TimeSpan.FromDays(2));
                    //Environment.MachineName
                    transaction.IncrementCounter(GetStatsFailedDateKey(machineItem), TimeSpan.FromDays(30));
                    transaction.IncrementCounter(GetStatsFailedHourKey(machineItem), TimeSpan.FromDays(7));
                    transaction.IncrementCounter(GetStatsFailedMinuteKey(machineItem), TimeSpan.FromDays(1));
                }
                else
                {
                    _database.SortedSetAddAsync(GetFailedKey(item), context.BackgroundJob.Id, timestamp);
                    IncrementCounter(GetStatsFailedDateKey(item), TimeSpan.FromDays(30));
                    IncrementCounter(GetStatsFailedHourKey(item), TimeSpan.FromDays(7));
                    IncrementCounter(GetStatsFailedMinuteKey(item), TimeSpan.FromDays(2));
                    //Environment.MachineName
                    IncrementCounter(GetStatsFailedDateKey(machineItem), TimeSpan.FromDays(30));
                    IncrementCounter(GetStatsFailedHourKey(machineItem), TimeSpan.FromDays(7));
                    IncrementCounter(GetStatsFailedMinuteKey(machineItem), TimeSpan.FromDays(1));
                }
            }
            try
            {
                //Environment.MachineName
                var machine = Environment.MachineName.ToLower();
                if (_useTransactions)
                {
                    transaction.IncrementCounter(GetStatsFailedDateKey(machine), TimeSpan.FromDays(30));
                    transaction.IncrementCounter(GetStatsFailedHourKey(machine), TimeSpan.FromDays(7));
                    transaction.IncrementCounter(GetStatsFailedMinuteKey(machine), TimeSpan.FromDays(1));
                }
                else
                {
                    IncrementCounter(GetStatsFailedDateKey(machine), TimeSpan.FromDays(30));
                    IncrementCounter(GetStatsFailedHourKey(machine), TimeSpan.FromDays(7));
                    IncrementCounter(GetStatsFailedMinuteKey(machine), TimeSpan.FromDays(1));
                }
            }
            catch
            {
            }
        }

        public override void Unapply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            var tags = GetTags(context);
            foreach (var item in tags)
            {
                if (_useTransactions)
                {
                    transaction.RemoveFromSet(GetFailedKey(item), context.BackgroundJob.Id);
                    //transaction.DecrementCounter(GetStatsFailedDateKey(item));
                    //transaction.DecrementCounter(GetStatsFailedHourKey(item));
                }
                else
                {
                    RemoveFromSet(GetFailedKey(item), context.BackgroundJob.Id);
                }
            }
        }

        public override string StateName => FailedState.StateName;
    }
}
