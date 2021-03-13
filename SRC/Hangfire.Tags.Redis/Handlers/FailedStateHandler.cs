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
                    transaction.IncrementCounter(_prefix + GetStatsFailedDateKey(item), TimeSpan.FromDays(30));
                    transaction.IncrementCounter(_prefix + GetStatsFailedHourKey(item), TimeSpan.FromDays(7));
                    transaction.IncrementCounter(_prefix + GetStatsFailedMinuteKey(item), TimeSpan.FromDays(2));
                    //Environment.MachineName
                    transaction.IncrementCounter(_prefix + GetStatsFailedDateKey(machineItem), TimeSpan.FromDays(30));
                    transaction.IncrementCounter(_prefix + GetStatsFailedHourKey(machineItem), TimeSpan.FromDays(7));
                    transaction.IncrementCounter(_prefix + GetStatsFailedMinuteKey(machineItem), TimeSpan.FromDays(1));
                }
                else
                {
                    AddToSet(_prefix + GetFailedKey(item), context.BackgroundJob.Id, timestamp);
                    IncrementCounter(_prefix + GetStatsFailedDateKey(item), TimeSpan.FromDays(30));
                    IncrementCounter(_prefix + GetStatsFailedHourKey(item), TimeSpan.FromDays(7));
                    IncrementCounter(_prefix + GetStatsFailedMinuteKey(item), TimeSpan.FromDays(2));
                    //Environment.MachineName
                    IncrementCounter(_prefix + GetStatsFailedDateKey(machineItem), TimeSpan.FromDays(30));
                    IncrementCounter(_prefix + GetStatsFailedHourKey(machineItem), TimeSpan.FromDays(7));
                    IncrementCounter(_prefix + GetStatsFailedMinuteKey(machineItem), TimeSpan.FromDays(1));
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
                    IncrementCounter(_prefix + GetStatsFailedDateKey(machine), TimeSpan.FromDays(30));
                    IncrementCounter(_prefix + GetStatsFailedHourKey(machine), TimeSpan.FromDays(7));
                    IncrementCounter(_prefix + GetStatsFailedMinuteKey(machine), TimeSpan.FromDays(1));
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
                    RemoveFromSet(_prefix + GetFailedKey(item), context.BackgroundJob.Id);
                }
            }
        }

        public override string StateName => FailedState.StateName;
    }
}
