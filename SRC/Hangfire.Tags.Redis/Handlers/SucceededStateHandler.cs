using System;
using Hangfire.Redis;
using Hangfire.States;
using Hangfire.Storage;
using StackExchange.Redis;

namespace Hangfire.Tags.Redis
{
    internal class SucceededStateHandler : StateHandler
    {
        public SucceededStateHandler(RedisStorageOptions options, IConnectionMultiplexer multiplexer) : base(options, multiplexer)
        {
        }

        public override void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            var tags = GetTags(context);
            var storage = context.Storage as RedisStorage;

            foreach (var item in tags)
            {
                var machineItem = $"{item}:{Environment.MachineName.ToLower()}";
                if (_useTransactions)
                {
                    transaction.InsertToList(GetSucceededKey(item), context.BackgroundJob.Id);
                    transaction.IncrementCounter(GetStatsSucceededKey(item));
                    transaction.IncrementCounter(GetStatsSucceededDateKey(item), TimeSpan.FromDays(30));
                    transaction.IncrementCounter(GetStatsSucceededHourKey(item), TimeSpan.FromDays(7));
                    transaction.IncrementCounter(GetStatsSucceededMinuteKey(item), TimeSpan.FromDays(2));
                    //Environment.MachineName
                    transaction.IncrementCounter(GetStatsSucceededDateKey(machineItem), TimeSpan.FromDays(30));
                    transaction.IncrementCounter(GetStatsSucceededHourKey(machineItem), TimeSpan.FromDays(7));
                    transaction.IncrementCounter(GetStatsSucceededMinuteKey(machineItem), TimeSpan.FromDays(1));

                    if (storage != null && SucceededListSize > 0)
                    {
                        transaction.TrimList(GetSucceededKey(item), 0, SucceededListSize);
                    }
                }
                else
                {
                    InsertToList(_prefix + GetSucceededKey(item), context.BackgroundJob.Id);
                    IncrementCounter(_prefix + GetStatsSucceededKey(item));
                    IncrementCounter(_prefix + GetStatsSucceededDateKey(item), TimeSpan.FromDays(30));
                    IncrementCounter(_prefix + GetStatsSucceededHourKey(item), TimeSpan.FromDays(7));
                    IncrementCounter(_prefix + GetStatsSucceededMinuteKey(item), TimeSpan.FromDays(2));
                    //Environment.MachineName
                    IncrementCounter(_prefix + GetStatsSucceededDateKey(machineItem), TimeSpan.FromDays(30));
                    IncrementCounter(_prefix + GetStatsSucceededHourKey(machineItem), TimeSpan.FromDays(7));
                    IncrementCounter(_prefix + GetStatsSucceededMinuteKey(machineItem), TimeSpan.FromDays(1));

                    if (storage != null && SucceededListSize > 0)
                    {
                        TrimList(_prefix + GetSucceededKey(item), 0, SucceededListSize);
                    }
                }
            }
            try
            {
                //Environment.MachineName
                var machine = Environment.MachineName.ToLower();
                if (_useTransactions)
                {
                    transaction.IncrementCounter(GetStatsSucceededDateKey(machine), TimeSpan.FromDays(30));
                    transaction.IncrementCounter(GetStatsSucceededHourKey(machine), TimeSpan.FromDays(7));
                    transaction.IncrementCounter(GetStatsSucceededMinuteKey(machine), TimeSpan.FromDays(1));
                }
                else
                {
                    IncrementCounter(_prefix + GetStatsSucceededDateKey(machine), TimeSpan.FromDays(30));
                    IncrementCounter(_prefix + GetStatsSucceededHourKey(machine), TimeSpan.FromDays(7));
                    IncrementCounter(_prefix + GetStatsSucceededMinuteKey(machine), TimeSpan.FromDays(1));
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
                    transaction.RemoveFromList(GetSucceededKey(item), context.BackgroundJob.Id);
                    transaction.DecrementCounter(GetStatsSucceededKey(item));
                    //transaction.DecrementCounter(GetStatsSucceededDateKey(item));
                    //transaction.DecrementCounter(GetStatsSucceededHourKey(item));
                }
                else
                {
                    RemoveFromList(_prefix + GetSucceededKey(item), context.BackgroundJob.Id);
                    DecrementCounter(_prefix + GetStatsSucceededKey(item));
                }
            }
        }

        public override string StateName => SucceededState.StateName;
    }
}
