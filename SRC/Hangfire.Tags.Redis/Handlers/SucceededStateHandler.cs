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
                    InsertToList(GetSucceededKey(item), context.BackgroundJob.Id);
                    IncrementCounter(GetStatsSucceededKey(item));
                    IncrementCounter(GetStatsSucceededDateKey(item), TimeSpan.FromDays(30));
                    IncrementCounter(GetStatsSucceededHourKey(item), TimeSpan.FromDays(7));
                    IncrementCounter(GetStatsSucceededMinuteKey(item), TimeSpan.FromDays(2));
                    //Environment.MachineName
                    IncrementCounter(GetStatsSucceededDateKey(machineItem), TimeSpan.FromDays(30));
                    IncrementCounter(GetStatsSucceededHourKey(machineItem), TimeSpan.FromDays(7));
                    IncrementCounter(GetStatsSucceededMinuteKey(machineItem), TimeSpan.FromDays(1));

                    if (storage != null && SucceededListSize > 0)
                    {
                        TrimList(GetSucceededKey(item), 0, SucceededListSize);
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
                    IncrementCounter(GetStatsSucceededDateKey(machine), TimeSpan.FromDays(30));
                    IncrementCounter(GetStatsSucceededHourKey(machine), TimeSpan.FromDays(7));
                    IncrementCounter(GetStatsSucceededMinuteKey(machine), TimeSpan.FromDays(1));
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
                    RemoveFromList(GetSucceededKey(item), context.BackgroundJob.Id);
                    DecrementCounter(GetStatsSucceededKey(item));
                }
            }
        }

        public override string StateName => SucceededState.StateName;
    }
}
