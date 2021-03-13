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

            try
            {
                //Environment.MachineName
                var machine = Environment.MachineName.ToLower();
                if (_useTransactions)
                {
                    foreach (var item in tags)
                    {
                        transaction.InsertToList(GetSucceededKey(item), context.BackgroundJob.Id);
                        transaction.IncrementCounter(GetStatsSucceededKey(item));
                        transaction.IncrementCounter(GetStatsSucceededDateKey(item), TimeSpan.FromDays(30));
                        transaction.IncrementCounter(GetStatsSucceededHourKey(item), TimeSpan.FromDays(7));
                        transaction.IncrementCounter(GetStatsSucceededMinuteKey(item), TimeSpan.FromDays(2));
                        //Environment.MachineName
                        var machineItem = $"{item}:{machine}";
                        transaction.IncrementCounter(GetStatsSucceededDateKey(machineItem), TimeSpan.FromDays(30));
                        transaction.IncrementCounter(GetStatsSucceededHourKey(machineItem), TimeSpan.FromDays(7));
                        transaction.IncrementCounter(GetStatsSucceededMinuteKey(machineItem), TimeSpan.FromDays(1));

                        if (storage != null && SucceededListSize > 0)
                        {
                            transaction.TrimList(GetSucceededKey(item), 0, SucceededListSize);
                        }
                    }
                    transaction.IncrementCounter(GetStatsSucceededDateKey(machine), TimeSpan.FromDays(30));
                    transaction.IncrementCounter(GetStatsSucceededHourKey(machine), TimeSpan.FromDays(7));
                    transaction.IncrementCounter(GetStatsSucceededMinuteKey(machine), TimeSpan.FromDays(1));
                }
                else
                {
                    var pipeline = _database.CreateBatch();
                    foreach (var item in tags)
                    {
                        InsertToList(pipeline, _prefix + GetSucceededKey(item), context.BackgroundJob.Id);
                        IncrementCounter(pipeline, _prefix + GetStatsSucceededKey(item));
                        IncrementCounter(pipeline, _prefix + GetStatsSucceededDateKey(item), TimeSpan.FromDays(30));
                        IncrementCounter(pipeline, _prefix + GetStatsSucceededHourKey(item), TimeSpan.FromDays(7));
                        IncrementCounter(pipeline, _prefix + GetStatsSucceededMinuteKey(item), TimeSpan.FromDays(2));
                        //Environment.MachineName
                        var machineItem = $"{item}:{machine}";
                        IncrementCounter(pipeline, _prefix + GetStatsSucceededDateKey(machineItem), TimeSpan.FromDays(30));
                        IncrementCounter(pipeline, _prefix + GetStatsSucceededHourKey(machineItem), TimeSpan.FromDays(7));
                        IncrementCounter(pipeline, _prefix + GetStatsSucceededMinuteKey(machineItem), TimeSpan.FromDays(1));

                        if (storage != null && SucceededListSize > 0)
                        {
                            TrimList(pipeline, _prefix + GetSucceededKey(item), 0, SucceededListSize);
                        }
                    }
                    IncrementCounter(pipeline, _prefix + GetStatsSucceededDateKey(machine), TimeSpan.FromDays(30));
                    IncrementCounter(pipeline, _prefix + GetStatsSucceededHourKey(machine), TimeSpan.FromDays(7));
                    IncrementCounter(pipeline, _prefix + GetStatsSucceededMinuteKey(machine), TimeSpan.FromDays(1));
                    pipeline.Execute();
                }
            }
            catch
            {
            }
        }

        public override void Unapply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            var tags = GetTags(context);
            if (_useTransactions)
            {
                foreach (var item in tags)
                {
                    transaction.RemoveFromList(GetSucceededKey(item), context.BackgroundJob.Id);
                    transaction.DecrementCounter(GetStatsSucceededKey(item));
                    //transaction.DecrementCounter(GetStatsSucceededDateKey(item));
                    //transaction.DecrementCounter(GetStatsSucceededHourKey(item));
                }
            }
            else
            {
                var pipeline = _database.CreateBatch();
                foreach (var item in tags)
                {
                    RemoveFromList(pipeline, _prefix + GetSucceededKey(item), context.BackgroundJob.Id);
                    DecrementCounter(pipeline, _prefix + GetStatsSucceededKey(item));
                }
                pipeline.Execute();
            }
        }

        public override string StateName => SucceededState.StateName;
    }
}
