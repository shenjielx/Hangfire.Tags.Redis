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

            try
            {
                //Environment.MachineName
                var machine = Environment.MachineName.ToLower();
                if (_useTransactions)
                {
                    foreach (var item in tags)
                    {
                        transaction.AddToSet(GetFailedKey(item), context.BackgroundJob.Id, timestamp);
                        transaction.IncrementCounter(_prefix + GetStatsFailedDateKey(item), TimeSpan.FromDays(30));
                        transaction.IncrementCounter(_prefix + GetStatsFailedHourKey(item), TimeSpan.FromDays(7));
                        transaction.IncrementCounter(_prefix + GetStatsFailedMinuteKey(item), TimeSpan.FromDays(2));
                        //Environment.MachineName
                        var machineItem = $"{item}:{machine}";
                        transaction.IncrementCounter(_prefix + GetStatsFailedDateKey(machineItem),
                            TimeSpan.FromDays(30));
                        transaction.IncrementCounter(_prefix + GetStatsFailedHourKey(machineItem),
                            TimeSpan.FromDays(7));
                        transaction.IncrementCounter(_prefix + GetStatsFailedMinuteKey(machineItem),
                            TimeSpan.FromDays(1));
                    }
                    transaction.IncrementCounter(GetStatsFailedDateKey(machine), TimeSpan.FromDays(30));
                    transaction.IncrementCounter(GetStatsFailedHourKey(machine), TimeSpan.FromDays(7));
                    transaction.IncrementCounter(GetStatsFailedMinuteKey(machine), TimeSpan.FromDays(1));
                }
                else
                {
                    var pipeline = _database.CreateBatch();
                    foreach (var item in tags)
                    {
                        AddToSet(pipeline, _prefix + GetFailedKey(item), context.BackgroundJob.Id, timestamp);
                        IncrementCounter(pipeline, _prefix + GetStatsFailedDateKey(item), TimeSpan.FromDays(30));
                        IncrementCounter(pipeline, _prefix + GetStatsFailedHourKey(item), TimeSpan.FromDays(7));
                        IncrementCounter(pipeline, _prefix + GetStatsFailedMinuteKey(item), TimeSpan.FromDays(2));
                        //Environment.MachineName
                        var machineItem = $"{item}:{machine}";
                        IncrementCounter(pipeline, _prefix + GetStatsFailedDateKey(machineItem), TimeSpan.FromDays(30));
                        IncrementCounter(pipeline, _prefix + GetStatsFailedHourKey(machineItem), TimeSpan.FromDays(7));
                        IncrementCounter(pipeline, _prefix + GetStatsFailedMinuteKey(machineItem), TimeSpan.FromDays(1));
                    }
                    IncrementCounter(pipeline, _prefix + GetStatsFailedDateKey(machine), TimeSpan.FromDays(30));
                    IncrementCounter(pipeline, _prefix + GetStatsFailedHourKey(machine), TimeSpan.FromDays(7));
                    IncrementCounter(pipeline, _prefix + GetStatsFailedMinuteKey(machine), TimeSpan.FromDays(1));
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
                    transaction.RemoveFromSet(GetFailedKey(item), context.BackgroundJob.Id);
                }
            }
            else
            {
                var pipeline = _database.CreateBatch();
                foreach (var item in tags)
                {
                    RemoveFromSet(pipeline, _prefix + GetFailedKey(item), context.BackgroundJob.Id);
                }
                pipeline.Execute();
            }
        }

        public override string StateName => FailedState.StateName;
    }
}
