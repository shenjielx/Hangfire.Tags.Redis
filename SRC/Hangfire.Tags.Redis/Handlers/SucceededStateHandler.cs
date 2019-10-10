﻿using System;
using Hangfire.Redis;
using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Tags.Redis
{
    internal class SucceededStateHandler : StateHandler
    {
        public SucceededStateHandler(RedisStorageOptions options) : base(options)
        {
        }

        public override void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            var tags = GetTags(context);
            var storage = context.Storage as RedisStorage;

            foreach (var item in tags)
            {
                transaction.InsertToList(GetSucceededKey(item), context.BackgroundJob.Id);
                transaction.IncrementCounter(GetStatsSucceededKey(item));
                transaction.IncrementCounter(GetStatsSucceededDateKey(item), TimeSpan.FromDays(30));
                transaction.IncrementCounter(GetStatsSucceededHourKey(item), TimeSpan.FromDays(1));

                if (storage != null && SucceededListSize > 0)
                {
                    transaction.TrimList(GetSucceededKey(item), 0, SucceededListSize);
                }
            }
        }

        public override void Unapply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            var tags = GetTags(context);
            foreach (var item in tags)
            {
                transaction.RemoveFromList(GetSucceededKey(item), context.BackgroundJob.Id);
                transaction.DecrementCounter(GetStatsSucceededKey(item));
                transaction.DecrementCounter(GetStatsSucceededDateKey(item));
                transaction.DecrementCounter(GetStatsSucceededHourKey(item));
            }
        }

        public override string StateName => SucceededState.StateName;
    }
}