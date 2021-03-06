﻿using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Redis;
using Hangfire.States;
using Hangfire.Storage;
using Newtonsoft.Json;
using StackExchange.Redis;

namespace Hangfire.Tags.Redis
{
    using M = RedisTagsKeyInfo;
    internal abstract class StateHandler : IStateHandler
    {
        protected readonly string _prefix;
        internal readonly int SucceededListSize;
        internal readonly int DeletedListSize;
        protected readonly bool _useTransactions;
        protected readonly IDatabase _database;

        public StateHandler(RedisStorageOptions options, IConnectionMultiplexer multiplexer)
        {
            SucceededListSize = options.SucceededListSize > 0 ? options.SucceededListSize : 1000;
            DeletedListSize = options.DeletedListSize > 0 ? options.DeletedListSize : 1000;
            _useTransactions = options.UseTransactions;
            _database = multiplexer.GetDatabase();
            _prefix = options.Prefix;
        }

        internal string GetSucceededKey(string tagName) => M.GetSucceededKey(tagName);
        internal string GetDeletedKey(string tagName) => M.GetDeletedKey(tagName);
        internal string GetFailedKey(string tagName) => M.GetFailedKey(tagName);
        internal string GetScheduledKey(string tagName) => M.GetScheduledKey(tagName);

        internal string GetAwaitingKey(string tagName) => M.GetAwaitingKey(tagName);
        internal string GetEnqueuedKey(string tagName) => M.GetEnqueuedKey(tagName);
        internal string GetProcessingKey(string tagName) => M.GetProcessingKey(tagName);

        internal string GetStatsSucceededKey(string tagName) => M.GetStatsSucceededKey(tagName);
        internal string GetStatsDeletedKey(string tagName) => M.GetStatsDeletedKey(tagName);

        internal string GetStatsSucceededDateKey(string tagName) => M.GetStatsSucceededDateKey(tagName, DateTime.Today);
        internal string GetStatsFailedDateKey(string tagName) => M.GetStatsFailedDateKey(tagName, DateTime.Today);

        internal string GetStatsSucceededHourKey(string tagName) => M.GetStatsSucceededHourKey(tagName, DateTime.Now);
        internal string GetStatsFailedHourKey(string tagName) => M.GetStatsFailedHourKey(tagName, DateTime.Now);

        internal string GetStatsSucceededMinuteKey(string tagName) => M.GetStatsSucceededMinuteKey(tagName, DateTime.Now);
        internal string GetStatsFailedMinuteKey(string tagName) => M.GetStatsFailedMinuteKey(tagName, DateTime.Now);

        private string getJobKey(string jobId) => M.GetJobKey(jobId);

        protected HashSet<string> GetTags(ApplyStateContext context)
        {
            var tags = context.Connection.GetAllItemsFromSet(getJobKey(context.BackgroundJob.Id));
            if (!tags.Any() && context.BackgroundJob.Job.Args.Count > 1)
            {
                try
                {
                    var jobRequest = JsonConvert.DeserializeObject<TagsJobArgs>(context.BackgroundJob.Job.Args.LastOrDefault().ToString());
                    if (jobRequest != null && (jobRequest.AppTags.Any() || jobRequest.Tags.Any()))
                    {
                        var tagsList = Enumerable.Empty<string>().Concat(jobRequest.AppTags).Concat(jobRequest.Tags)
                            .Where(x => !string.IsNullOrWhiteSpace(x))
                            .Distinct();
                        tags = new HashSet<string>(tagsList);
                    }
                }
                catch
                {
                }
            }
            return tags;
        }

        public abstract string StateName { get; }

        public abstract void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction);
        public abstract void Unapply(ApplyStateContext context, IWriteOnlyTransaction transaction);

        protected void IncrementCounter(IBatch pipeline, string key, TimeSpan expireIn)
        {
            pipeline.StringIncrementAsync(key);
            pipeline.KeyExpireAsync(key, expireIn);
        }
        
        protected void IncrementCounter(IBatch pipeline, string key) => pipeline.StringIncrementAsync(key);

        protected void InsertToList(IBatch pipeline, string key, string value) => pipeline.ListLeftPushAsync(key, (RedisValue) value);

        protected void RemoveFromList(IBatch pipeline, string key, string value) => pipeline.ListRemoveAsync(key, (RedisValue) value);

        protected void TrimList(IBatch pipeline, string key, int keepStartingFrom, int keepEndingAt) => pipeline.ListTrimAsync(key, keepStartingFrom, keepEndingAt);

        protected void DecrementCounter(IBatch pipeline, string key) => pipeline.StringDecrementAsync(key);

        protected void AddToSet(IBatch pipeline, string key, string value) => AddToSet(pipeline, key, value, 0.0);

        protected void AddToSet(IBatch pipeline, string key, string value, double score)
        {
            if (value == null)
                throw new ArgumentNullException(nameof (value));
            pipeline.SortedSetAddAsync(key, (RedisValue) value, score);
        }

        protected void RemoveFromSet(IBatch pipeline, string key, string value)
        {
            if (value == null)
                throw new ArgumentNullException(nameof (value));
            pipeline.SortedSetRemoveAsync((RedisKey) key, (RedisValue) value);
        }
    }

    internal class TagsJobArgs
    {
        public IList<string> Tags { get; set; } = new List<string> { };
        public IList<string> AppTags { get; set; } = new List<string> { };
    }
}
