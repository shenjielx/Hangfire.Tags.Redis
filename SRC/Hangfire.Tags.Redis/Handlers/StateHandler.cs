using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Redis;
using Hangfire.States;
using Hangfire.Storage;
using Newtonsoft.Json;

namespace Hangfire.Tags.Redis
{
    using M = RedisTagsKeyInfo;
    internal abstract class StateHandler : IStateHandler
    {
        internal readonly int SucceededListSize;
        internal readonly int DeletedListSize;

        public StateHandler(RedisStorageOptions options)
        {
            SucceededListSize = options.SucceededListSize > 0 ? options.SucceededListSize : 1000;
            DeletedListSize = options.DeletedListSize > 0 ? options.DeletedListSize : 1000;
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

    }

    internal class TagsJobArgs
    {
        public IList<string> Tags { get; set; } = new List<string> { };
        public IList<string> AppTags { get; set; } = new List<string> { };
    }
}
