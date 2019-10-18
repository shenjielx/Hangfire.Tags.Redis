using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Newtonsoft.Json;

namespace Hangfire.Tags.Redis
{
    using M = RedisTagsKeyInfo;
    internal class AutomaticRetryFilter : IApplyStateFilter
    {
        protected HashSet<string> GetTags(ApplyStateContext context)
        {
            var tags = context.Connection.GetAllItemsFromSet(M.GetJobKey(context.BackgroundJob.Id));
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

        public void OnStateApplied(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            var tags = GetTags(context);
            if (context.NewState is ScheduledState &&
                context.NewState.Reason != null &&
                context.NewState.Reason.StartsWith("Retry attempt")) // from hangfire.core AutomaticRetryAttribute.cs
            {
                foreach (var item in tags)
                {
                    transaction.AddToSet(M.GetRetryKey(item), context.BackgroundJob.Id, JobHelper.ToTimestamp(DateTime.UtcNow));
                }
            }
        }

        public void OnStateUnapplied(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            var tags = GetTags(context);
            if (context.OldStateName == ScheduledState.StateName) // from hangfire.core AutomaticRetryAttribute.cs
            {
                foreach (var item in tags)
                {
                    transaction.RemoveFromSet(M.GetRetryKey(item), context.BackgroundJob.Id);
                }
            }
        }
    }
}
