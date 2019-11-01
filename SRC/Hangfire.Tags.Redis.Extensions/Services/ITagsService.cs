using System;
using System.Collections.Generic;
using Hangfire.Annotations;
using Hangfire.Storage.Monitoring;

namespace Hangfire.Tags.Redis.Extensions
{
    public interface ITagsService
    {
        long ScheduledCount(string tagName);
        long EnqueuedCount(string tagName);
        long FailedCount(string tagName);
        long ProcessingCount(string tagName);
        long RetriesCount(string tagName);

        long SucceededListCount(string tagName);
        long DeletedListCount(string tagName);

        IList<ServerDto> Servers();
        IList<string> GetTags();

        TagsStatisticVM GetStatistics([NotNull]string tagName);
        List<TagsStatisticVM> GetStatisticsSummary(string[] tags);

        JobList<TagsSucceededJobDto> SucceededJobs(string tagName, int from, int count);
        JobList<EnqueuedJobDto> EnqueuedJobs([NotNull] string tagName, int from, int count);
        JobList<DeletedJobDto> DeletedJobs(string tagName, int from, int count);
        JobList<FailedJobDto> FailedJobs(string tagName, int from, int count);
        JobList<ProcessingJobDto> ProcessingJobs(string tagName, int from, int count);
        JobList<ScheduledJobDto> ScheduledJobs(string tagName, int from, int count);
        JobList<RetriesJobDto> RetriesJobs(string tagName, int from, int count);
        JobDetailsDto JobDetails([NotNull] string jobId);

        IDictionary<DateTime, long> DateSucceededJobs(string tagCode, DateTime? startDate = null, DateTime? endDate = null);
        IDictionary<DateTime, long> DateFailedJobs(string tagCode, DateTime? startDate = null, DateTime? endDate = null);
        IDictionary<DateTime, long> HourlySucceededJobs(string tagCode, DateTime? startDate = null, DateTime? endDate = null);
        IDictionary<DateTime, long> HourlyFailedJobs(string tagCode, DateTime? startDate = null, DateTime? endDate = null);
        IDictionary<DateTime, long> MinuteSucceededJobs(string tagCode, DateTime? startDate = null, DateTime? endDate = null);
        IDictionary<DateTime, long> MinuteFailedJobs(string tagCode, DateTime? startDate = null, DateTime? endDate = null);
    }
}
