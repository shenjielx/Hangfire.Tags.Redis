﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.Redis;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using StackExchange.Redis;

namespace Hangfire.Tags.Redis.Extensions
{
    public sealed class TagsService : ITagsService
    {
        private readonly IDatabase _database;
        private readonly RedisStorageOptions _options;

        public TagsService(IConnectionMultiplexer multiplexer, RedisStorageOptions options)
        {
            _database = multiplexer.GetDatabase();
            _options = options;
        }

        public long ScheduledCount([NotNull] string tagName)
        {
            if (tagName == null) throw new ArgumentNullException(nameof(tagName));

            return UseConnection(redis => redis.SortedSetLength(GetRedisKey(RedisTagsKeyInfo.GetScheduledKey(tagName))));
        }

        public long EnqueuedCount([NotNull] string tagName)
        {
            if (tagName == null) throw new ArgumentNullException(nameof(tagName));

            return UseConnection(redis => redis.SortedSetLength(GetRedisKey(RedisTagsKeyInfo.GetEnqueuedKey(tagName))));
        }

        public long ProcessingCount([NotNull] string tagName)
        {
            if (tagName == null) throw new ArgumentNullException(nameof(tagName));

            return UseConnection(redis => redis.SortedSetLength(GetRedisKey(RedisTagsKeyInfo.GetProcessingKey(tagName))));
        }

        public long SucceededListCount([NotNull] string tagName)
        {
            if (tagName == null) throw new ArgumentNullException(nameof(tagName));

            return UseConnection(redis => redis.ListLength(GetRedisKey(RedisTagsKeyInfo.GetSucceededKey(tagName))));
        }

        public long FailedCount([NotNull] string tagName)
        {
            if (tagName == null) throw new ArgumentNullException(nameof(tagName));

            return UseConnection(redis => redis.SortedSetLength(GetRedisKey(RedisTagsKeyInfo.GetFailedKey(tagName))));
        }

        public long DeletedListCount([NotNull] string tagName)
        {
            if (tagName == null) throw new ArgumentNullException(nameof(tagName));

            return UseConnection(redis => redis.ListLength(GetRedisKey(RedisTagsKeyInfo.GetDeletedKey(tagName))));
        }

        public IList<ServerDto> Servers()
        {
            return UseConnection(redis =>
            {
                var serverNames = redis
                    .SetMembers(GetRedisKey("servers"))
                    .Select(x => (string)x)
                    .ToList();

                if (serverNames.Count == 0)
                {
                    return new List<ServerDto>();
                }

                var servers = new Dictionary<string, List<string>>();
                var queues = new Dictionary<string, List<string>>();

                foreach (var serverName in serverNames)
                {
                    servers.Add(serverName,
                        redis.HashGet(GetRedisKey($"server:{serverName}"), new RedisValue[] { "WorkerCount", "StartedAt", "Heartbeat" })
                            .ToStringArray().ToList()
                        );
                    queues.Add(serverName,
                        redis.ListRange(GetRedisKey($"server:{serverName}:queues"))
                            .ToStringArray().ToList()
                        );
                }


                return serverNames.Select(x => new ServerDto
                {
                    Name = x,
                    WorkersCount = int.Parse(servers[x][0]),
                    Queues = queues[x],
                    StartedAt = JobHelper.DeserializeDateTime(servers[x][1]),
                    Heartbeat = JobHelper.DeserializeNullableDateTime(servers[x][2])
                }).ToList();
            });
        }

        public IList<string> GetTags()
        {
            return UseConnection(redis =>
            {
                return redis.SortedSetScan(GetRedisKey("tags"))
                   .Select(x => x.Element.ToString())
                   .ToList();
            });
        }

        public TagsStatisticVM GetStatistics([NotNull]string tagName)
        {
            tagName = tagName.ToLower();
            return UseConnection(redis =>
            {
                var stats = new TagsStatisticVM { };

                var pipeline = redis.CreateBatch();
                var tasks = new Task[7];

                tasks[0] = pipeline.SortedSetLengthAsync(GetRedisKey(RedisTagsKeyInfo.GetEnqueuedKey(tagName)))
                    .ContinueWith(x => stats.Enqueued = x.Result);

                tasks[1] = pipeline.SortedSetLengthAsync(GetRedisKey(RedisTagsKeyInfo.GetScheduledKey(tagName)))
                    .ContinueWith(x => stats.Scheduled = x.Result);

                tasks[2] = pipeline.SortedSetLengthAsync(GetRedisKey(RedisTagsKeyInfo.GetProcessingKey(tagName)))
                    .ContinueWith(x => stats.Processing = x.Result);

                tasks[3] = pipeline.StringGetAsync(GetRedisKey(RedisTagsKeyInfo.GetStatsSucceededKey(tagName)))
                    .ContinueWith(x => stats.Succeeded = long.Parse(x.Result.HasValue ? (string)x.Result : "0"));

                tasks[4] = pipeline.SortedSetLengthAsync(GetRedisKey(RedisTagsKeyInfo.GetFailedKey(tagName)))
                    .ContinueWith(x => stats.Failed = x.Result);

                tasks[5] = pipeline.StringGetAsync(GetRedisKey(RedisTagsKeyInfo.GetStatsDeletedKey(tagName)))
                    .ContinueWith(x => stats.Deleted = long.Parse(x.Result.HasValue ? (string)x.Result : "0"));

                tasks[6] = pipeline.SortedSetLengthAsync(GetRedisKey(RedisTagsKeyInfo.GetAwaitingKey(tagName)))
                    .ContinueWith(x => stats.Awaiting = x.Result);


                pipeline.Execute();
                Task.WaitAll(tasks);

                return stats;
            });
        }

        public List<TagsStatisticVM> GetStatisticsSummary(string[] tags)
        {
            return UseConnection(redis =>
            {
                var result = new List<TagsStatisticVM> { };

                var pipeline = redis.CreateBatch();
                var tasks = new Task[tags.Length * 7];

                var index = 0;
                foreach (var tagName in tags)
                {
                    var stats = new TagsStatisticVM
                    {
                        TagCode = tagName
                    };
                    tasks[index++] = pipeline.SortedSetLengthAsync(GetRedisKey(RedisTagsKeyInfo.GetEnqueuedKey(tagName)))
                        .ContinueWith(x => stats.Enqueued = x.Result);

                    tasks[index++] = pipeline.SortedSetLengthAsync(GetRedisKey(RedisTagsKeyInfo.GetScheduledKey(tagName)))
                        .ContinueWith(x => stats.Scheduled = x.Result);

                    tasks[index++] = pipeline.SortedSetLengthAsync(GetRedisKey(RedisTagsKeyInfo.GetProcessingKey(tagName)))
                        .ContinueWith(x => stats.Processing = x.Result);

                    tasks[index++] = pipeline.StringGetAsync(GetRedisKey(RedisTagsKeyInfo.GetStatsSucceededKey(tagName)))
                        .ContinueWith(x => stats.Succeeded = long.Parse(x.Result.HasValue ? (string)x.Result : "0"));

                    tasks[index++] = pipeline.SortedSetLengthAsync(GetRedisKey(RedisTagsKeyInfo.GetFailedKey(tagName)))
                        .ContinueWith(x => stats.Failed = x.Result);

                    tasks[index++] = pipeline.StringGetAsync(GetRedisKey(RedisTagsKeyInfo.GetStatsDeletedKey(tagName)))
                        .ContinueWith(x => stats.Deleted = long.Parse(x.Result.HasValue ? (string)x.Result : "0"));

                    tasks[index++] = pipeline.SortedSetLengthAsync(GetRedisKey(RedisTagsKeyInfo.GetAwaitingKey(tagName)))
                        .ContinueWith(x => stats.Awaiting = x.Result);
                    result.Add(stats);
                }


                pipeline.Execute();
                Task.WaitAll(tasks);

                return result;
            });
        }


        public JobList<SucceededJobDto> SucceededJobs(string tagName, int from, int count)
        {
            tagName = tagName.ToLower();
            return UseConnection(redis =>
            {
                var succeededJobIds = redis
                    .ListRange(GetRedisKey(RedisTagsKeyInfo.GetSucceededKey(tagName)), from, from + count - 1)
                    .ToStringArray();

                return GetJobsWithProperties(
                    redis,
                    succeededJobIds,
                    null,
                    new[] { "SucceededAt", "PerformanceDuration", "Latency", "State", "Result" },
                    (job, jobData, state) => new SucceededJobDto
                    {
                        Job = job,
                        Result = state[4],
                        SucceededAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        TotalDuration = state[1] != null && state[2] != null
                            ? (long?)long.Parse(state[1]) + (long?)long.Parse(state[2])
                            : null,
                        InSucceededState = SucceededState.StateName.Equals(state[3], StringComparison.OrdinalIgnoreCase)
                    });
            });
        }

        public JobList<EnqueuedJobDto> EnqueuedJobs([NotNull] string tagName, int from, int count)
        {
            if (tagName == null) throw new ArgumentNullException(nameof(tagName));

            return UseConnection(redis =>
            {
                var jobIds = redis
                    .SortedSetRangeByRankWithScores(GetRedisKey(RedisTagsKeyInfo.GetEnqueuedKey(tagName)), from, from + count - 1, order: Order.Descending)
                    .Select(x => x.Element.ToString())
                    .ToArray();

                return GetJobsWithProperties(
                    redis,
                    jobIds,
                    new[] { "State" },
                    new[] { "EnqueuedAt", "State" },
                    (job, jobData, state) => new EnqueuedJobDto
                    {
                        Job = job,
                        State = jobData[0],
                        EnqueuedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        InEnqueuedState = jobData[0].Equals(state[1], StringComparison.OrdinalIgnoreCase)
                    });
            });
        }

        public JobList<DeletedJobDto> DeletedJobs(string tagName, int from, int count)
        {
            return UseConnection(redis =>
            {
                var deletedJobIds = redis
                    .ListRange(GetRedisKey(RedisTagsKeyInfo.GetDeletedKey(tagName)), from, from + count - 1)
                    .ToStringArray();

                return GetJobsWithProperties(
                    redis,
                    deletedJobIds,
                    null,
                    new[] { "DeletedAt", "State" },
                    (job, jobData, state) => new DeletedJobDto
                    {
                        Job = job,
                        DeletedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        InDeletedState = DeletedState.StateName.Equals(state[1], StringComparison.OrdinalIgnoreCase)
                    });
            });
        }

        public JobList<FailedJobDto> FailedJobs(string tagName, int from, int count)
        {
            return UseConnection(redis =>
            {
                var failedJobIds = redis
                    .SortedSetRangeByRankWithScores(GetRedisKey(RedisTagsKeyInfo.GetFailedKey(tagName)), from, from + count - 1, order: Order.Descending)
                    .Select(x => x.Element.ToString())
                    .ToArray();

                return GetJobsWithProperties(
                    redis,
                    failedJobIds,
                    null,
                    new[] { "FailedAt", "ExceptionType", "ExceptionMessage", "ExceptionDetails", "State", "Reason" },
                    (job, jobData, state) => new FailedJobDto
                    {
                        Job = job,
                        Reason = state[5],
                        FailedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        ExceptionType = state[1],
                        ExceptionMessage = state[2],
                        ExceptionDetails = state[3],
                        InFailedState = FailedState.StateName.Equals(state[4], StringComparison.OrdinalIgnoreCase)
                    });
            });
        }

        public JobList<ProcessingJobDto> ProcessingJobs(string tagName, int from, int count)
        {
            return UseConnection(redis =>
            {
                var jobIds = redis
                    .SortedSetRangeByRankWithScores(GetRedisKey(RedisTagsKeyInfo.GetProcessingKey(tagName)), from, from + count - 1)
                    .Select(x => x.Element.ToString())
                    .ToArray();

                return new JobList<ProcessingJobDto>(GetJobsWithProperties(redis,
                    jobIds,
                    null,
                    new[] { "StartedAt", "ServerName", "ServerId", "State" },
                    (job, jobData, state) => new ProcessingJobDto
                    {
                        ServerId = state[2] ?? state[1],
                        Job = job,
                        StartedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        InProcessingState = ProcessingState.StateName.Equals(
                            state[3], StringComparison.OrdinalIgnoreCase),
                    })
                    .Where(x => x.Value.ServerId != null)
                    .OrderBy(x => x.Value.StartedAt).ToList());
            });
        }

        public JobList<ScheduledJobDto> ScheduledJobs(string tagName, int from, int count)
        {
            return UseConnection(redis =>
            {
                var scheduledJobs = redis
                    .SortedSetRangeByRankWithScores(GetRedisKey(RedisTagsKeyInfo.GetScheduledKey(tagName)), from, from + count - 1)
                    .ToList();

                if (scheduledJobs.Count == 0)
                {
                    return new JobList<ScheduledJobDto>(new List<KeyValuePair<string, ScheduledJobDto>>());
                }

                var jobs = new ConcurrentDictionary<string, List<string>>();
                var states = new ConcurrentDictionary<string, List<string>>(); ;

                var pipeline = redis.CreateBatch();
                var tasks = new Task[scheduledJobs.Count * 2];
                int i = 0;
                foreach (var scheduledJob in scheduledJobs)
                {
                    var jobId = scheduledJob.Element;
                    tasks[i] = pipeline.HashGetAsync(
                                GetRedisKey($"job:{jobId}"),
                                new RedisValue[] { "Type", "Method", "ParameterTypes", "Arguments" })
                        .ContinueWith(x => jobs.TryAdd(jobId, x.Result.ToStringArray().ToList()));
                    i++;
                    tasks[i] = pipeline.HashGetAsync(
                                GetRedisKey($"job:{jobId}:state"),
                                new RedisValue[] { "State", "ScheduledAt" })
                        .ContinueWith(x => states.TryAdd(jobId, x.Result.ToStringArray().ToList()));
                    i++;
                }

                pipeline.Execute();
                Task.WaitAll(tasks);

                return new JobList<ScheduledJobDto>(scheduledJobs
                    .Select(job => new KeyValuePair<string, ScheduledJobDto>(
                        job.Element,
                        new ScheduledJobDto
                        {
                            EnqueueAt = JobHelper.FromTimestamp((long)job.Score),
                            Job = TryToGetJob(jobs[job.Element][0], jobs[job.Element][1], jobs[job.Element][2], jobs[job.Element][3]),
                            ScheduledAt =
                                states[job.Element].Count > 1
                                    ? JobHelper.DeserializeNullableDateTime(states[job.Element][1])
                                    : null,
                            InScheduledState =
                                ScheduledState.StateName.Equals(states[job.Element][0], StringComparison.OrdinalIgnoreCase)
                        }))
                    .ToList());
            });
        }

        public JobDetailsDto JobDetails([NotNull] string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            return UseConnection(redis =>
            {
                var job = redis
                    .HashGetAll(GetRedisKey($"job:{jobId}"))
                    .ToStringDictionary();

                if (job.Count == 0) return null;

                var hiddenProperties = new[] { "Type", "Method", "ParameterTypes", "Arguments", "State", "CreatedAt", "Fetched" };

                var history = redis
                    .ListRange(GetRedisKey($"job:{jobId}:history"))
                    .ToStringArray()
                    .Select(SerializationHelper.Deserialize<Dictionary<string, string>>)
                    .ToList();

                // history is in wrong order, fix this
                history.Reverse();

                var stateHistory = new List<StateHistoryDto>(history.Count);
                foreach (var entry in history)
                {
                    var stateData = new Dictionary<string, string>(entry, StringComparer.OrdinalIgnoreCase);
                    var dto = new StateHistoryDto
                    {
                        StateName = stateData["State"],
                        Reason = stateData.ContainsKey("Reason") ? stateData["Reason"] : null,
                        CreatedAt = JobHelper.DeserializeDateTime(stateData["CreatedAt"]),
                    };

                    // Each history item contains all of the information,
                    // but other code should not know this. We'll remove
                    // unwanted keys.
                    stateData.Remove("State");
                    stateData.Remove("Reason");
                    stateData.Remove("CreatedAt");

                    dto.Data = stateData;
                    stateHistory.Add(dto);
                }

                // For compatibility
                if (!job.ContainsKey("Method")) job.Add("Method", null);
                if (!job.ContainsKey("ParameterTypes")) job.Add("ParameterTypes", null);

                return new JobDetailsDto
                {
                    Job = TryToGetJob(job["Type"], job["Method"], job["ParameterTypes"], job["Arguments"]),
                    CreatedAt =
                        job.ContainsKey("CreatedAt")
                            ? JobHelper.DeserializeDateTime(job["CreatedAt"])
                            : (DateTime?)null,
                    Properties =
                        job.Where(x => !hiddenProperties.Contains(x.Key)).ToDictionary(x => x.Key, x => x.Value),
                    History = stateHistory
                };
            });
        }


        public IDictionary<DateTime, long> DateSucceededJobs(string tagCode)
        {
            return UseConnection(redis => GetTimelineStats(redis, x => RedisTagsKeyInfo.GetStatsSucceededDateKey(tagCode, x)));
        }

        public IDictionary<DateTime, long> DateFailedJobs(string tagCode)
        {
            return UseConnection(redis => GetTimelineStats(redis, x => RedisTagsKeyInfo.GetStatsFailedDateKey(tagCode, x)));
        }

        public IDictionary<DateTime, long> HourlySucceededJobs(string tagCode)
        {
            return UseConnection(redis => GetHourlyTimelineStats(redis, x => RedisTagsKeyInfo.GetStatsSucceededHourKey(tagCode, x)));
        }

        public IDictionary<DateTime, long> HourlyFailedJobs(string tagCode)
        {
            return UseConnection(redis => GetHourlyTimelineStats(redis, x => RedisTagsKeyInfo.GetStatsFailedHourKey(tagCode, x)));
        }

        private Dictionary<DateTime, long> GetTimelineStats([NotNull] IDatabase redis, [NotNull] Func<DateTime, string> key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var endDate = DateTime.Today;
            var startDate = endDate.AddDays(-7);
            var dates = new List<DateTime>();

            while (startDate <= endDate)
            {
                dates.Add(endDate);
                endDate = endDate.AddDays(-1);
            }

            var keys = dates.Select(x => GetRedisKey(key(x))).ToArray();

            var valuesMap = redis.GetValuesMap(keys);

            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < dates.Count; i++)
            {
                long value;
                if (!long.TryParse(valuesMap[valuesMap.Keys.ElementAt(i)], out value) || value < 0)
                {
                    value = 0;
                }
                result.Add(dates[i], value);
            }

            return result;
        }

        private Dictionary<DateTime, long> GetHourlyTimelineStats([NotNull] IDatabase redis, [NotNull] Func<DateTime, string> key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var endDate = DateTime.Now;
            var dates = new List<DateTime>();
            for (var i = 0; i < 24; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddHours(-1);
            }

            var keys = dates.Select(x => GetRedisKey(key(x))).ToArray();
            var valuesMap = redis.GetValuesMap(keys);

            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < dates.Count; i++)
            {
                long value;
                if (!long.TryParse(valuesMap[valuesMap.Keys.ElementAt(i)], out value) || value < 0)
                {
                    value = 0;
                }

                result.Add(dates[i], value);
            }

            return result;
        }

        private JobList<T> GetJobsWithProperties<T>(
        [NotNull] IDatabase redis,
        [NotNull] string[] jobIds,
        string[] properties,
        string[] stateProperties,
        [NotNull] Func<Job, IReadOnlyList<string>, IReadOnlyList<string>, T> selector)
        {
            if (jobIds == null) throw new ArgumentNullException(nameof(jobIds));
            if (selector == null) throw new ArgumentNullException(nameof(selector));

            if (jobIds.Length == 0) return new JobList<T>(new List<KeyValuePair<string, T>>());

            var jobs = new Dictionary<string, Task<RedisValue[]>>(jobIds.Length, StringComparer.OrdinalIgnoreCase);
            var states = new Dictionary<string, Task<RedisValue[]>>(jobIds.Length, StringComparer.OrdinalIgnoreCase);

            properties = properties ?? new string[0];

            var extendedProperties = properties
                .Concat(new[] { "Type", "Method", "ParameterTypes", "Arguments" })
                .ToRedisValues();

            var pipeline = redis.CreateBatch();
            var tasks = new List<Task>(jobIds.Length * 2);
            foreach (var jobId in jobIds.Distinct())
            {
                var jobTask = pipeline.HashGetAsync(
                        GetRedisKey($"job:{jobId}"),
                        extendedProperties);
                tasks.Add(jobTask);
                jobs.Add(jobId, jobTask);

                if (stateProperties != null)
                {
                    var taskStateJob = pipeline.HashGetAsync(
                        GetRedisKey($"job:{jobId}:state"),
                        stateProperties.ToRedisValues());
                    tasks.Add(taskStateJob);
                    states.Add(jobId, taskStateJob);
                }
            }

            pipeline.Execute();
            Task.WaitAll(tasks.ToArray());

            var jobList = new JobList<T>(jobIds
                .Select(jobId => new
                {
                    JobId = jobId,
                    Job = jobs[jobId].Result.ToStringArray(),
                    Method = TryToGetJob(
                        jobs[jobId].Result[properties.Length],
                        jobs[jobId].Result[properties.Length + 1],
                        jobs[jobId].Result[properties.Length + 2],
                        jobs[jobId].Result[properties.Length + 3]),
                    State = stateProperties != null ? states[jobId].Result.ToStringArray() : null
                })
                .Select(x => new KeyValuePair<string, T>(
                    x.JobId,
                    x.Job.Any(y => y != null)
                        ? selector(x.Method, x.Job, x.State)
                        : default(T))));
            return jobList;
        }

        private static Job TryToGetJob(
            string type, string method, string parameterTypes, string arguments)
        {
            try
            {
                return new InvocationData(
                    type,
                    method,
                    parameterTypes,
                    arguments).DeserializeJob();
            }
            catch (Exception)
            {
                return null;
            }
        }



        private T UseConnection<T>(Func<IDatabase, T> action)
        {
            return action(_database);
        }

        private string GetRedisKey([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _options.Prefix + key;
        }

    }
}
