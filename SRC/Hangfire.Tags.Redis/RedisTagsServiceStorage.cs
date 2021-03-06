﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.Redis;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using Hangfire.Tags.Dashboard.Monitoring;
using Hangfire.Tags.Storage;
using StackExchange.Redis;

namespace Hangfire.Tags.Redis
{
    internal class RedisTagsServiceStorage : ITagsServiceStorage
    {
        private readonly RedisStorageOptions _options;
        private readonly IDatabase _database;

        private RedisTagsMonitoringApi MonitoringApi => new RedisTagsMonitoringApi(JobStorage.Current.GetMonitoringApi());

        public RedisTagsServiceStorage(IConnectionMultiplexer connectionMultiplexer) : this(connectionMultiplexer, new RedisStorageOptions())
        {
        }

        public RedisTagsServiceStorage(IConnectionMultiplexer connectionMultiplexer, RedisStorageOptions options)
        {
            _options = options;
            _database = connectionMultiplexer.GetDatabase(options.Db);
        }

        public ITagsTransaction GetTransaction(IWriteOnlyTransaction transaction)
        {
            return new RedisTagsTransaction(_options, transaction, _database);
        }

        public IEnumerable<TagDto> SearchWeightedTags(string tag = null, string setKey = "tags")
        {
            var monitoringApi = MonitoringApi;
            return monitoringApi.UseConnection(connection =>
            {
                var tags = connection.SortedSetScan(GetRedisKey(setKey));
                var pipeline = connection.CreateBatch();
                var tasks = new List<Task> { };
                var total = tags.Count();
                var result = new List<TagDto> { };
                foreach (var tagItem in tags)
                {
                    var tagName = tagItem.Element.ToString();
                    var task = pipeline.SortedSetRangeByRankAsync(GetRedisKey($"{setKey}:{tagName}"))
                    .ContinueWith(x =>
                    {
                        var item = new TagDto
                        {
                            Tag = tagName,
                            Amount = x.Result.Length,
                            Percentage = total > 0 ? (x.Result.Length * 1.0 / total) : 0
                        };
                        result.Add(item);
                    });
                    tasks.Add(task);
                }
                pipeline.Execute();
                Task.WaitAll(tasks.ToArray());
                return result;
            });
        }

        public IEnumerable<string> SearchTags(string tag, string setKey = "tags")
        {
            var monitoringApi = MonitoringApi;
            return monitoringApi.UseConnection(connection =>
            {
                // TODO: 
                var tags = connection.SortedSetScan(GetRedisKey(setKey)).Where(x => x.Element.StartsWith(tag));

                return tags.Select(x => x.Element.ToString()).ToList();
            });
        }

        public int GetJobCount(string[] tags, string stateName = null)
        {
            var monitoringApi = MonitoringApi;
            return monitoringApi.UseConnection(connection => GetJobCount(connection, tags, stateName));
        }

        public IDictionary<string, int> GetJobStateCount(string[] tags, int maxTags = 50)
        {
            var monitoringApi = MonitoringApi;

            return monitoringApi.UseConnection(connection =>
            {
                var succeededCount = 0L;
                var deletedCount = 0L;
                var failedCount = 0L;

                var pipeline = connection.CreateBatch();
                var tasks = new List<Task> { };
                foreach (var tag in tags)
                {
                    var tagCode = tag.Replace("tags:", string.Empty);
                    var task = pipeline.ListLengthAsync(GetRedisKey(RedisTagsKeyInfo.GetSucceededKey(tagCode)))
                    .ContinueWith(x =>
                    {
                        succeededCount += x.Result;
                    });
                    tasks.Add(task);

                    var deletedTask = pipeline.ListLengthAsync(GetRedisKey(RedisTagsKeyInfo.GetDeletedKey(tagCode)))
                    .ContinueWith(x =>
                    {
                        deletedCount += x.Result;
                    });
                    tasks.Add(deletedTask);

                    var failedTask = pipeline.SortedSetLengthAsync(GetRedisKey(RedisTagsKeyInfo.GetFailedKey(tagCode)))
                    .ContinueWith(x =>
                    {
                        failedCount += x.Result;
                    });
                    tasks.Add(failedTask);
                }
                pipeline.Execute();
                Task.WaitAll(tasks.ToArray());

                var result = new Dictionary<string, int> { };
                if (succeededCount > 0)
                {
                    result.Add("Succeeded", Convert.ToInt32(succeededCount));
                }

                if (failedCount > 0)
                {
                    result.Add("Failed", Convert.ToInt32(failedCount));
                }

                if (deletedCount > 0)
                {
                    result.Add("Deleted", Convert.ToInt32(deletedCount));
                }
                return result;
            });
        }

        public JobList<MatchingJobDto> GetMatchingJobs(string[] tags, int @from, int count, string stateName = null)
        {
            var monitoringApi = MonitoringApi;
            var result = monitoringApi.UseConnection(connection =>
             {
                 var jobs = GetJobs(connection, from, count, tags, stateName,
                 (job, jobData, stateData) =>
                     new MatchingJobDto
                     {
                         Job = job,
                         State = jobData[0],
                         CreatedAt = JobHelper.DeserializeNullableDateTime(jobData[1]),
                         ResultAt = GetStateDate(stateData, jobData[0])
                     });
                 return jobs;
             });
            return result;
        }

        private JobList<TDto> GetJobs<TDto>(IDatabase connection, int from, int count, string[] tags, string stateName, Func<Job, IReadOnlyList<string>, SafeDictionary<string, string>, TDto> selector)
        {
            var properties = new string[] { "State", "CreatedAt" };
            var extendedProperties = properties.Concat(new[] { "Type", "Method", "ParameterTypes", "Arguments" }).ToRedisValues();
            var stateProperties = new string[] { };

            var jobIdSource = new List<string> { };
            foreach (var tag in tags)
            {
                var tagCode = tag.Replace("tags:", string.Empty);
                if (string.IsNullOrWhiteSpace(stateName) || stateName.ToLower() == "succeeded")
                {
                    var succeededJobIdList = connection
                        .ListRange(GetRedisKey(RedisTagsKeyInfo.GetSucceededKey(tagCode)))
                        .ToStringArray();
                    jobIdSource.AddRange(succeededJobIdList);
                }
                if (string.IsNullOrWhiteSpace(stateName) || stateName.ToLower() == "deleted")
                {
                    var deletedJobIdList = connection
                        .ListRange(GetRedisKey(RedisTagsKeyInfo.GetDeletedKey(tagCode)))
                        .ToStringArray();
                    jobIdSource.AddRange(deletedJobIdList);
                }
                if (string.IsNullOrWhiteSpace(stateName) || stateName.ToLower() == "failed")
                {
                    var failedJobIdList = connection
                        .SortedSetRangeByRankWithScores(GetRedisKey(RedisTagsKeyInfo.GetFailedKey(tagCode)), order: Order.Descending)
                        .Select(x => x.Element.ToString())
                        .ToArray();
                    jobIdSource.AddRange(failedJobIdList);
                }
            }
            var jobIds = jobIdSource.Distinct().Skip(from).Take(count).ToArray();
            return GetJobsWithProperties(connection, jobIds, properties, stateProperties, selector);
        }

        private JobList<T> GetJobsWithProperties<T>(
            [NotNull] IDatabase connection,
            [NotNull] string[] jobIds,
            string[] properties,
            string[] stateProperties,
            [NotNull] Func<Job, IReadOnlyList<string>, SafeDictionary<string, string>, T> selector)
        {
            if (jobIds == null) throw new ArgumentNullException(nameof(jobIds));
            if (selector == null) throw new ArgumentNullException(nameof(selector));

            if (jobIds.Length == 0) return new JobList<T>(new List<KeyValuePair<string, T>>());

            var jobs = new Dictionary<string, Task<RedisValue[]>>(jobIds.Length, StringComparer.OrdinalIgnoreCase);
            var states = new Dictionary<string, Task<HashEntry[]>>(jobIds.Length, StringComparer.OrdinalIgnoreCase);

            properties = properties ?? new string[0];

            var extendedProperties = properties
                .Concat(new[] { "Type", "Method", "ParameterTypes", "Arguments" })
                .ToRedisValues();

            var pipeline = connection.CreateBatch();
            var tasks = new List<Task>(jobIds.Length * 2);
            foreach (var jobId in jobIds.Distinct())
            {
                var jobTask = pipeline.HashGetAsync(this.GetRedisKey($"job:{jobId}"), extendedProperties);
                tasks.Add(jobTask);
                jobs.Add(jobId, jobTask);

                if (stateProperties == null || !stateProperties.Any())
                {
                    var taskStateJob = pipeline.HashGetAllAsync(this.GetRedisKey($"job:{jobId}:state"));//, stateProperties.ToRedisValues()
                    tasks.Add(taskStateJob);
                    states.Add(jobId, taskStateJob);
                }
            }

            pipeline.Execute();
            Task.WaitAll(tasks.ToArray());

            var datas = jobIds
                .Select(jobId => new
                {
                    JobId = jobId,
                    Job = jobs[jobId].Result.ToStringArray(),
                    Method = TryToGetJob(
                        jobs[jobId].Result[properties.Length],
                        jobs[jobId].Result[properties.Length + 1],
                        jobs[jobId].Result[properties.Length + 2],
                        jobs[jobId].Result[properties.Length + 3]
                        ),
                    StateData = states[jobId].Result.ToStringDictionary()
                });

            var jobList = new JobList<T>(datas
                .Select(x => new KeyValuePair<string, T>(
                    x.JobId,
                    x.Job.Any(y => y != null)
                        ? selector(x.Method, x.Job, x.StateData != null ? new SafeDictionary<string, string>(x.StateData, StringComparer.OrdinalIgnoreCase) : null)
                        : default(T))));

            return jobList;
        }

        private int GetJobCount(IDatabase connection, string[] tags, string stateName)
        {
            var jobCount = 0L;

            var pipeline = connection.CreateBatch();
            var tasks = new List<Task> { };
            foreach (var tag in tags)
            {
                var tagCode = tag.Replace("tags:", string.Empty);

                if (string.IsNullOrWhiteSpace(stateName) || stateName.ToLower() == "succeeded")
                {
                    var task = pipeline.ListLengthAsync(GetRedisKey(RedisTagsKeyInfo.GetSucceededKey(tagCode)))
                    .ContinueWith(x =>
                    {
                        jobCount += x.Result;
                    });
                    tasks.Add(task);
                }

                if (string.IsNullOrWhiteSpace(stateName) || stateName.ToLower() == "deleted")
                {
                    var deletedTask = pipeline.ListLengthAsync(GetRedisKey(RedisTagsKeyInfo.GetDeletedKey(tagCode)))
                    .ContinueWith(x =>
                    {
                        jobCount += x.Result;
                    });
                    tasks.Add(deletedTask);
                }

                if (string.IsNullOrWhiteSpace(stateName) || stateName.ToLower() == "failed")
                {
                    var failedTask = pipeline.SortedSetLengthAsync(GetRedisKey(RedisTagsKeyInfo.GetFailedKey(tagCode)))
                    .ContinueWith(x =>
                    {
                        jobCount += x.Result;
                    });
                    tasks.Add(failedTask);
                }
            }
            pipeline.Execute();
            Task.WaitAll(tasks.ToArray());

            return Convert.ToInt32(jobCount);
        }

        private static Job TryToGetJob(string type, string method, string parameterTypes, string arguments)
        {
            try
            {
                return new InvocationData(type, method, parameterTypes, arguments).DeserializeJob();
            }
            catch (Exception)
            {
                return null;
            }
        }

        private DateTime? GetStateDate(SafeDictionary<string, string> stateData, string stateName)
        {
            var stateDateName = stateName == "Processing" ? "StartedAt" : $"{stateName}At";
            return JobHelper.DeserializeNullableDateTime(stateData?[stateDateName]) ?? (DateTime?)null;
        }

        internal string GetRedisKey([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _options.Prefix + key;
        }

        /// <summary>
        /// Overloaded dictionary that doesn't throw if given an invalid key
        /// Fixes issues such as https://github.com/HangfireIO/Hangfire/issues/871
        /// </summary>
        private class SafeDictionary<TKey, TValue> : Dictionary<TKey, TValue>
        {
            public SafeDictionary(IDictionary<TKey, TValue> dictionary, IEqualityComparer<TKey> comparer)
                : base(dictionary, comparer)
            {
            }

            public new TValue this[TKey i]
            {
                get => ContainsKey(i) ? base[i] : default(TValue);
                set => base[i] = value;
            }
        }

    }


}
