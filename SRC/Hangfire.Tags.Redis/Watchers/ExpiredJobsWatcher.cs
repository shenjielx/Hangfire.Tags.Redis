using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Hangfire.Annotations;
using Hangfire.Logging;
using Hangfire.Redis;
using Hangfire.Server;
using StackExchange.Redis;

namespace Hangfire.Tags.Redis
{
    public class ExpiredJobsWatcher : IBackgroundProcess
    {
        private RedisTagsMonitoringApi MonitoringApi => new RedisTagsMonitoringApi(JobStorage.Current.GetMonitoringApi());

        private static readonly ILog Logger = LogProvider.For<ExpiredJobsWatcher>();

        private readonly TimeSpan _checkInterval;
        private readonly RedisStorageOptions _options;

        private IEnumerable<string> getProcessedKeys(string tagName)
        {
            yield return $"{RedisTagsKeyInfo.Prefix}{tagName}:succeeded";
            yield return $"{RedisTagsKeyInfo.Prefix}{tagName}:deleted";
            yield return $"tags:{tagName}";

        }

        public ExpiredJobsWatcher(RedisStorageOptions options)
        {
            if (options == null)
                throw new ArgumentNullException(nameof(options));
            _options = options;
            var checkInterval = options.ExpiryCheckInterval;
            if (checkInterval.Ticks <= 0)
                throw new ArgumentOutOfRangeException(nameof(checkInterval), "Check interval should be positive.");

            _checkInterval = checkInterval;
        }

        public override string ToString()
        {
            return GetType().ToString();
        }

        void IBackgroundProcess.Execute([NotNull] BackgroundProcessContext context)
        {
            MonitoringApi.UseConnection(redis =>
            {
                var connection = context.Storage.GetConnection();
                var tags = redis.SortedSetRangeByRank(GetRedisKey("tags")).ToStringArray();
                var processedKeys = tags.SelectMany(x => getProcessedKeys(x));

                foreach (var key in processedKeys)
                {
                    var redisKey = GetRedisKey(key);

                    var count = IsTagsKey(key) ? redis.SortedSetLength(redisKey) : redis.ListLength(redisKey);
                    if (count == 0) continue;

                    Logger.InfoFormat("Removing expired records from the '{0}' list...", key);

                    const int batchSize = 100;
                    var keysToRemove = new List<string>();

                    for (var last = count - 1; last >= 0; last -= batchSize)
                    {
                        var first = Math.Max(0, last - batchSize + 1);

                        var jobIds = IsTagsKey(key)
                            ? redis.SortedSetRangeByRank(redisKey, first, last).ToStringArray()
                            : redis.ListRange(redisKey, first, last).ToStringArray();
                        if (jobIds.Length == 0) continue;

                        var pipeline = redis.CreateBatch();
                        var tasks = new Task[jobIds.Length];

                        for (var i = 0; i < jobIds.Length; i++)
                        {
                            tasks[i] = pipeline.KeyExistsAsync(GetRedisKey($"job:{jobIds[i]}"));
                        }

                        pipeline.Execute();
                        Task.WaitAll(tasks);

                        keysToRemove.AddRange(jobIds.Where((t, i) => !((Task<bool>)tasks[i]).Result));
                    }

                    if (keysToRemove.Count == 0) continue;

                    Logger.InfoFormat("Removing {0} expired jobs from '{1}' list...", keysToRemove.Count, key);

                    using (var transaction = connection.CreateWriteTransaction())
                    {
                        foreach (var jobId in keysToRemove)
                        {
                            //connection.ListRemoveAsync(_storage.GetRedisKey(key), value);
                            if (IsTagsKey(key))
                            {
                                transaction.RemoveFromSet(key, jobId);
                            }
                            else
                            {
                                transaction.RemoveFromList(key, jobId);
                            }
                        }

                        transaction.Commit();
                    }
                }
                context.StoppingToken.WaitHandle.WaitOne(_checkInterval);
            });
        }

        private bool IsTagsKey(string key) => key.StartsWith("tags:");

        internal string GetRedisKey([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _options.Prefix + key;
        }
    }
}
