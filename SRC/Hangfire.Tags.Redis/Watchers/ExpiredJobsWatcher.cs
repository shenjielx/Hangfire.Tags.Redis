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
        private static readonly string OwnerId = Guid.NewGuid().ToString();
        private static readonly TimeSpan DefaultHoldDuration = TimeSpan.FromSeconds(30);

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

        private  static readonly string LuaScriptCheckKeys = @"local result = {}
for i= 1, #KEYS do
  result[i] = redis.call('EXISTS', KEYS[i])
end

return result";

        private static readonly string LuaScriptListRemove = @"
for i= 1, #ARGV do
  redis.call('LREM', KEYS[1], -1, ARGV[i])
end";

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

                    var redisKeyLock = $"{redisKey}:execute:lock";
                    if (!redis.LockTake(redisKeyLock, OwnerId, DefaultHoldDuration))
                    {
                        continue;
                    }

                    try
                    {
                        var count = IsTagsKey(key) ? redis.SortedSetLength(redisKey) : redis.ListLength(redisKey);
                        if (count == 0) continue;

                        Logger.InfoFormat("Removing expired records from the '{0}' list...", key);

                        const int batchSize = 10;
                        var keysToRemove = new List<string>();

                        for (var last = count - 1; last >= 0; last -= batchSize)
                        {
                            var first = Math.Max(0, last - batchSize + 1);

                            var jobIds = IsTagsKey(key)
                                ? redis.SortedSetRangeByRank(redisKey, first, last).ToStringArray()
                                : redis.ListRange(redisKey, first, last).ToStringArray();
                            if (jobIds.Length == 0) continue;

                            // var pipeline = redis.CreateBatch();
                            // var tasks = new Task[jobIds.Length];
                            //
                            // for (var i = 0; i < jobIds.Length; i++)
                            // {
                            //     tasks[i] = pipeline.KeyExistsAsync(GetRedisKey($"job:{jobIds[i]}"));
                            // }
                            //
                            // pipeline.Execute();
                            // Task.WaitAll(tasks);
                            //
                            // keysToRemove.AddRange(jobIds.Where((t, i) => !((Task<bool>) tasks[i]).Result));
                            var result = (int[]) redis.ScriptEvaluate(LuaScriptCheckKeys, jobIds.Select(x=>(RedisKey)GetRedisKey($"job:{x}")).ToArray());
                            keysToRemove.AddRange(jobIds.Where((t, i) => result != null && result.Length >= i && result[i] == 0));
                        }

                        if (keysToRemove.Count == 0) continue;

                        Logger.InfoFormat("Removing {0} expired jobs from '{1}' list...", keysToRemove.Count, key);

                        var transaction = redis.CreateTransaction();
                        var keysToRemove2 = new List<string>();
                        var hasTags = false;
                        foreach (var jobId in keysToRemove)
                        {
                            //connection.ListRemoveAsync(_storage.GetRedisKey(key), value);
                            if (IsTagsKey(key))
                            {
                                hasTags = true;
                                transaction.SortedSetRemoveAsync(redisKey, jobId);
                                // transaction.RemoveFromSet(key, jobId);
                            }
                            else
                            {
                                keysToRemove2.Add(jobId);
                                // transaction.ListRemoveAsync(redisKey, jobId, -1);
                                // transaction.RemoveFromList(key, jobId);
                            }
                        }

                        if (hasTags)
                        {
                            Commit(transaction);
                        }
                        redis.ScriptEvaluate(LuaScriptListRemove, new RedisKey[] {redisKey}, keysToRemove.Select(x=>(RedisValue)x).ToArray());
                        // using (var transaction = redis.CreateWriteTransaction())
                        // {
                        //     foreach (var jobId in keysToRemove)
                        //     {
                        //         //connection.ListRemoveAsync(_storage.GetRedisKey(key), value);
                        //         if (IsTagsKey(key))
                        //         {
                        //             transaction.RemoveFromSet(key, jobId);
                        //         }
                        //         else
                        //         {
                        //             transaction.RemoveFromList(key, jobId);
                        //         }
                        //     }
                        //
                        //     transaction.Commit();
                        // }
                    }
                    finally
                    {
                        redis.LockRelease(redisKeyLock, OwnerId);
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
        
        private void Commit(ITransaction transaction)
        {
            if (!transaction.Execute()) 
            {
                // RedisTransaction.Commit returns false only when
                // WATCH condition has been failed. So, we should 
                // re-play the transaction.

                int replayCount = 1;
                const int maxReplayCount = 3;
                while (!transaction.Execute())
                {
                    if (replayCount++ >= maxReplayCount)
                    {
                        throw new HangFireRedisException("Transaction commit was failed due to WATCH condition failure. Retry attempts exceeded.");
                    }
                }
            }
        }
    }
}
