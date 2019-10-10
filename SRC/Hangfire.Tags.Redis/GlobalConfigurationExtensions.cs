using Hangfire.Redis;
using StackExchange.Redis;

namespace Hangfire.Tags.Redis
{
    public static class GlobalConfigurationExtensions
    {
        /// <summary>
        /// Configures Hangfire to use Tags.
        /// </summary>
        /// <param name="configuration">Global configuration</param>
        /// <param name="connectionMultiplexer"></param>
        /// <param name="options">Options for tags</param>
        /// <param name="redisOptions">Options for mysql storage</param>
        /// <returns></returns>
        public static IGlobalConfiguration UseTagsWithRedis(this IGlobalConfiguration configuration, IConnectionMultiplexer connectionMultiplexer, TagsOptions options = null, RedisStorageOptions redisOptions = null)
        {
            options = options ?? new TagsOptions();
            redisOptions = redisOptions ?? new RedisStorageOptions()
            {
                Db = connectionMultiplexer.GetDatabase().Database
            };

            GlobalStateHandlers.Handlers.Add(new SucceededStateHandler(redisOptions));
            GlobalStateHandlers.Handlers.Add(new DeletedStateHandler(redisOptions));
            GlobalStateHandlers.Handlers.Add(new FailedStateHandler(redisOptions));
            GlobalStateHandlers.Handlers.Add(new ScheduledStateHandler(redisOptions));
            GlobalStateHandlers.Handlers.Add(new EnqueuedStateHandler(redisOptions));
            GlobalStateHandlers.Handlers.Add(new ProcessingStateHandler(redisOptions));
            GlobalStateHandlers.Handlers.Add(new AwaitingStateHandler(redisOptions));

            options.Storage = new RedisTagsServiceStorage(connectionMultiplexer, redisOptions);
            return configuration.UseTags(options);
        }
    }
}
