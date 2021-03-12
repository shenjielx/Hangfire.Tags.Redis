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
        /// <param name="multiplexer"></param>
        /// <param name="options">Options for tags</param>
        /// <param name="redisOptions">Options for mysql storage</param>
        /// <returns></returns>
        public static IGlobalConfiguration UseTagsWithRedis(this IGlobalConfiguration configuration, IConnectionMultiplexer multiplexer, TagsOptions options = null, RedisStorageOptions redisOptions = null)
        {
            options = options ?? new TagsOptions();
            redisOptions = redisOptions ?? new RedisStorageOptions()
            {
                Db = multiplexer.GetDatabase().Database
            };

            GlobalStateHandlers.Handlers.Add(new SucceededStateHandler(redisOptions, multiplexer));
            GlobalStateHandlers.Handlers.Add(new DeletedStateHandler(redisOptions, multiplexer));
            GlobalStateHandlers.Handlers.Add(new FailedStateHandler(redisOptions, multiplexer));
            GlobalStateHandlers.Handlers.Add(new ScheduledStateHandler(redisOptions, multiplexer));
            GlobalStateHandlers.Handlers.Add(new EnqueuedStateHandler(redisOptions, multiplexer));
            GlobalStateHandlers.Handlers.Add(new ProcessingStateHandler(redisOptions, multiplexer));
            GlobalStateHandlers.Handlers.Add(new AwaitingStateHandler(redisOptions, multiplexer));

            GlobalJobFilters.Filters.Add(new AutomaticRetryFilter(redisOptions, multiplexer));

            options.Storage = new RedisTagsServiceStorage(multiplexer, redisOptions);
            return configuration.UseTags(options);
        }
    }
}
