using Hangfire.Redis;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace Hangfire.Tags.Redis.Extensions
{
    using M = IServiceCollection;
    public static class GlobalConfigurationExtensions
    {
        public static M AddTagsService(this M services, string connectionString, RedisStorageOptions redisOptions = null)
        {
            services.AddTagsService(ConnectionMultiplexer.Connect(connectionString), redisOptions);
            return services;
        }

        public static M AddTagsService(this M services, IConnectionMultiplexer multiplexer, RedisStorageOptions redisOptions = null)
        {
            services.AddSingleton(multiplexer);
            services.AddTagsService(redisOptions);
            return services;
        }

        public static M AddTagsService(this M services, RedisStorageOptions redisOptions = null)
        {
            services.Configure<RedisStorageOptions>(x =>
            {
                x.Prefix = redisOptions?.Prefix ?? "hangfire:";
                x.SucceededListSize = redisOptions?.SucceededListSize ?? 9999;
                x.DeletedListSize = redisOptions?.DeletedListSize ?? 4999;
            });
            services.AddSingleton<ITagsService, TagsService>();
            return services;
        }
    }
}
