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
            if (redisOptions is null)
            {
                services.Configure<RedisStorageOptions>(x =>
                {
                    x.Prefix = "hangfire:";
                    x.SucceededListSize = 9999;
                    x.DeletedListSize = 4999;
                    x.Db = multiplexer.GetDatabase().Database;
                });
            }
            else
            {
                services.Configure<RedisStorageOptions>(x => x = redisOptions);
            }
            services.AddSingleton<ITagsService, TagsService>();
            return services;
        }
    }
}
