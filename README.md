# Hangfire.Tags.Redis

[Hangfire.Tags.Redis](https://www.nuget.org/packages/Hangfire.Tags.Redis)
[Hangfire.Tags.Redis.Extensions](https://www.nuget.org/packages/Hangfire.Tags.Redis.Extensions)

基于Hangfire和Hangfire.Tags的使用Redis做持久化。

**Dependencies**: 

- HangFire.Core 1.7.6
- HangFire.Redis.StackExchange 1.8.0
- Hangfire.Tags 0.8.0
- StackExchange.Redis  2.0.601

## Setup

In .NET Core's Startup.cs:

```
public void ConfigureServices(IServiceCollection services)
{
    var redis = ConnectionMultiplexer.Connect(connectionString);
    var options = new RedisStorageOptions
    {
        Prefix = "hangfire:",
        SucceededListSize = 9999,
        DeletedListSize = 4999,
        Db = redis.GetDatabase().Database
    };
    services.AddHangfire(config =>
    {
    	config.UseRedisStorage(redis, options);
    	config.UseHeartbeatPage(checkInterval: TimeSpan.FromSeconds(5));
        config.UseTagsWithRedis(redis, redisOptions: options);
    });
}
```

Otherwise,

```

var redis = ConnectionMultiplexer.Connect(connectionString);
var options = new RedisStorageOptions
{
    Prefix = "hangfire:",
    SucceededListSize = 9999,
    DeletedListSize = 4999,
    Db = redis.GetDatabase().Database
};
GlobalConfiguration.Configuration
    .UseRedisStorage(redis, options)
    .UseTagsWithRedis(redis, redisOptions: options);
```



### Additional options

定时清理Tags里的过期jobId，基于参数ExpiryCheckInterval定时清理.

初始化BackgroundJobServer时增加这个配置：

```
public void Configure(IApplicationBuilder app, IHostingEnvironment env)
{
    var redisOptions = new RedisStorageOptions
    {
        Prefix = "hangfire:",
        SucceededListSize = 9999,
        DeletedListSize = 4999,
        InvisibilityTimeout = TimeSpan.FromSeconds(10),
        ExpiryCheckInterval = TimeSpan.FromHours(1)
    };
    var additionalProcesses = new List<IBackgroundProcess>
    {
    	new ExpiredJobsWatcher(redisOptions)
    };
    var options = new BackgroundJobServerOptions
    {
        //ShutdownTimeout = TimeSpan.FromMinutes(30),
        //Queues = queues,
        WorkerCount = Math.Max(Environment.ProcessorCount * 5, workerCount)
    };
    app.UseHangfireServer(options, additionalProcesses: additionalProcesses);
}
```

Otherwise,

```
 var redisOptions = new RedisStorageOptions
 {
     Prefix = "hangfire:",
     SucceededListSize = 9999,
     DeletedListSize = 4999,
     InvisibilityTimeout = TimeSpan.FromSeconds(10),
     ExpiryCheckInterval = TimeSpan.FromHours(1)
 };
 var additionalProcesses = new List<IBackgroundProcess>
 {
 	new ExpiredJobsWatcher(redisOptions)
 };
 var options = new BackgroundJobServerOptions
 {
     //ShutdownTimeout = TimeSpan.FromMinutes(30),
     //Queues = queues,
     WorkerCount = Math.Max(Environment.ProcessorCount * 5, workerCount)
 };
 var server = new BackgroundJobServer(options, JobStorage.Current, additionalProcesses: additionalProcesses);
```



