using Hangfire.Storage.Monitoring;

namespace Hangfire.Tags.Redis.Extensions
{
    public sealed class TagsSucceededJobDto : SucceededJobDto
    {
        public long? Latency { get; set; }
        public long? Duration { get; set; }
    }
}
