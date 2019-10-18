using System;
using Hangfire.Common;

namespace Hangfire.Tags.Redis.Extensions
{
    public class RetriesJobDto
    {
        public Job Job { get; set; }
        public string State { get; set; }
        public string Reason { get; set; }
        public DateTime? EnqueueAt { get; set; }
        public int RetryCount { get; set; }
        public DateTime CreatedAt { get; set; }
    }
}
