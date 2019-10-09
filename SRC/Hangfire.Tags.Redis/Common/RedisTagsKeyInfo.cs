using System;

namespace Hangfire.Tags.Redis
{
    public static class RedisTagsKeyInfo
    {
        public static readonly string Prefix = "statistics:";

        public static string GetSucceededKey(string tagName) => $"{Prefix}{tagName}:succeeded";
        public static string GetDeletedKey(string tagName) => $"{Prefix}{tagName}:deleted";
        public static string GetFailedKey(string tagName) => $"{Prefix}{tagName}:failed";
        public static string GetScheduledKey(string tagName) => $"{Prefix}{tagName}:scheduled";

        public static string GetAwaitingKey(string tagName) => $"{Prefix}{tagName}:awaiting";
        public static string GetEnqueuedKey(string tagName) => $"{Prefix}{tagName}:enqueued";
        public static string GetProcessingKey(string tagName) => $"{Prefix}{tagName}:processing";

        public static string GetStatsSucceededKey(string tagName) => $"{Prefix}{tagName}:stats:succeeded";
        public static string GetStatsDeletedKey(string tagName) => $"{Prefix}{tagName}:stats:deleted";

        public static string GetStatsSucceededDateKey(string tagName, DateTime date) => $"{Prefix}{tagName}:stats:succeeded:{date.ToString("yyyy-MM-dd")}";
        public static string GetStatsFailedDateKey(string tagName, DateTime date) => $"{Prefix}{tagName}:stats:failed:{date.ToString("yyyy-MM-dd")}";

        public static string GetStatsSucceededHourKey(string tagName, DateTime date) => $"{Prefix}{tagName}:stats:succeeded:{date.ToString("yyyy-MM-dd-HH")}";
        public static string GetStatsFailedHourKey(string tagName, DateTime date) => $"{Prefix}{tagName}:stats:failed:{date.ToString("yyyy-MM-dd-HH")}";

        public static string GetJobKey(string jobId) => $"tags:{jobId}";
    }
}
