using System;

namespace Hangfire.Tags.Redis
{
    public static class RedisTagsKeyInfo
    {
        /// <summary>
        /// statistics:
        /// </summary>
        public static readonly string Prefix = "statistics:";

        public static string GetRetryKey(string tagCode) => $"{Prefix}{tagCode.ToLower()}:retries";

        public static string GetSucceededKey(string tagCode) => $"{Prefix}{tagCode.ToLower()}:succeeded";
        public static string GetDeletedKey(string tagCode) => $"{Prefix}{tagCode.ToLower()}:deleted";
        public static string GetFailedKey(string tagCode) => $"{Prefix}{tagCode.ToLower()}:failed";
        public static string GetScheduledKey(string tagCode) => $"{Prefix}{tagCode.ToLower()}:scheduled";

        public static string GetAwaitingKey(string tagCode) => $"{Prefix}{tagCode.ToLower()}:awaiting";
        public static string GetEnqueuedKey(string tagCode) => $"{Prefix}{tagCode.ToLower()}:enqueued";
        public static string GetProcessingKey(string tagCode) => $"{Prefix}{tagCode.ToLower()}:processing";

        public static string GetStatsSucceededKey(string tagCode) => $"{Prefix}{tagCode.ToLower()}:stats:succeeded";
        public static string GetStatsDeletedKey(string tagCode) => $"{Prefix}{tagCode.ToLower()}:stats:deleted";

        public static string GetStatsSucceededDateKey(string key, DateTime date) => $"{Prefix}{key.ToLower()}:stats:succeeded:{date.ToString("yyyy-MM-dd")}";
        public static string GetStatsFailedDateKey(string key, DateTime date) => $"{Prefix}{key.ToLower()}:stats:failed:{date.ToString("yyyy-MM-dd")}";

        public static string GetStatsSucceededHourKey(string key, DateTime date) => $"{Prefix}{key.ToLower()}:stats:succeeded:{date.ToString("yyyy-MM-dd-HH")}";
        public static string GetStatsFailedHourKey(string key, DateTime date) => $"{Prefix}{key.ToLower()}:stats:failed:{date.ToString("yyyy-MM-dd-HH")}";

        public static string GetStatsSucceededMinuteKey(string key, DateTime date) => $"{Prefix}{key.ToLower()}:stats:succeeded:{date.ToString("yyyy-MM-dd-HH-mm")}";
        public static string GetStatsFailedMinuteKey(string key, DateTime date) => $"{Prefix}{key.ToLower()}:stats:failed:{date.ToString("yyyy-MM-dd-HH-mm")}";

        public static string GetStatsSucceededDateKey(string tagCode, string server, DateTime date) => $"{Prefix}{tagCode.ToLower()}:{server.ToLower()}:stats:succeeded:{date.ToString("yyyy-MM-dd")}";
        public static string GetStatsFailedDateKey(string tagCode, string server, DateTime date) => $"{Prefix}{tagCode.ToLower()}:{server.ToLower()}:stats:failed:{date.ToString("yyyy-MM-dd")}";

        public static string GetStatsSucceededHourKey(string tagCode, string server, DateTime date) => $"{Prefix}{tagCode.ToLower()}:{server.ToLower()}:stats:succeeded:{date.ToString("yyyy-MM-dd-HH")}";
        public static string GetStatsFailedHourKey(string tagCode, string server, DateTime date) => $"{Prefix}{tagCode.ToLower()}:{server.ToLower()}:stats:failed:{date.ToString("yyyy-MM-dd-HH")}";

        public static string GetStatsSucceededMinuteKey(string tagCode, string server, DateTime date) => $"{Prefix}{tagCode.ToLower()}:{server.ToLower()}:stats:succeeded:{date.ToString("yyyy-MM-dd-HH-mm")}";
        public static string GetStatsFailedMinuteKey(string tagCode, string server, DateTime date) => $"{Prefix}{tagCode.ToLower()}:{server.ToLower()}:stats:failed:{date.ToString("yyyy-MM-dd-HH-mm")}";

        public static string GetJobKey(string jobId) => $"tags:{jobId}";


        public static string GetRecurringJobKey(string tagCode) => $"{Prefix}{ tagCode.ToLower()}:recurring-jobs";
        public static string GetRecurringJobId(string tagCode, string recurringJobId) => $"{Prefix}{ tagCode.ToLower()}:recurring-jobs:{recurringJobId}";
    }
}
