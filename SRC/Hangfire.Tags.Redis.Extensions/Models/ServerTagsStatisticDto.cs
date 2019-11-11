using System;
using System.Collections.Generic;

namespace Hangfire.Tags.Redis.Extensions
{
    public sealed class ServerTagsStatisticDto
    {
        public string Server { get; set; }
        public string TagCode { get; set; }

        public IDictionary<DateTime, long> Statistics { get; set; }
    }
}
