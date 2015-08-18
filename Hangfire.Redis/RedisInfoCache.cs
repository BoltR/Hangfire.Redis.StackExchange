using System.Collections.Generic;

namespace Hangfire.Redis.StackExchange
{
    internal class RedisInfoCache
    {
        public int LastUpdateTime { get; set; }
        public Dictionary<string, string> InfoLookup { get; set; }

        public RedisInfoCache()
        {
            InfoLookup = new Dictionary<string, string>();
        }
    }
}
