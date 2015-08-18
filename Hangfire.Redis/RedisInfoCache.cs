using System.Collections.Generic;

namespace Hangfire.Redis.StackExchange
{
    internal class RedisInfoCache
    {
        public int LastUpdateTime { get; set; }
        public Dictionary<string, string> InfoLookup { get; set; }
        public bool Uninitialized { get; set; }
        public readonly object Locker;

        public RedisInfoCache()
        {
            Locker = new object();
            InfoLookup = new Dictionary<string, string>();
            Uninitialized = true;
        }
    }
}
