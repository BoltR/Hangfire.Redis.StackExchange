using System.Collections.Generic;
using System.Threading;

namespace Hangfire.Redis.StackExchange
{
    internal class RedisInfoCache
    {
        public int LastUpdateTime { get; set; }
        public Dictionary<string, string> InfoLookup { get { return lookup; } }
        public object Locker { get { return locker; } }

        public readonly object locker;
        private Dictionary<string, string> lookup;

        public RedisInfoCache()
        {
            locker = new object();
        }

        public void UpdateInfo(Dictionary<string, string> NewInfoLookup)
        {
            Interlocked.Exchange(ref lookup, NewInfoLookup);
        }
    }
}
