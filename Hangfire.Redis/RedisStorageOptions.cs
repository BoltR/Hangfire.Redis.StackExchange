using System;

namespace Hangfire.Redis.StackExchange
{
    public class RedisStorageOptions
    {
        public int Db { get; set; }
        public string Prefix { get; set; }
        public TimeSpan InvisibilityTimeout { get; set; }

        internal const string DefaultPrefix = "hangfire:";
        private const int DefaultDatabase = 0;

        public RedisStorageOptions()
        {
            Db = DefaultDatabase;
            Prefix = DefaultPrefix;
            InvisibilityTimeout = TimeSpan.FromMinutes(30);
        }
    }
}
