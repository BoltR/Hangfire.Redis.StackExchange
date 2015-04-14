using StackExchange.Redis;
using System;

namespace Hangfire.Redis.StackExchange
{
    public static class RedisStorageExtensions
    {
        public static IGlobalConfiguration<RedisStorage> UseRedisStorage(this IGlobalConfiguration configuration, string OptionString)
        {
            if (configuration == null) throw new ArgumentNullException("configuration");
            if (OptionString == null) throw new ArgumentNullException("OptionString");
            var storage = new RedisStorage(OptionString);
            return configuration.UseStorage(storage);
        }

        public static IGlobalConfiguration<RedisStorage> UseRedisStorage(this IGlobalConfiguration configuration, string OptionString, int db)
        {
            if (configuration == null) throw new ArgumentNullException("configuration");
            if (OptionString == null) throw new ArgumentNullException("OptionString");
            var storage = new RedisStorage(OptionString, db);
            return configuration.UseStorage(storage);
        }

        public static IGlobalConfiguration<RedisStorage> UseRedisStorage(this IGlobalConfiguration configuration, ConfigurationOptions Options)
        {
            if (configuration == null) throw new ArgumentNullException("configuration");
            if (Options == null) throw new ArgumentNullException("Options");
            var storage = new RedisStorage(Options);
            return configuration.UseStorage(storage);
        }

        public static IGlobalConfiguration<RedisStorage> UseRedisStorage(this IGlobalConfiguration configuration, ConfigurationOptions Options, int db)
        {
            if (configuration == null) throw new ArgumentNullException("configuration");
            if (Options == null) throw new ArgumentNullException("Options");
            var storage = new RedisStorage(Options, db);
            return configuration.UseStorage(storage);
        }
    }
}
