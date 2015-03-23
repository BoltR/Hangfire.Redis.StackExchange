using Hangfire.Redis.StackExchange;
using System;

namespace Hangfire.Redis.StackExchange.Tests
{
    public class RedisFixture : IDisposable
    {
        public RedisStorage Storage { get; private set; }

        private const string IP = "10.1.1.24:6379";
        private const int Db = 1;

        public RedisFixture()
        {
            Storage = new RedisStorage(IP + ",allowAdmin=true", Db);
            CleanRedisAttribute.Server = Storage.GetDatabase().Multiplexer.GetServer(IP); //Ugly
            CleanRedisAttribute.Db = Db;
        }

        public void Dispose()
        {
            Storage.Dispose();
        }
    }
}
