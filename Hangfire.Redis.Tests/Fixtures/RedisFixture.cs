using Hangfire.Redis.StackExchange;
using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Threading;
using Xunit;

namespace Hangfire.Redis.StackExchange.Tests
{
    public class RedisFixture : IDisposable
    {
        public RedisStorage Storage { get; private set; }
        private Process RedisServer;

        private readonly string IP = "localhost";
        private readonly string Port;
        private const int Db = 1;

        public RedisFixture()
        {
            var rnd = new Random();
            Port = (rnd.Next(10000) + 6379).ToString();

            RedisServer = new Process();
            RedisServer.StartInfo.FileName = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) + "\\redis-server.exe";
            RedisServer.StartInfo.Arguments = "--maxheap 8000000 --port " + Port;
            RedisServer.StartInfo.CreateNoWindow = true;
            RedisServer.StartInfo.RedirectStandardOutput = true;
            RedisServer.StartInfo.UseShellExecute = false;
            RedisServer.Start();

            var Server = String.Format("{0}:{1}", IP, Port);
            Storage = new RedisStorage(Server + ",allowAdmin=true", Db);
            CleanRedisAttribute.Server = Storage.GetDatabase().Multiplexer.GetServer(Server); //Ugly
            CleanRedisAttribute.Db = Db;

        }

        public void Dispose()
        {
            Storage.Dispose();
            RedisServer.Kill();
            RedisServer.Dispose();
        }
    }

    //Unfortunately, Fixture Collections do not work with my test runner. Need to use ClassFixtures
    //[CollectionDefinition("Redis")]
    //public class RedisCollection : ICollectionFixture<RedisFixture>
    //{
    //}
}
