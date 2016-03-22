using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using Xunit;

namespace Hangfire.Redis.StackExchange.Tests
{
    public class RedisFixture : IDisposable
    {
        public RedisStorage Storage { get; private set; }
        private Process RedisServer;

        private readonly string IP = "localhost";
        private readonly string Port;
        public string ServerInfo
        {
            get
            {
                return String.Format("{0}:{1}", IP, Port);
            }
        }
        private const int Db = 1;

        public RedisFixture()
        {
            var rnd = new Random();
            Port = (rnd.Next(10000) + 6379).ToString();

            string assemblyFile = new DirectoryInfo(new Uri(Assembly.GetExecutingAssembly().CodeBase).AbsolutePath).Parent.FullName;

            RedisServer = new Process();
            RedisServer.StartInfo.FileName = assemblyFile + "//redis-server.exe";
            RedisServer.StartInfo.Arguments = "--port " + Port;
            RedisServer.StartInfo.UseShellExecute = false;
            RedisServer.Start();

            var Server = String.Format("{0}:{1}", IP, Port);
            Storage = new RedisStorage(Server + ",allowAdmin=true", new RedisStorageOptions { Db = Db });
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

    [CollectionDefinition("Redis")]
    public class RedisCollection : ICollectionFixture<RedisFixture>
    {
    }
}
