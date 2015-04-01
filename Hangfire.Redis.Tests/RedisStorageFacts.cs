using Hangfire.Logging;
using NSubstitute;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;

namespace Hangfire.Redis.StackExchange.Tests
{
    [Collection("Redis")]
    public class RedisStorageFacts
    {

        private readonly RedisFixture Redis;
        public RedisStorageFacts(RedisFixture Redis) 
        {
            this.Redis = Redis;
        } 

        [Fact]
        public void DefaultCtor_InitializesCorrectDefaultValues()
        {
            using (var storage = new RedisStorage())
            {
                Assert.Equal(0, storage.Db);
            }
        }

        [Fact]
        public void GetStateHandlers_ReturnsAllHandlers()
        {
            var handlers = Redis.Storage.GetStateHandlers();

            var handlerTypes = handlers.Select(x => x.GetType()).ToArray();
            Assert.Contains(typeof(FailedStateHandler), handlerTypes);
            Assert.Contains(typeof(ProcessingStateHandler), handlerTypes);
            Assert.Contains(typeof(SucceededStateHandler), handlerTypes);
            Assert.Contains(typeof(DeletedStateHandler), handlerTypes);
        }

        [Fact]
        public void GetServers()
        {
            var server = Redis.Storage.ToString();
            var t = Regex.Match(server, @"redis://localhost:\d+/1");
            Assert.True(t.Success);
        }

        [Fact]
        public void GetOptions()
        {
            var logger = Substitute.For<ILog>();
            Redis.Storage.WriteOptionsToLog(logger);
            var received = logger.Received().Log(LogLevel.Info, null);
        }

        [Fact]
        public void GetComponents()
        {
            var Componentes = Redis.Storage.GetComponents();
            Assert.Equal(1, Componentes.Count());

            var JobWatcher = Componentes.First() as FetchedJobsWatcher;

            Assert.NotNull(JobWatcher);
        }

        [Fact]
        public void GetMonitoringAPI()
        {
            var api = Redis.Storage.GetMonitoringApi();
            var queues = api.Queues();
            Assert.Equal(0, queues.Count);
        }

    }
}
