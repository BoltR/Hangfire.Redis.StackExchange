using Hangfire.Dashboard;
using Hangfire.Logging;
using NSubstitute;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
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

        [Fact, CleanRedis]
        public void GetMonitoringAPI()
        {
            var api = Redis.Storage.GetMonitoringApi();
            var queues = api.Queues();
            Assert.Equal(0, queues.Count);
        }

        [Fact]
        public void GetInfo()
        {
            var DashboardMetric = Redis.Storage.GetDashboardInfo("Version", "redis_version");
            Assert.Equal("Version", DashboardMetric.Title);
            Assert.Equal("3.0.501", DashboardMetric.Func(null).Value);
        }

        [Fact]
        public void GetInfo_NotFound()
        {
            var DashboardMetric = Redis.Storage.GetDashboardInfo("None", "null");
            var Metric = DashboardMetric.Func(null);
            Assert.Equal("Key not found", Metric.Value);
            Assert.Equal(MetricStyle.Danger, Metric.Style);
        }

        [Fact]
        public void GetInfo_Threaded()
        {
            DashboardMetric Result1 = null;
            DashboardMetric Result2 = null;

            var t = new Thread(() => Result1 = Redis.Storage.GetDashboardInfo("Version", "redis_version"));
            var t2 = new Thread(() => Result2 = Redis.Storage.GetDashboardInfo("Blocked Clients", "blocked_clients"));
            t.IsBackground = true;
            t2.IsBackground = true;
            t.Start();
            t2.Start();

            t.Join();
            t2.Join();
            Assert.Equal("3.0.501", Result1.Func(null).Value);
            Assert.Equal("0", Result2.Func(null).Value);
        }
    }
}
