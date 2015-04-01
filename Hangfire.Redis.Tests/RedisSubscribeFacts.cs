using System;
using System.Threading;
using Xunit;

namespace Hangfire.Redis.StackExchange.Tests
{
    [Collection("Redis")]
    public class RedisSubscribeFacts
    {

        private readonly RedisFixture fixture;

        public RedisSubscribeFacts(RedisFixture fixture)
        {
            this.fixture = fixture;
        }

        [Fact]
        public void ContinueAfterReceive()
        {
            var sub = fixture.Storage.GetSubscribe();
            var cancel = new CancellationTokenSource();

            bool ThrewOnCancel = false;

            var t = new Thread(() => ThrewOnCancel = WaitForJob(sub, cancel.Token));
            t.IsBackground = true;
            t.Start();

            sub.AnnounceJob();
            t.Join();
            sub.JobClaimed();
            Assert.True(!ThrewOnCancel);
        }

        [Fact]
        public void ThrowForShutdown()
        {
            var sub = fixture.Storage.GetSubscribe();
            var cancel = new CancellationTokenSource();

            bool ThrewOnCancel = false;

            var t = new Thread(() => ThrewOnCancel = WaitForJob(sub, cancel.Token));
            t.IsBackground = true;
            t.Start();
            cancel.Cancel();

            t.Join();
            Assert.True(ThrewOnCancel);
        }

        private bool WaitForJob(RedisSubscribe sub, CancellationToken token)
        {
            try
            {
                sub.WaitForJob(token);
            }
            catch (OperationCanceledException)
            {
                return true;
            }
            return false;
        }
    }
}
