using StackExchange.Redis;
using System;
using Xunit;

namespace Hangfire.Redis.StackExchange.Tests
{
    public class RedisMonitoringApiFacts : IClassFixture<RedisFixture>
    {

        private readonly RedisFixture Redis;
        private readonly RedisMonitoringApi Monitor;

        public RedisMonitoringApiFacts(RedisFixture Redis)
        {
            this.Redis = Redis;
            Monitor = new RedisMonitoringApi(Redis.Storage);
        }

        [Fact, CleanRedis]
        public void ProcessingJobs()
        {
            UseRedis(redis =>
            {
                redis.SortedSetAdd("hangfire:processing", 1, 1);
                redis.SortedSetAdd("hangfire:processing", 2, 2);

                for (int i = 1; i <= 3; i++)
                {
                    redis.HashSet(String.Format("hangfire:job:{0}", i), new HashEntry[] {
                    new HashEntry("Type", "System.Threading.Thread, mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089"),
                    new HashEntry("Method", "Sleep"),
                    new HashEntry("ParameterTypes", @"[""System.Int32, mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089""]"),
                    new HashEntry("Arguments", "1"),
                    new HashEntry("State", "Processing")
                });

                    redis.HashSet(String.Format("hangfire:job:{0}:state", i), new HashEntry[] {
                    new HashEntry("StartedAt", "2015-03-17T14:55:30.3674415Z"),
                    new HashEntry("ServerName", "Test"),
                    new HashEntry("ServerId", "1")
                });
                }

            });
            Assert.Equal(2, Monitor.ProcessingCount());

            var p = Monitor.ProcessingJobs(0, 5);
            Assert.Equal(2, p.Count);
            Assert.Equal("1", p[0].Value.ServerId);
            Assert.Equal(true, p[1].Value.InProcessingState);
        }

        [Fact, CleanRedis]
        public void SchedledJobs()
        {
            UseRedis(redis =>
            {
                var id = Guid.NewGuid().ToString();

                redis.SortedSetAdd("hangfire:schedule", id, 0);

                redis.HashSet(String.Format("hangfire:job:{0}", id), new HashEntry[] {
                    new HashEntry("Type", "Test"),
                    new HashEntry("Method", "Test"),
                    new HashEntry("ParameterTypes", "Test"),
                    new HashEntry("Arguments", "Blag")
                });

                redis.HashSet(String.Format("hangfire:job:{0}:state", id), new HashEntry[] {
                    new HashEntry("", "")
                });
            });

            Assert.Equal(1, Monitor.ScheduledCount());

            var p = Monitor.ScheduledJobs(0, 2);
            Assert.Equal(1, p.Count);

        }

        [Fact, CleanRedis]
        public void FailedJobs()
        {
            UseRedis(redis =>
            {
                var id = Guid.NewGuid().ToString();
                redis.SortedSetAdd("hangfire:failed", id, 0);

                redis.HashSet(String.Format("hangfire:job:{0}", id), new HashEntry[] {
                    new HashEntry("Type", "Test"),
                    new HashEntry("Method", "Test"),
                    new HashEntry("ParameterTypes", "Test"),
                    new HashEntry("Arguments", "Blag")
                });

                redis.HashSet(String.Format("hangfire:job:{0}:state", id), new HashEntry[] {
                    new HashEntry("", "")
                });
            });

            Assert.Equal(1, Monitor.FailedCount());

            var p = Monitor.FailedJobs(0, 2);
            Assert.Equal(1, p.Count);

        }

        [Fact, CleanRedis]
        public void Servers()
        {
            var b = Monitor.Servers();
            Monitor.GetStatistics();
        }

        [Fact, CleanRedis]
        public void Queues()
        {
            UseRedis(redis =>
            {
                redis.SetAdd("hangfire:queues", "test");
                redis.ListLeftPush("hangfire:queue:test", new RedisValue[] {
                     "1", "2", "3" 
                });

                for (int i = 1; i <= 3; i++)
                {
                    redis.HashSet(i.ToString(), new HashEntry[] {
                        new HashEntry("Type", "System.Threading.Thread, mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089"),
                        new HashEntry("Method", "Sleep"),
                        new HashEntry("ParameterTypes", @"[""System.Int32, mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089""]"),
                        new HashEntry("Arguments", "1")
                    });
                }
            });

            var queue = Monitor.Queues();
            Assert.Equal(1, queue.Count);
            Assert.Equal(3, queue[0].Length);
        }

        [Fact, CleanRedis]
        public void Fetched()
        {
            UseRedis(redis =>
            {
                redis.SetAdd("hangfire:queues", "test");
                redis.ListLeftPush("hangfire:queue:test:dequeued", new RedisValue[] {
                     "1", "2", "3" 
                });

                for (int i = 1; i <= 3; i++)
                {
                    redis.HashSet(String.Format("hangfire:job:{0}", i), new HashEntry[] {
                        new HashEntry("Type", "System.Threading.Thread, mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089"),
                        new HashEntry("Method", "Sleep"),
                        new HashEntry("ParameterTypes", @"[""System.Int32, mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089""]"),
                        new HashEntry("Arguments", "1"),
                        new HashEntry("State", "Processing"),
                        new HashEntry("Fetched", "2015-03-17T14:55:30.3674415Z")
                    });
                }
            });

            var b = Monitor.FetchedJobs("test", 0, 5);
            Assert.Equal(3, b.Count);
            Assert.Equal("3", b[0].Key);
            Assert.Equal("Processing", b[0].Value.State);
        }

        private void UseRedis(Action<IDatabase> action)
        {
            action(Redis.Storage.GetDatabase());
        }

    }
}
