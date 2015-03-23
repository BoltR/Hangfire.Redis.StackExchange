using StackExchange.Redis;
using System;
using Xunit;

namespace Hangfire.Redis.StackExchange.Tests
{
    public class RedisMonitoringApiFacts : IClassFixture<RedisFixture>
    {

        private static RedisFixture _Redis;

        public RedisMonitoringApiFacts(RedisFixture Redis)
        {
            _Redis = Redis;
        }

        [Fact, CleanRedis]
        public void ProcessingJobs()
        {
            var t = new RedisMonitoringApi(_Redis.Storage);
            var Redis = _Redis.Storage.GetDatabase();
            Redis.SortedSetAdd("hangfire:processing", 1, 1);
            Redis.SortedSetAdd("hangfire:processing", 2, 2);

            for (int i = 1; i <= 3; i++)
            {
                Redis.HashSet(String.Format("hangfire:job:{0}", i), new HashEntry[] {
                    new HashEntry("Type", "System.Threading.Thread, mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089"),
                    new HashEntry("Method", "Sleep"),
                    new HashEntry("ParameterTypes", @"[""System.Int32, mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089""]"),
                    new HashEntry("Arguments", "1"),
                });

                Redis.HashSet(String.Format("hangfire:job:{0}:state", i), new HashEntry[] {
                    new HashEntry("StartedAt", "2015-03-17T14:55:30.3674415Z"),
                    new HashEntry("ServerName", "Test"),
                    new HashEntry("ServerId", "1"),
                    new HashEntry("State", "Processing")
                });
            }
            
            Assert.Equal(2, t.ProcessingCount());

            var p = t.ProcessingJobs(0, 5);
            Assert.Equal(2, p.Count);
            Assert.Equal("1", p[0].Value.ServerId);
            Assert.Equal(true, p[0].Value.InProcessingState);

        }

        [Fact, CleanRedis]
        public void SchedledJobs()
        {
            var t = new RedisMonitoringApi(_Redis.Storage);
            var Redis = _Redis.Storage.GetDatabase();

            var id = Guid.NewGuid().ToString();

            Redis.SortedSetAdd("hangfire:schedule", id, 0);

            Redis.HashSet(String.Format("hangfire:job:{0}", id), new HashEntry[] {
                new HashEntry("Type", "Test"),
                new HashEntry("Method", "Test"),
                new HashEntry("ParameterTypes", "Test"),
                new HashEntry("Arguments", "Blag")
            });

            Redis.HashSet(String.Format("hangfire:job:{0}:state", id), new HashEntry[] {
                new HashEntry("", "")
            });

            Assert.Equal(1, t.ScheduledCount());

            var p = t.ScheduledJobs(0, 2);
            Assert.Equal(1, p.Count);

        }

        [Fact, CleanRedis]
        public void FailedJobs()
        {
            var t = new RedisMonitoringApi(_Redis.Storage);

            var Redis = _Redis.Storage.GetDatabase();

            var id = Guid.NewGuid().ToString();

            Redis.SortedSetAdd("hangfire:failed", id, 0);

            Redis.HashSet(String.Format("hangfire:job:{0}", id), new HashEntry[] {
                new HashEntry("Type", "Test"),
                new HashEntry("Method", "Test"),
                new HashEntry("ParameterTypes", "Test"),
                new HashEntry("Arguments", "Blag")
            });

            Redis.HashSet(String.Format("hangfire:job:{0}:state", id), new HashEntry[] {
                new HashEntry("", "")
            });

            Assert.Equal(1, t.FailedCount());

            var p = t.FailedJobs(0, 2);
            Assert.Equal(1, p.Count);

        }

        [Fact, CleanRedis]
        public void Servers()
        {
            var t = new RedisMonitoringApi(_Redis.Storage);

            var Redis = _Redis.Storage.GetDatabase();
            var b = t.Servers();
            t.GetStatistics();
        }

        [Fact, CleanRedis]
        public void Queues()
        {
            var t = new RedisMonitoringApi(_Redis.Storage);
            var Redis = _Redis.Storage.GetDatabase();
            Redis.SetAdd("hangfire:queues", "test");
            Redis.ListLeftPush("hangfire:queue:test", new RedisValue[] {
                 "1", "2", "3" 
            });

            for (int i = 1; i <= 3; i++)
            {
                Redis.HashSet(i.ToString(), new HashEntry[] {
                    new HashEntry("Type", "System.Threading.Thread, mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089"),
                    new HashEntry("Method", "Sleep"),
                    new HashEntry("ParameterTypes", @"[""System.Int32, mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089""]"),
                    new HashEntry("Arguments", "1")
                });
            }
            var queue = t.Queues();
            Assert.Equal(1, queue.Count);
            Assert.Equal(3, queue[0].Length);
        }

        [Fact, CleanRedis]
        public void Fetched()
        {
            var t = new RedisMonitoringApi(_Redis.Storage);
            var Redis = _Redis.Storage.GetDatabase();
            Redis.SetAdd("hangfire:queues", "test");
            Redis.ListLeftPush("hangfire:queue:test:dequeued", new RedisValue[] {
                 "1", "2", "3" 
            });

            for (int i = 1; i <= 3; i++)
            {
                Redis.HashSet(String.Format("hangfire:job:{0}", i), new HashEntry[] {
                    new HashEntry("Type", "System.Threading.Thread, mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089"),
                    new HashEntry("Method", "Sleep"),
                    new HashEntry("ParameterTypes", @"[""System.Int32, mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089""]"),
                    new HashEntry("Arguments", "1"),
                    new HashEntry("State", "Processing"),
                    new HashEntry("Fetched", "2015-03-17T14:55:30.3674415Z")
                });
            }
            var b = t.FetchedJobs("test", 0, 5);
            Assert.Equal(3, b.Count);
            Assert.Equal("3", b[0].Key);
            Assert.Equal("Processing", b[0].Value.State);
        }

    }
}
