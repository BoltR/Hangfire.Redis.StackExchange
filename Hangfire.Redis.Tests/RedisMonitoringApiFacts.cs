using Hangfire.Common;
using StackExchange.Redis;
using System;
using System.Reflection;
using System.Linq;
using Xunit;
using System.Linq.Expressions;

namespace Hangfire.Redis.StackExchange.Tests
{
    [Collection("Redis")]
    public class RedisMonitoringApiFacts
    {

        private readonly RedisFixture Redis;
        private readonly RedisMonitoringApi Monitor;
        private readonly string Prefix;

        public RedisMonitoringApiFacts(RedisFixture Redis)
        {
            this.Redis = Redis;
            Monitor = new RedisMonitoringApi(Redis.Storage);
            Prefix = Redis.Storage.Prefix;
        }

        [Fact, CleanRedis]
        public void ProcessingJobs()
        {
            var StartedAt = DateTime.UtcNow;
            var FunctionHash = FunctionToHashEntry(() => Console.WriteLine("Test"));

            UseRedis(redis =>
            {
                redis.SortedSetAdd(Prefix + "processing", 1, 1);
                redis.SortedSetAdd(Prefix + "processing", 2, 2);

                for (int i = 1; i <= 3; i++)
                {
                    redis.HashSet(String.Format(Prefix + "job:{0}", i), FunctionHash);

                    redis.HashSet(String.Format(Prefix + "job:{0}", i), "State", "Processing");

                    redis.HashSet(String.Format(Prefix + "job:{0}:state", i), new HashEntry[] {
                        new HashEntry("StartedAt", JobHelper.SerializeDateTime(StartedAt)),
                        new HashEntry("ServerName", "Test"),
                        new HashEntry("ServerId", "1")
                    });
                }

            });
            Assert.Equal(2, Monitor.ProcessingCount());

            var Processing = Monitor.ProcessingJobs(0, 5);
            Assert.Equal(2, Processing.Count);
            Assert.Equal("1", Processing[0].Value.ServerId);
            Assert.Equal(true, Processing[1].Value.InProcessingState);
            Assert.Equal(StartedAt, Processing[0].Value.StartedAt);
        }

        [Fact, CleanRedis]
        public void JobDetails()
        {
            var CreatedAt = DateTime.UtcNow;
            
            UseRedis(redis =>
            {
                redis.SortedSetAdd(Prefix + "processing", 1, 1);

                redis.HashSet(String.Format(Prefix + "job:{0}", 1), FunctionToHashEntry(() => Console.WriteLine("Test")));

                redis.HashSet(String.Format(Prefix + "job:{0}", 1), "CreatedAt", JobHelper.SerializeDateTime(CreatedAt));

                //TODO: Add history
            });

            var Job = Monitor.JobDetails("1");

            Assert.Equal(CreatedAt, Job.CreatedAt);
            Assert.Equal("Console", Job.Job.Type.Name);
            Assert.Equal(@"""Test""", Job.Job.Arguments[0]);

        }

        [Fact, CleanRedis]
        public void ScheduledJobs()
        {
            UseRedis(redis =>
            {
                var id = Guid.NewGuid().ToString();

                redis.SortedSetAdd(Prefix + "schedule", id, 0);

                redis.HashSet(String.Format(Prefix + "job:{0}", id), FunctionToHashEntry(() => Console.WriteLine("Test")));

                redis.HashSet(String.Format(Prefix + "job:{0}:state", id), new HashEntry[] {
                    new HashEntry("", "")
                });
            });

            Assert.Equal(1, Monitor.ScheduledCount());

            var Scheduled = Monitor.ScheduledJobs(0, 2);
            Assert.Equal(1, Scheduled.Count);

        }

        
        [Fact, CleanRedis]
        public void ScheduledJobs_Empty()
        {
            var p = Monitor.ScheduledJobs(0, 2);
            Assert.Equal(0, p.Count);
        }

        [Fact, CleanRedis]
        public void FailedJobs()
        {
            UseRedis(redis =>
            {
                var id = Guid.NewGuid().ToString();
                redis.SortedSetAdd(Prefix + "failed", id, 0);

                redis.HashSet(String.Format(Prefix + "job:{0}", id), FunctionToHashEntry(() => Console.WriteLine("Test")));
                redis.HashSet(String.Format(Prefix + "job:{0}:state", id), new HashEntry[] {
                    new HashEntry("", "")
                });
            });

            Assert.Equal(1, Monitor.FailedCount());

            var Failed = Monitor.FailedJobs(0, 2);
            Assert.Equal(1, Failed.Count);

        }

        [Fact, CleanRedis]
        public void DeletedJobs()
        {
            var DeletedTime = DateTime.UtcNow;
            UseRedis(redis =>
            {
                var id = Guid.NewGuid().ToString();
                redis.ListRightPush(Prefix + "deleted", id, 0);

                redis.HashSet(String.Format(Prefix + "job:{0}", id), FunctionToHashEntry(() => Console.WriteLine("Test")));
                redis.HashSet(String.Format(Prefix + "job:{0}:state", id), new HashEntry[] {
                    new HashEntry("DeletedAt", Hangfire.Common.JobHelper.SerializeDateTime(DeletedTime))
                });
            });

            var Deleted = Monitor.DeletedJobs(0, 2);
            Assert.Equal(1, Deleted.Count);
            Assert.Equal(DeletedTime, Deleted[0].Value.DeletedAt);

            Assert.Equal(1, Monitor.DeletedListCount());
        }

        [Fact, CleanRedis]
        public void SucceededJobs()
        {
            var SucceededTime = DateTime.UtcNow;
            UseRedis(redis =>
            {
                var id = Guid.NewGuid().ToString();
                redis.ListRightPush(Prefix + "succeeded", id, 0);

                redis.HashSet(String.Format(Prefix + "job:{0}", id), FunctionToHashEntry(() => Console.WriteLine("Test")));
                redis.HashSet(String.Format(Prefix + "job:{0}:state", id), new HashEntry[] {
                    new HashEntry("SucceededAt", Hangfire.Common.JobHelper.SerializeDateTime(SucceededTime))
                });
            });

            var Succeeded = Monitor.SucceededJobs(0, 2);
            Assert.Equal(1, Succeeded.Count);
            Assert.Equal(SucceededTime, Succeeded[0].Value.SucceededAt);

            Assert.Equal(1, Monitor.SucceededListCount());
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
            var QueuedTime = DateTime.UtcNow;
            UseRedis(redis =>
            {
                redis.SetAdd(Prefix + "queues", "test");
                redis.ListLeftPush(Prefix + "queue:test", new RedisValue[] {
                     "1", "2", "3" 
                });

                for (int i = 1; i <= 3; i++)
                {
                    redis.HashSet(i.ToString(), FunctionToHashEntry(() => Console.WriteLine("Test")));
                    if (i == 2)
                    {
                        redis.HashSet(i.ToString() + ":state", new HashEntry[] {
                            new HashEntry("EnqueuedAt", Hangfire.Common.JobHelper.SerializeDateTime(QueuedTime))
                        });
                    }

                }
            });

            var queue = Monitor.Queues();
            Assert.Equal(1, queue.Count);
            Assert.Equal(3, queue[0].Length);

            Assert.Equal(3, Monitor.EnqueuedCount("test"));
        }

        [Fact, CleanRedis]
        public void Fetched()
        {
            var FetchedDate = DateTime.UtcNow;
            var FunctionHash = FunctionToHashEntry(() => Console.WriteLine("Test"));
            UseRedis(redis =>
            {
                redis.SetAdd(Prefix + "queues", "test");
                redis.ListLeftPush(Prefix + "queue:test:dequeued", new RedisValue[] {
                     "1", "2", "3" 
                });

                for (int i = 1; i <= 3; i++)
                {
                    redis.HashSet(String.Format(Prefix + "job:{0}", i), FunctionHash);

                    redis.HashSet(String.Format(Prefix + "job:{0}", i), new HashEntry[] {
                        new HashEntry("State", "Processing"),
                        new HashEntry("Fetched", JobHelper.SerializeDateTime(FetchedDate))
                    });
                }
            });

            var Fetched = Monitor.FetchedJobs("test", 0, 5);
            Assert.Equal(3, Fetched.Count);
            Assert.Equal("3", Fetched[0].Key);
            Assert.Equal("Processing", Fetched[0].Value.State);
            Assert.Equal(FetchedDate, Fetched[0].Value.FetchedAt);

            Assert.Equal(3, Monitor.FetchedCount("test"));
        }

        private HashEntry[] FunctionToHashEntry(Expression<Action> Method)
        {

            var Call = Method.Body as MethodCallExpression;
            var Arguments = Call.Arguments.Select(x => JobHelper.ToJson((x as ConstantExpression).Value)).ToArray();

            return new HashEntry[] {
                    new HashEntry("Type", Call.Method.DeclaringType.AssemblyQualifiedName),
                    new HashEntry("Method", Call.Method.Name),
                    new HashEntry("ParameterTypes", JobHelper.ToJson(Call.Method.GetParameters().Select(x => x.ParameterType))),
                    new HashEntry("Arguments", JobHelper.ToJson(Arguments)),
            };
        }

        private void UseRedis(Action<IDatabase> action)
        {
            action(Redis.Storage.GetDatabase());
        }

    }
}
