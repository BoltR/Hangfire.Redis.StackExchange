using Hangfire.Common;
using StackExchange.Redis;
using System;
using System.Linq;
using Xunit;
using System.Linq.Expressions;
using System.Collections.Generic;

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

                    redis.HashSet(String.Format(Prefix + "job:{0}:state", i), new HashEntry[] {
                        new HashEntry("StartedAt", JobHelper.SerializeDateTime(StartedAt)),
                        new HashEntry("ServerName", "Test"),
                        new HashEntry("ServerId", "1"),
                        new HashEntry("State", "Processing")
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


                var SerializedCreated = JobHelper.SerializeDateTime(CreatedAt);

                redis.HashSet(String.Format(Prefix + "job:{0}", 1), "CreatedAt", SerializedCreated);


                var HistoryItem = new Dictionary<string, string>()
                {
                    { "EnqueuedAt" ,"2015-05-20T16:02:53.1060098Z" },
                    { "Queue", "default" },
                    { "State", "Enqueued" },
                    { "Reason", null },
                    { "CreatedAt", SerializedCreated }
                };
                redis.ListRightPush(String.Format(Prefix + "job:{0}:history", 1), JobHelper.ToJson(HistoryItem));

                HistoryItem = new Dictionary<string, string>()
                {
                    { "StartedAt", JobHelper.SerializeDateTime(CreatedAt.AddMilliseconds(30)) },
                    { "ServerId", Redis.ServerInfo },
                    { "WorkerNumber", "2" },
                    { "State", "Processing" },
                    { "Reason", null },
                    { "CreatedAt", SerializedCreated}
                };
                redis.ListRightPush(String.Format(Prefix + "job:{0}:history", 1), JobHelper.ToJson(HistoryItem));

                HistoryItem = new Dictionary<string, string>()
                {
                    { "SucceededAt", JobHelper.SerializeDateTime(CreatedAt.AddMilliseconds(40)) },
                    { "PerformanceDuration", "10" },
                    { "Latency", "5" },
                    { "State", "Succeeded" },
                    { "Reason", null },
                    { "CreatedAt", SerializedCreated}
                };
                redis.ListRightPush(String.Format(Prefix + "job:{0}:history", 1), JobHelper.ToJson(HistoryItem));
            });

            var Job = Monitor.JobDetails("1");

            Assert.Equal(CreatedAt, Job.CreatedAt);
            Assert.Equal("Console", Job.Job.Type.Name);
            Assert.Equal("Test", Job.Job.Args[0]);

            Assert.Equal(3, Job.History.Count);
            Assert.Equal("Enqueued", Job.History[0].StateName);
            Assert.Equal(CreatedAt, Job.History[2].CreatedAt);

        }

        [Fact, CleanRedis]
        public void ScheduledJobs()
        {
            var ScheduledAt = DateTime.UtcNow;
            UseRedis(redis =>
            {
                var id = Guid.NewGuid().ToString();

                redis.SortedSetAdd(Prefix + "schedule", id, 0);

                redis.HashSet(String.Format(Prefix + "job:{0}", id), FunctionToHashEntry(() => Console.WriteLine("Test")));

                redis.HashSet(String.Format(Prefix + "job:{0}:state", id), new HashEntry[] {
                        new HashEntry("ScheduledAt", JobHelper.SerializeDateTime(ScheduledAt)),
                        new HashEntry("ServerName", "Test"),
                        new HashEntry("ServerId", "1"),
                        new HashEntry("State", "Scheduled")
                });
            });

            Assert.Equal(1, Monitor.ScheduledCount());

            var Scheduled = Monitor.ScheduledJobs(0, 2);
            Assert.Equal(1, Scheduled.Count);
            Assert.True(Scheduled[0].Value.InScheduledState);
            Assert.Equal(ScheduledAt, Scheduled[0].Value.ScheduledAt);
        }

        
        [Fact, CleanRedis]
        public void ScheduledJobs_Empty()
        {
            var p = Monitor.ScheduledJobs(0, 2);
            Assert.Equal(0, p.Count);
        }

        [Fact, CleanRedis]
        public void SucceededByDatesCount()
        {
            var SucceededTime = DateTime.UtcNow;
            UseRedis(redis =>
            {
                redis.StringIncrement(Prefix + "stats:failed:" + SucceededTime.ToString("yyyy-MM-dd"));
                redis.StringIncrement(Prefix + "stats:succeeded:" + SucceededTime.ToString("yyyy-MM-dd"));
                redis.StringIncrement(Prefix + "stats:succeeded:" + SucceededTime.ToString("yyyy-MM-dd"));
                redis.StringIncrement(Prefix + "stats:succeeded:" + SucceededTime.AddDays(-2).ToString("yyyy-MM-dd"));
            });

            var p = Monitor.SucceededByDatesCount();
            Assert.Equal(8, p.Count);
            Assert.Equal(3, p.Sum(x => x.Value));
            Assert.Equal(2, p.First().Value); //Today
        }

        [Fact, CleanRedis]
        public void FailedByDatesCount()
        {
            var SucceededTime = DateTime.UtcNow;
            UseRedis(redis =>
            {
                redis.StringIncrement(Prefix + "stats:succeeded:" + SucceededTime.ToString("yyyy-MM-dd"));
                redis.StringIncrement(Prefix + "stats:failed:" + SucceededTime.ToString("yyyy-MM-dd"));
                redis.StringIncrement(Prefix + "stats:failed:" + SucceededTime.ToString("yyyy-MM-dd"));
                redis.StringIncrement(Prefix + "stats:failed:" + SucceededTime.AddDays(-2).ToString("yyyy-MM-dd"));
            });

            var p = Monitor.FailedByDatesCount();
            Assert.Equal(8, p.Count);
            Assert.Equal(3, p.Sum(x => x.Value));
            Assert.Equal(2, p.First().Value); //Today
        }

        [Fact, CleanRedis]
        public void FailedJobs()
        {
            var FailedAt = DateTime.UtcNow;
            UseRedis(redis =>
            {
                var id = Guid.NewGuid().ToString();
                redis.SortedSetAdd(Prefix + "failed", id, 0);

                redis.HashSet(String.Format(Prefix + "job:{0}", id), FunctionToHashEntry(() => Console.WriteLine("Test")));
                redis.HashSet(String.Format(Prefix + "job:{0}:state", id), new HashEntry[] {
                        new HashEntry("FailedAt", JobHelper.SerializeDateTime(FailedAt)),
                        new HashEntry("ServerName", "Test"),
                        new HashEntry("ServerId", "1"),
                        new HashEntry("State", "Failed"),
                        new HashEntry("Reason", "UN"),

                });
            });

            Assert.Equal(1, Monitor.FailedCount());
            var Failed = Monitor.FailedJobs(0, 2);
            Assert.Equal(1, Failed.Count);
            Assert.True(Failed[0].Value.InFailedState);
            Assert.Equal(FailedAt, Failed[0].Value.FailedAt);
        }

        [Fact, CleanRedis]
        public void HourlySucceededJobs()
        {
            var SucceededTime = DateTime.UtcNow;
            UseRedis(redis =>
            {
                redis.StringIncrement(Prefix + "stats:failed:" + SucceededTime.ToString("yyyy-MM-dd-HH"));
                redis.StringIncrement(Prefix + "stats:succeeded:" + SucceededTime.ToString("yyyy-MM-dd-HH"));
                redis.StringIncrement(Prefix + "stats:succeeded:" + SucceededTime.ToString("yyyy-MM-dd-HH"));
                redis.StringIncrement(Prefix + "stats:succeeded:" + SucceededTime.AddHours(-2).ToString("yyyy-MM-dd-HH"));
            });

            var p = Monitor.HourlySucceededJobs();
            Assert.Equal(24, p.Count);
            Assert.Equal(3, p.Sum(x => x.Value));
            Assert.Equal(2, p.First().Value); //Today
        }

        [Fact, CleanRedis]
        public void HourlyFailedJobs()
        {
            var SucceededTime = DateTime.UtcNow;
            UseRedis(redis =>
            {
                redis.StringIncrement(Prefix + "stats:succeeded:" + SucceededTime.ToString("yyyy-MM-dd-HH"));
                redis.StringIncrement(Prefix + "stats:failed:" + SucceededTime.ToString("yyyy-MM-dd-HH"));
                redis.StringIncrement(Prefix + "stats:failed:" + SucceededTime.ToString("yyyy-MM-dd-HH"));
                redis.StringIncrement(Prefix + "stats:failed:" + SucceededTime.AddHours(-2).ToString("yyyy-MM-dd-HH"));
            });

            var p = Monitor.HourlyFailedJobs();
            Assert.Equal(24, p.Count);
            Assert.Equal(3, p.Sum(x => x.Value));
            Assert.Equal(2, p.First().Value); //Today
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
                    new HashEntry("DeletedAt", Hangfire.Common.JobHelper.SerializeDateTime(DeletedTime)),
                    new HashEntry("State", "Deleted")
                });
            });

            var Deleted = Monitor.DeletedJobs(0, 2);
            Assert.Equal(1, Deleted.Count);
            Assert.Equal(DeletedTime, Deleted[0].Value.DeletedAt);
            Assert.True(Deleted[0].Value.InDeletedState);

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
                    new HashEntry("SucceededAt", JobHelper.SerializeDateTime(SucceededTime)),
                    new HashEntry("State", "Succeeded")
                });
            });

            var Succeeded = Monitor.SucceededJobs(0, 2);
            Assert.Equal(1, Succeeded.Count);
            Assert.Equal(SucceededTime, Succeeded[0].Value.SucceededAt);
            Assert.True(Succeeded[0].Value.InSucceededState);

            Assert.Equal(1, Monitor.SucceededListCount());
        }

        [Fact, CleanRedis]
        public void Servers()
        {
            var StartedAt = DateTime.UtcNow.AddDays(-1);
            var Heartbeat = DateTime.UtcNow;
            UseRedis(redis =>
            {
                redis.SetAdd(Prefix + "servers", "server1");
                redis.SetAdd(Prefix + "servers", "server2");
                for (int i = 1; i <= 2; i++)
                {
                    redis.HashSet(Prefix + String.Format("server:server{0}", i), new HashEntry[] {
                        new HashEntry("WorkerCount", "20"),
                        new HashEntry("StartedAt", JobHelper.SerializeDateTime(StartedAt)),
                        new HashEntry("Heartbeat",  JobHelper.SerializeDateTime(Heartbeat))
                    });
                    redis.ListLeftPush(Prefix + String.Format("server:server{0}:queues", i), new RedisValue[]
                    {
                        "queue1",
                        "queue2",
                        "queue3"
                    });
                }
            });
            var Servers = Monitor.Servers();
            Assert.Equal(2, Servers.Count);
            Assert.Equal(StartedAt, Servers[0].StartedAt);
            Assert.Equal(Heartbeat, Servers[0].Heartbeat);
            Assert.Equal(3, Servers[0].Queues.Count);
        }

        [Fact, CleanRedis]
        public void Servers_Empty()
        {
            var Servers = Monitor.Servers();
            Assert.Equal(0, Servers.Count);
        }

        [Fact, CleanRedis]
        public void GetStatistics()
        {
            UseRedis(redis =>
            {
                redis.SetAdd(Prefix + "servers", "server1");
                redis.SetAdd(Prefix + "servers", "server2");
                redis.SetAdd(Prefix + "queues", "1");
                redis.SetAdd(Prefix + "queues", "2");
                var id = Guid.NewGuid().ToString();
                redis.ListRightPush(Prefix + "queue:1", id, 0);
                redis.ListRightPush(Prefix + "queue:2", id, 0);
                redis.ListRightPush(Prefix + "queue:1", "3", 0);
                redis.SortedSetAdd(Prefix + "schedule", new SortedSetEntry[] 
                {
                    new SortedSetEntry("job1", 0),
                });
                redis.SortedSetAdd(Prefix + "processing", new SortedSetEntry[] 
                {
                    new SortedSetEntry("job1", 0),
                    new SortedSetEntry("job2", 0),
                    new SortedSetEntry("job3", 0),
                });
                redis.StringSet(Prefix + "stats:succeeded", "32");
                redis.SortedSetAdd(Prefix + "failed", new SortedSetEntry[] 
                {
                    new SortedSetEntry("job1", 0),
                });
                redis.StringSet(Prefix + "stats:deleted", "5");
                redis.SortedSetAdd(Prefix + "recurring-jobs", new SortedSetEntry[] 
                {
                    new SortedSetEntry("job1", 0),
                    new SortedSetEntry("job2", 0),
                });
            });
            var Stats = Monitor.GetStatistics();

            Assert.Equal(2, Stats.Servers);
            Assert.Equal(2, Stats.Queues);
            Assert.Equal(3, Stats.Enqueued);
            Assert.Equal(1, Stats.Scheduled);
            Assert.Equal(3, Stats.Processing);
            Assert.Equal(32, Stats.Succeeded);
            Assert.Equal(1, Stats.Failed);
            Assert.Equal(5, Stats.Deleted);
            Assert.Equal(2, Stats.Recurring);
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
                        redis.HashSet(i.ToString(), "State", "Enqueued");
                        redis.HashSet(i.ToString() + ":state", new HashEntry[] {
                            new HashEntry("EnqueuedAt", JobHelper.SerializeDateTime(QueuedTime)),
                            new HashEntry("State", "Enqueued")
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
        public void EnqueuedJobs()
        {
            var EnqueuedAt = DateTime.UtcNow;
            UseRedis(redis =>
            {
                var id = Guid.NewGuid().ToString();
                redis.ListRightPush(Prefix + "queue:1", id, 0);

                redis.HashSet(String.Format(Prefix + "job:{0}", id), FunctionToHashEntry(() => Console.WriteLine("Test")));
                redis.HashSet(String.Format(Prefix + "job:{0}", id), "State", "Enqueued");
                redis.HashSet(String.Format(Prefix + "job:{0}:state", id), new HashEntry[] {
                    new HashEntry("EnqueuedAt", JobHelper.SerializeDateTime(EnqueuedAt)),
                    new HashEntry("State", "Enqueued")
                });
            });

            var Enqueued = Monitor.EnqueuedJobs("1", 0, 2);
            Assert.Equal(1, Enqueued.Count);
            Assert.Equal(EnqueuedAt, Enqueued[0].Value.EnqueuedAt);
            Assert.True(Enqueued[0].Value.InEnqueuedState);

            Assert.Equal(1, Monitor.EnqueuedCount("1"));
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
