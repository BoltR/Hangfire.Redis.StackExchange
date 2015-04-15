using Hangfire.Common;
using Hangfire.Storage;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Xunit;

namespace Hangfire.Redis.StackExchange.Tests
{
    [Collection("Redis")]
    public class RedisConnectionFacts
    {
        private readonly RedisFixture Redis;
        public RedisConnectionFacts(RedisFixture Redis) 
        {
            this.Redis = Redis;
        }

        [Fact, CleanRedis]
        public void AcquireDistributedLock_LockCollision()
        {
            using (var lock1 = Redis.Storage.GetConnection().AcquireDistributedLock("some-hash", TimeSpan.FromMinutes(1)))
            {
                Assert.Throws<AccessViolationException>(() => Redis.Storage.GetConnection().AcquireDistributedLock("some-hash", TimeSpan.FromMinutes(1)));
            }
        }

        [Fact, CleanRedis]
        public void CreateTransaction_AndCommit()
        {
            UseConnections((redis, connection) =>
            {
                var transaction = connection.CreateWriteTransaction();
                transaction.AddToSet("some-set", "test");
                transaction.Commit();
                var setitems = redis.SortedSetScan("hangfire:some-set");
                Assert.Equal(1, setitems.Count());
                Assert.Equal("test", setitems.First().Element);
            });
        }

        [Fact, CleanRedis]
        public void FetchNextJob_JobWaiting()
        {
            var cancel = new CancellationTokenSource();
            IFetchedJob Job = null;
            UseConnections((redis, connection) =>
            {
                redis.ListRightPush("hangfire:queue:1", "job1");
                var t = new Thread(() => Job = connection.FetchNextJob(new string[] { "1" }, cancel.Token));
                t.IsBackground = true;
                t.Start();
                t.Join();

                Assert.Equal("job1", Job.JobId);
            });

        }

        [Fact, CleanRedis]
        public void FetchNextJob_WithPub()
        {
            var cancel = new CancellationTokenSource();
            IFetchedJob Job = null;
            UseConnections((redis, connection) =>
            {
                var t = new Thread(() => Job = connection.FetchNextJob(new string[] { "1" }, cancel.Token));
                t.IsBackground = true;
                t.Start();
                Thread.Sleep(10); //Enough time for Redis to respond that there are no jobs in queue, and the thread to start waiting
                redis.ListRightPush("hangfire:queue:1", "job2");
                redis.Publish("Hangfire:announce", "1"); //Pub to wake up thread
                t.Join();

                Assert.Equal("job2", Job.JobId);
            });

        }


        [Fact, CleanRedis]
        public void FetchNextJob_NoJobs()
        {
            var cancel = new CancellationTokenSource();
            bool Threw = false;

            UseConnection(connection =>
            {
                var t = new Thread(() => 
                {
                    try { connection.FetchNextJob(new string[] { "1" }, cancel.Token); }
                    catch (OperationCanceledException) { Threw = true; }
                });
                t.IsBackground = true;
                t.Start();
                Thread.Sleep(10); //Enough time for Redis to respond that there are no jobs in queue, and the thread to start waiting
                cancel.Cancel();
                t.Join();
                Assert.True(Threw);
            });
        }

        [Fact, CleanRedis]
        public void CreateExpiredJob()
        {
            UseConnections((redis, connection) =>
            {
                Type type = typeof(Console);
                var method = type.GetMethods().Where(x => x.Name == "WriteLine").FirstOrDefault();
                var job = new Job(type, method);
                var parameters = new Dictionary<string, string>() { {"Key1", "Value1" } };
                var jobid = connection.CreateExpiredJob(job, parameters, DateTime.UtcNow, TimeSpan.FromMinutes(5));

                var ReturnedJob = redis.HashGetAll("hangfire:job:" + jobid).ToStringDictionary(); ;
                Assert.Equal("WriteLine", ReturnedJob["Method"]);
                var CreatedAt = JobHelper.DeserializeDateTime(ReturnedJob["CreatedAt"]);
                Assert.Equal(0, (int)(CreatedAt - DateTime.UtcNow).TotalSeconds);
                Assert.Equal("Value1", ReturnedJob["Key1"]);
            });
        }

        [Fact, CleanRedis]
        public void GetJobData()
        {
            UseConnections((redis, connection) =>
            {
                Type type = typeof(Console);
                var method = type.GetMethods().Where(x => x.Name == "WriteLine").FirstOrDefault();
                var job = new Job(type, method);
                var parameters = new Dictionary<string, string>();
                var jobid = connection.CreateExpiredJob(job, parameters, DateTime.UtcNow, TimeSpan.FromMinutes(5));

                var ReturnedJob = connection.GetJobData(jobid);
                Assert.Equal("WriteLine", ReturnedJob.Job.Method.Name);
                Assert.Equal(0, (int)(ReturnedJob.CreatedAt - DateTime.UtcNow).TotalSeconds);
            });
        }

        [Fact, CleanRedis]
        public void GetJobData_InvalidJobData()
        {
            UseConnections((redis, connection) =>
            {
                Type type = typeof(Console);
                var method = type.GetMethods().Where(x => x.Name == "WriteLine").FirstOrDefault();
                var job = new Job(type, method);
                var parameters = new Dictionary<string, string>();
                var jobid = connection.CreateExpiredJob(job, parameters, DateTime.UtcNow, TimeSpan.FromMinutes(5));

                redis.HashSet("hangfire:job:" + jobid, "Method", "Invalid");

                var ReturnedJob = connection.GetJobData(jobid);
                Assert.IsType<JobLoadException>(ReturnedJob.LoadException);
                Assert.NotNull(ReturnedJob.LoadException);
            });
        }

        [Fact, CleanRedis]
        public void GetStateData_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(
                connection => Assert.Throws<ArgumentNullException>(
                    () => connection.GetStateData(null)));
        }

        [Fact, CleanRedis]
        public void GetStateData_ReturnsNull_WhenJobDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetStateData("random-id");
                Assert.Null(result);
            });
        }

        [Fact, CleanRedis]
        public void GetStateData_ReturnsCorrectResult()
        {
            UseConnections((redis, connection) =>
            {
                redis.HashSet(
                    "hangfire:job:my-job:state",
                    new HashEntry[]
                    {
                        new HashEntry("State", "Name"),
                        new HashEntry("Reason", "Reason"),
                        new HashEntry("Key", "Value")
                    });

                var result = connection.GetStateData("my-job");

                Assert.NotNull(result);
                Assert.Equal("Name", result.Name);
                Assert.Equal("Reason", result.Reason);
                Assert.Equal("Value", result.Data["Key"]);
            });
        }

        [Fact, CleanRedis]
        public void GetStateData_ReturnsNullReason_IfThereIsNoSuchKey()
        {
            UseConnections((redis, connection) =>
            {
                redis.HashSet(
                    "hangfire:job:my-job:state",
                    new HashEntry[]
                    {
                        new HashEntry( "State", "Name")
                    });

                var result = connection.GetStateData("my-job");

                Assert.NotNull(result);
                Assert.Null(result.Reason);
            });
        }

        [Fact, CleanRedis]
        public void GetAllItemsFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
                Assert.Throws<ArgumentNullException>(() => connection.GetAllItemsFromSet(null)));
        }

        [Fact, CleanRedis]
        public void GetAllItemsFromSet_ReturnsEmptyCollection_WhenSetDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetAllItemsFromSet("some-set");

                Assert.NotNull(result);
                Assert.Equal(0, result.Count);
            });
        }

        [Fact, CleanRedis]
        public void GetAllItemsFromSet_ReturnsAllItems()
        {
            UseConnections((redis, connection) =>
            {
                // Arrange
                redis.SortedSetAdd("hangfire:some-set", "1", 0);
                redis.SortedSetAdd("hangfire:some-set", "2", 0);

                // Act
                var result = connection.GetAllItemsFromSet("some-set");

                // Assert
                Assert.Equal(2, result.Count);
                Assert.Contains("1", result);
                Assert.Contains("2", result);
            });
        }

        [Fact, CleanRedis]
        public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetRangeInHash(null, new Dictionary<string, string>()));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanRedis]
        public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetRangeInHash("some-hash", null));

                Assert.Equal("keyValuePairs", exception.ParamName);
            });
        }

        [Fact, CleanRedis]
        public void SetRangeInHash_SetsAllGivenKeyPairs()
        {
            UseConnections((redis, connection) =>
            {
                connection.SetRangeInHash("some-hash", new Dictionary<string, string>
                {
                    { "Key1", "Value1" },
                    { "Key2", "Value2" }
                });

                var hash = redis.HashGetAll("hangfire:some-hash").ToDictionary(x => x.Name, x => x.Value);
                Assert.Equal("Value1", hash["Key1"]);
                Assert.Equal("Value2", hash["Key2"]);
            });
        }

        [Fact, CleanRedis]
        public void GetAllEntriesFromHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
                Assert.Throws<ArgumentNullException>(() => connection.GetAllEntriesFromHash(null)));
        }

        [Fact, CleanRedis]
        public void GetAllEntriesFromHash_ReturnsNullValue_WhenHashDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetAllEntriesFromHash("some-hash");
                Assert.Null(result);
            });
        }

        [Fact, CleanRedis]
        public void GetAllEntriesFromHash_ReturnsAllEntries()
        {
            UseConnections((redis, connection) =>
            {
                // Arrange
                redis.HashSet("hangfire:some-hash", new HashEntry[]
                {
                    new HashEntry("Key1", "Value1"),
                    new HashEntry("Key2", "Value2")
                });

                // Act
                var result = connection.GetAllEntriesFromHash("some-hash");

                // Assert
                Assert.NotNull(result);
                Assert.Equal("Value1", result["Key1"]);
                Assert.Equal("Value2", result["Key2"]);
            });
        }

        [Fact, CleanRedis]
        public void GetCounter()
        {
            UseConnections((redis, connection) =>
            {
                redis.StringIncrement("hangfire:counter");
                Assert.Equal(1, connection.GetCounter("counter"));

                redis.StringIncrement("hangfire:counter");
                Assert.Equal(2, connection.GetCounter("counter"));
            });
        }

        [Fact, CleanRedis]
        public void GetSetCount()
        {
            UseConnections((redis, connection) =>
            {
                redis.SortedSetAdd("hangfire:some-set", new SortedSetEntry[]
                {
                    new SortedSetEntry("Key1", 0.2),
                    new SortedSetEntry("Key2", 0.3)
                });

                Assert.Equal(2, connection.GetSetCount("some-set"));
            });
        }

        [Fact, CleanRedis]
        public void GetFirstByLowestScoreFromSet()
        {
            UseConnections((redis, connection) =>
            {
                redis.SortedSetAdd("hangfire:some-set", new SortedSetEntry[]
                {
                    new SortedSetEntry("Key1", 0.2),
                    new SortedSetEntry("Key2", 0.3),
                    new SortedSetEntry("Key3", 0.7),
                    new SortedSetEntry("Key4", 0.4),
                    new SortedSetEntry("Key5", 0.5)
                });

                Assert.Equal("Key2", connection.GetFirstByLowestScoreFromSet("some-set", 0.25, 1));
            });
        }

        [Fact, CleanRedis]
        public void GetRangeFromSet()
        {
            UseConnections((redis, connection) =>
            {
                redis.SortedSetAdd("hangfire:some-set", new SortedSetEntry[]
                {
                    new SortedSetEntry("Key1", 0.2),
                    new SortedSetEntry("Key2", 0.3),
                    new SortedSetEntry("Key3", 0.7),
                    new SortedSetEntry("Key4", 0.4),
                    new SortedSetEntry("Key5", 0.5)
                });
                
                var set = connection.GetRangeFromSet("some-set", 1, 3);
                Assert.Equal(3, set.Count);
                Assert.Equal("Key2", set[0]);
                Assert.Equal("Key5", set[2]);
            });
        }

        [Fact, CleanRedis]
        public void GetSetTtl()
        {
            UseConnections((redis, connection) =>
            {
                redis.SortedSetAdd("hangfire:some-set", new SortedSetEntry[]
                {
                    new SortedSetEntry("Key1", 0.2),
                    new SortedSetEntry("Key2", 0.3)
                });

                Assert.Equal(TimeSpan.FromSeconds(-1), connection.GetSetTtl("some-set"));

                redis.KeyExpire("hangfire:some-set", DateTime.UtcNow.AddMinutes(1));
                Assert.Equal(0, (int)(TimeSpan.FromMinutes(1) - connection.GetSetTtl("some-set")).TotalSeconds);
            });
        }

        [Fact, CleanRedis]
        public void GetHashCount()
        {
            UseConnections((redis, connection) =>
            {
                redis.HashSet("hangfire:some-hash", new HashEntry[]
                {
                    new HashEntry("Key1", "Value1"),
                    new HashEntry("Key2", "Value2")
                });

                Assert.Equal(2, connection.GetHashCount("some-hash"));
            });
        }

        [Fact, CleanRedis]
        public void GetValueFromHash()
        {
            UseConnections((redis, connection) =>
            {
                redis.HashSet("hangfire:some-hash", new HashEntry[]
                {
                    new HashEntry("Key1", "Value1"),
                    new HashEntry("Key2", "Value2")
                });

                Assert.Equal("Value2", connection.GetValueFromHash("some-hash", "Key2"));
            });
        }

        [Fact, CleanRedis]
        public void GetHashTtl()
        {
            UseConnections((redis, connection) =>
            {
                redis.HashSet("hangfire:some-hash", new HashEntry[]
                {
                    new HashEntry("Key1", "Value1"),
                    new HashEntry("Key2", "Value2")
                });

                Assert.Equal(TimeSpan.FromSeconds(-1), connection.GetHashTtl("some-hash"));

                redis.KeyExpire("hangfire:some-hash", DateTime.UtcNow.AddMinutes(1));
                Assert.Equal(0, (int)(TimeSpan.FromMinutes(1) - connection.GetHashTtl("some-hash")).TotalSeconds);
            });
        }

        [Fact, CleanRedis]
        public void GetListCount()
        {
            UseConnections((redis, connection) =>
            {
                redis.ListRightPush("hangfire:some-list", "Value");
                redis.ListRightPush("hangfire:some-list", "Value2");

                Assert.Equal(2, connection.GetListCount("some-list"));
            });
        }

        [Fact, CleanRedis]
        public void GetRangeFromList()
        {
            UseConnections((redis, connection) =>
            {
                redis.ListRightPush("hangfire:some-list", "Value");
                redis.ListRightPush("hangfire:some-list", "Value2");
                redis.ListRightPush("hangfire:some-list", "Value3");
                redis.ListRightPush("hangfire:some-list", "Value4");

                var list = connection.GetRangeFromList("some-list", 1, 2);


                Assert.Equal(2, list.Count);
                Assert.Equal("Value3", list[1]);
            });
        }

        [Fact, CleanRedis]
        public void GetAllItemsFromList()
        {
            UseConnections((redis, connection) =>
            {
                redis.ListRightPush("hangfire:some-list", "Value");
                redis.ListRightPush("hangfire:some-list", "Value2");
                redis.ListRightPush("hangfire:some-list", "Value3");
                redis.ListRightPush("hangfire:some-list", "Value4");

                var list = connection.GetAllItemsFromList("some-list");

                Assert.Equal(4, list.Count);
            });
        }

        [Fact, CleanRedis]
        public void GetListTtl()
        {
            UseConnections((redis, connection) =>
            {
                redis.ListLeftPush("hangfire:some-list", "Value");
                Assert.Equal(TimeSpan.FromSeconds(-1), connection.GetListTtl("some-list"));

                redis.KeyExpire("hangfire:some-list", DateTime.UtcNow.AddMinutes(1));
                Assert.Equal(0, (int)(TimeSpan.FromMinutes(1) - connection.GetListTtl("some-list")).TotalSeconds);
            });
        }

        [Fact, CleanRedis]
        public void SetGetJobParameter()
        {
            UseConnections((redis, connection) =>
            {
                connection.SetJobParameter("1", "data", "testvalue");
                Assert.Equal("testvalue", connection.GetJobParameter("1", "data"));
            });
        }

        [Fact, CleanRedis]
        public void Heartbeat()
        {
            UseConnections((redis, connection) =>
            {
                connection.Heartbeat("1");
                var pong = JobHelper.DeserializeDateTime(redis.HashGet("hangfire:server:1", "Heartbeat"));
                Assert.Equal(0, (int)(pong - DateTime.UtcNow).TotalSeconds);
            });
        }

        [Fact, CleanRedis]
        public void AnnounceServer()
        {
            UseConnections((redis, connection) =>
            {
                var server = new Server.ServerContext();
                server.Queues = new string[1] {"queue1"};
                server.WorkerCount = 5;
                connection.AnnounceServer("1", server);

                Assert.Equal(1, redis.SetLength("hangfire:servers"));
                Assert.Equal(1, redis.ListLength("hangfire:server:1:queues"));
                Assert.Equal("5", redis.HashGet("hangfire:server:1", "WorkerCount"));
                var pong = JobHelper.DeserializeDateTime(redis.HashGet("hangfire:server:1", "StartedAt"));
                Assert.Equal(0, (int)(pong - DateTime.UtcNow).TotalSeconds);
            });
        }

        [Fact, CleanRedis]
        public void RemoveServer()
        {
            UseConnections((redis, connection) =>
            {
                var server = new Server.ServerContext();
                server.Queues = new string[1] { "queue1" };
                server.WorkerCount = 5;
                connection.AnnounceServer("1", server);
                connection.RemoveServer("1");
                Assert.Equal(0, redis.SetLength("hangfire:servers"));
                Assert.Equal(0, redis.ListLength("hangfire:server:1:queues"));
                Assert.Equal(RedisValue.Null, redis.HashGet("hangfire:server:1", "WorkerCount"));
            });
        }

        [Fact, CleanRedis]
        public void RemoveTimedOut()
        {
            UseConnections((redis, connection) =>
            {
                redis.SetAdd("hangfire:servers", "1");
                redis.HashSet("hangfire:server:1", "Heartbeat", JobHelper.SerializeDateTime(DateTime.UtcNow.AddDays(-1)));
                redis.HashSet("hangfire:server:1", "StartedAt", JobHelper.SerializeDateTime(DateTime.UtcNow.AddDays(-2)));
                redis.SetAdd("hangfire:servers", "2");
                redis.HashSet("hangfire:server:2", "Heartbeat", JobHelper.SerializeDateTime(DateTime.UtcNow.AddMinutes(-1)));
                redis.HashSet("hangfire:server:2", "StartedAt", JobHelper.SerializeDateTime(DateTime.UtcNow.AddDays(-2)));
                connection.RemoveTimedOutServers(TimeSpan.FromHours(1));
                Assert.Equal(1, redis.SetLength("hangfire:servers"));
            });
        }

        private void UseConnections(Action<IDatabase, JobStorageConnection> action)
        {
            action(Redis.Storage.GetDatabase(), Redis.Storage.GetConnection() as JobStorageConnection);
        }

        private void UseConnection(Action<JobStorageConnection> action)
        {
            action(Redis.Storage.GetConnection() as JobStorageConnection);
        }

    }
}
