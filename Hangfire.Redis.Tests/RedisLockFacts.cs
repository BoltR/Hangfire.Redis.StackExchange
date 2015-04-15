using StackExchange.Redis;
using System;
using System.Threading;
using Xunit;

namespace Hangfire.Redis.StackExchange.Tests
{
    [Collection("Redis")]
    public class RedisLockFacts
    {
        private readonly RedisFixture Redis;
        public RedisLockFacts(RedisFixture Redis) 
        {
            this.Redis = Redis;
        }

        [Fact, CleanRedis]
        public void LockReentry()
        {
            UseDatabase(Database =>
            {
                using (var lock1 = new RedisLock(Redis.Storage.GetDatabase(), "lock", "1", TimeSpan.FromMilliseconds(30)))
                {
                    using (var lock2 = new RedisLock(Database, "lock", "1", TimeSpan.FromMilliseconds(5)))
                    {

                    }
                }
            });
        }

        [Fact, CleanRedis]
        public void LockReentry_DontReleaseEarly()
        {
            UseDatabase(Database =>
            {
                using (var lock1 = new RedisLock(Database, "lock", "1", TimeSpan.FromMilliseconds(60)))
                {
                    using (var lock3 = new RedisLock(Database, "lock", "1", TimeSpan.FromMilliseconds(5)))
                    {

                    } //Make sure the lock isn't removed when the nested lock is disposed
                  
                    Assert.Throws<TimeoutException>(() =>
                    {
                        var lock2 = new RedisLock(Database, "lock", "2", TimeSpan.FromMilliseconds(1));
                    });
                }
            });
        }

        [Fact, CleanRedis]
        public void LockReentry_ExtendLock()
        {
            UseDatabase(Database =>
            {
                using (var lock1 = new RedisLock(Database, "lock", "1", TimeSpan.FromMilliseconds(10)))
                {
                    using (var lock3 = new RedisLock(Database, "lock", "1", TimeSpan.FromMinutes(1)))
                    {

                    }
                    Thread.Sleep(20);
                    Assert.Throws<TimeoutException>(() =>
                    {
                        var lock2 = new RedisLock(Database, "lock", "2", TimeSpan.FromMilliseconds(1));
                    });
                }
            });
        }

        [Fact, CleanRedis]
        public void LockCollision_WithTimeout()
        {
            UseDatabase(Database =>
            {
                using (var lock1 = new RedisLock(Database, "lock", "1", TimeSpan.FromMilliseconds(30)))
                {
                    Assert.Throws<TimeoutException>(() =>
                    {
                        var lock2 = new RedisLock(Database, "lock", "2", TimeSpan.FromMilliseconds(1));
                    });
                }
            });
        }

        [Fact, CleanRedis]
        public void LockCollision()
        {
            UseDatabase(Database =>
            {
                using (var lock1 = new RedisLock(Database, "lock", "1", TimeSpan.FromMilliseconds(30)))
                {
                    Thread.Sleep(20); //Sleep to give the wait loop time to run. If not set high enough lock2 will Timeout
                    using (var lock2 = new RedisLock(Database, "lock", "2", TimeSpan.FromMilliseconds(30)))
                    {

                    }
                }
            });
        }

        [Fact, CleanRedis]
        public void LockRelease()
        {
            UseDatabase(Database =>
            {
                using (var lock1 = new RedisLock(Database, "lock", "1", TimeSpan.FromMinutes(1)))
                {
                }
                using (var lock2 = new RedisLock(Database, "lock", "2", TimeSpan.FromMinutes(1)))
                {
                }
            });
        }

        private void UseDatabase(Action<IDatabase> action)
        {
            action(Redis.Storage.GetDatabase());
        }
    }
}
