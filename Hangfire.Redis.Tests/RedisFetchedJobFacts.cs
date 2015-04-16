﻿using System;
using Microsoft.Win32;
using NSubstitute;
using StackExchange.Redis;
using Xunit;

namespace Hangfire.Redis.StackExchange.Tests
{
    [Collection("Redis")]
    public class RedisFetchedJobFacts
    {
        private const string JobId = "id";
        private const string Queue = "queue";
        private readonly string Prefix;

        private readonly IDatabase _redis;
        private readonly RedisFixture Redis;

        public RedisFetchedJobFacts(RedisFixture Redis)
        {
            _redis = Substitute.For<IDatabase>();
            this.Redis = Redis;
            Prefix = Redis.Storage.Prefix;
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenRedisIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new RedisFetchedJob(null, JobId, Queue, Prefix));

            Assert.Equal("client", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenJobIdIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new RedisFetchedJob(_redis, null, Queue, Prefix));

            Assert.Equal("jobId", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenQueueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new RedisFetchedJob(_redis, JobId, null, Prefix));

            Assert.Equal("queue", exception.ParamName);
        }

        [Fact, CleanRedis]
        public void RemoveFromQueue_RemovesJobFromTheFetchedList()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.ListLeftPush(Prefix + "queue:my-queue:dequeued", "job-id");

                var fetchedJob = new RedisFetchedJob(redis, "job-id", "my-queue", Prefix);

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                Assert.Equal(0, redis.ListLength(Prefix + "queue:my-queue:dequeued"));
            });
        }

        [Fact, CleanRedis]
        public void RemoveFromQueue_RemovesOnlyJobWithTheSpecifiedId()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.ListRightPush(Prefix + "queue:my-queue:dequeued", "job-id");
                redis.ListRightPush(Prefix + "queue:my-queue:dequeued", "another-job-id");

                var fetchedJob = new RedisFetchedJob(redis, "job-id", "my-queue", Prefix);

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                Assert.Equal(1, redis.ListLength(Prefix + "queue:my-queue:dequeued"));
                Assert.Equal("another-job-id", redis.ListLeftPop(Prefix + "queue:my-queue:dequeued"));
            });
        }

        [Fact, CleanRedis]
        public void RemoveFromQueue_RemovesOnlyOneJob()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.ListRightPush(Prefix + "queue:my-queue:dequeued", "job-id");
                redis.ListRightPush(Prefix + "queue:my-queue:dequeued", "job-id");

                var fetchedJob = new RedisFetchedJob(redis, "job-id", "my-queue", Prefix);

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                Assert.Equal(1, redis.ListLength(Prefix + "queue:my-queue:dequeued"));
            });
        }

        [Fact, CleanRedis]
        public void RemoveFromQueue_RemovesTheFetchedFlag()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.HashSet(Prefix + "job:my-job", "Fetched", "value");
                var fetchedJob = new RedisFetchedJob(redis, "my-job", "my-queue", Prefix);

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                Assert.False(redis.HashExists(Prefix + "job:my-job", "Fetched"));
            });
        }

        [Fact, CleanRedis]
        public void RemoveFromQueue_RemovesTheCheckedFlag()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.HashSet(Prefix + "job:my-job", "Checked", "value");
                var fetchedJob = new RedisFetchedJob(redis, "my-job", "my-queue", Prefix);

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                Assert.False(redis.HashExists(Prefix + "job:my-job", "Checked"));
            });
        }

        [Fact, CleanRedis]
        public void Requeue_PushesAJobBackToQueue()
        {
            UseRedis(redis => 
            {
                // Arrange
                redis.ListRightPush(Prefix + "queue:my-queue:dequeued", "my-job");
                var fetchedJob = new RedisFetchedJob(redis, "my-job", "my-queue", Prefix);

                // Act
                fetchedJob.Requeue();

                // Assert
                Assert.Equal("my-job", redis.ListRightPop(Prefix + "queue:my-queue"));
            });
        }

        [Fact, CleanRedis]
        public void Requeue_PushesAJobToTheRightSide()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.ListRightPush(Prefix + "queue:my-queue", "another-job");
                redis.ListRightPush(Prefix + "queue:my-queue:dequeued", "my-job");

                var fetchedJob = new RedisFetchedJob(redis, "my-job", "my-queue", Prefix);

                // Act
                fetchedJob.Requeue();

                // Assert - RPOP
                Assert.Equal("my-job", redis.ListRightPop(Prefix + "queue:my-queue")); 
            });
        }

        [Fact, CleanRedis]
        public void Requeue_RemovesAJobFromFetchedList()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.ListRightPush(Prefix + "queue:my-queue:dequeued", "my-job");
                var fetchedJob = new RedisFetchedJob(redis, "my-job", "my-queue", Prefix);

                // Act
                fetchedJob.Requeue();

                // Assert
                Assert.Equal(0, redis.ListLength(Prefix + "queue:my-queue:dequeued"));
            });
        }

        [Fact, CleanRedis]
        public void Requeue_RemovesTheFetchedFlag()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.HashSet(Prefix + "job:my-job", "Fetched", "value");
                var fetchedJob = new RedisFetchedJob(redis, "my-job", "my-queue", Prefix);

                // Act
                fetchedJob.Requeue();

                // Assert
                Assert.False(redis.HashExists(Prefix + "job:my-job", "Fetched"));
            });
        }

        [Fact, CleanRedis]
        public void Requeue_RemovesTheCheckedFlag()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.HashSet(Prefix + "job:my-job", "Checked", "value");
                var fetchedJob = new RedisFetchedJob(redis, "my-job", "my-queue", Prefix);

                // Act
                fetchedJob.Requeue();

                // Assert
                Assert.False(redis.HashExists(Prefix + "job:my-job", "Checked"));
            });
        }

        [Fact, CleanRedis]
        public void Dispose_WithNoComplete_RequeuesAJob()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.ListRightPush(Prefix + "queue:my-queue:dequeued", "my-job");
                var fetchedJob = new RedisFetchedJob(redis, "my-job", "my-queue", Prefix);

                // Act
                fetchedJob.Dispose();

                // Assert
                Assert.Equal(1, redis.ListLength(Prefix + "queue:my-queue"));
            });
        }

        [Fact, CleanRedis]
        public void Dispose_AfterRemoveFromQueue_DoesNotRequeueAJob()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.ListRightPush(Prefix + "queue:my-queue:dequeued", "my-job");
                redis.ListRightPush(Prefix + "queue:my-queue:dequeued", "my-job");
                var fetchedJob = new RedisFetchedJob(redis, "my-job", "my-queue", Prefix);

                // Act
                fetchedJob.RemoveFromQueue();
                fetchedJob.Dispose();

                // Assert
                Assert.Equal(0, redis.ListLength(Prefix + "queue:my-queue"));
            });
        }

        private void UseRedis(Action<IDatabase> action)
        {
            action(Redis.Storage.GetDatabase());
        }
    }
}
