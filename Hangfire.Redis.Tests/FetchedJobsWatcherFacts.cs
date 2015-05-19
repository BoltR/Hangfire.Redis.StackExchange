using Hangfire.Common;
using NSubstitute;
using Owin;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Xunit;

namespace Hangfire.Redis.StackExchange.Tests
{
	[Collection("Redis")]
	public class FetchedJobsWatcherFacts
	{

		private RedisFixture Redis;
		private readonly string Prefix;
		private static readonly FetchedJobsWatcherOptions FetchedOptions = new FetchedJobsWatcherOptions()
		{
			InvisibilityTimeout = TimeSpan.FromSeconds(10)
		};

		private readonly CancellationTokenSource _cts;

		public FetchedJobsWatcherFacts(RedisFixture Redis)
		{
			this.Redis = Redis;
			Prefix = Redis.Storage.Prefix;
			_cts = new CancellationTokenSource();
			_cts.Cancel();
		}

		[Fact]
		public void CheckDefaultConstructor()
		{
			var watcher = new FetchedJobsWatcher(Redis.Storage);

			var redis = Redis.Storage.GetDatabase();
			// Arrange
			redis.SortedSetAdd(Prefix + "queues", "my-queue", 0);
			redis.ListLeftPush(Prefix + "queue:my-queue:dequeued", "my-job");

			watcher.Execute(_cts.Token);

			Assert.NotNull(JobHelper.DeserializeNullableDateTime(
				redis.HashGet(Prefix + "job:my-job", "Checked")));
		}

		[Fact]
		public void Ctor_ThrowsAnException_WhenStorageIsNull()
		{
			var exception = Assert.Throws<ArgumentNullException>(
				() => new FetchedJobsWatcher(null, new FetchedJobsWatcherOptions()));

			Assert.Equal("storage", exception.ParamName);
		}

		[Fact]
		public void Ctor_ThrowsAnException_WhenInvisibilityTimeoutIsZero()
		{
			var exception = Assert.Throws<ArgumentOutOfRangeException>(
				() => new FetchedJobsWatcher(Redis.Storage, new FetchedJobsWatcherOptions() { InvisibilityTimeout = TimeSpan.Zero }));

			Assert.Equal("invisibilityTimeout", exception.ParamName);
		}

		[Fact]
		public void Ctor_ThrowsAnException_WhenInvisibilityTimeoutIsNegative()
		{
			var exception = Assert.Throws<ArgumentOutOfRangeException>(
				() => new FetchedJobsWatcher(Redis.Storage, new FetchedJobsWatcherOptions() { InvisibilityTimeout =  TimeSpan.FromSeconds(-1) }));

			Assert.Equal("invisibilityTimeout", exception.ParamName);
		}

		[Fact, CleanRedis]
		public void Execute_EnqueuesTimedOutJobs_AndDeletesThemFromFetchedList()
		{
			var redis = Redis.Storage.GetDatabase();
			// Arrange
			redis.SortedSetAdd(Prefix + "queues", "my-queue", 0);
			redis.ListLeftPush(Prefix + "queue:my-queue:dequeued", "my-job");
			redis.HashSet(Prefix + "job:my-job", "Fetched",
				JobHelper.SerializeDateTime(DateTime.UtcNow.AddDays(-1)));

			var watcher = CreateWatcher();

			// Act
			watcher.Execute(_cts.Token);

			// Assert
			Assert.Equal(0, redis.ListLength(Prefix + "queue:my-queue:dequeued"));

			var listEntry = redis.ListLeftPop(Prefix + "queue:my-queue");
			Assert.Equal("my-job", listEntry);

			var job = redis.HashGetAll(Prefix + "job:my-job").ToDictionary(x => x.Name, x => x.Value);
			Assert.False(job.ContainsKey("Fetched"));
		}

		[Fact, CleanRedis]
		public void Execute_MarksDequeuedJobAsChecked_IfItHasNoFetchedFlagSet()
		{
			var redis = Redis.Storage.GetDatabase();
			// Arrange
			redis.SortedSetAdd(Prefix + "queues", "my-queue", 0);
			redis.ListLeftPush(Prefix + "queue:my-queue:dequeued", "my-job");

			var watcher = CreateWatcher();

			// Act
			watcher.Execute(_cts.Token);

			Assert.NotNull(JobHelper.DeserializeNullableDateTime(
				redis.HashGet(Prefix + "job:my-job", "Checked")));
		}

		[Fact, CleanRedis]
		public void Execute_EnqueuesCheckedAndTimedOutJob_IfNoFetchedFlagSet()
		{
			var redis = Redis.Storage.GetDatabase();
			// Arrange
			redis.SortedSetAdd(Prefix + "queues", "my-queue", 0);
			redis.ListLeftPush(Prefix + "queue:my-queue:dequeued", "my-job");
			redis.HashSet(Prefix + "job:my-job", "Checked",
				JobHelper.SerializeDateTime(DateTime.UtcNow.AddDays(-1)));

			var watcher = CreateWatcher();

			// Act
			watcher.Execute(_cts.Token);

			// Arrange
			Assert.Equal(0, redis.ListLength(Prefix + "queue:my-queue:dequeued"));
			Assert.Equal(1, redis.ListLength(Prefix + "queue:my-queue"));

			var job = redis.HashGetAll(Prefix + "job:my-job").ToDictionary(x => x.Name, x => x.Value);
			Assert.False(job.ContainsKey("Checked"));
		}

		[Fact, CleanRedis]
		public void Execute_DoesNotEnqueueTimedOutByCheckedFlagJob_IfFetchedFlagSet()
		{
			var redis = Redis.Storage.GetDatabase();
			// Arrange
			redis.SortedSetAdd(Prefix + "queues", "my-queue", 0);
			redis.ListLeftPush(Prefix + "queue:my-queue:dequeued", "my-job");
			redis.HashSet(Prefix + "job:my-job", "Checked",
				JobHelper.SerializeDateTime(DateTime.UtcNow.AddDays(-1)));
			redis.HashSet(Prefix + "job:my-job", "Fetched",
				JobHelper.SerializeDateTime(DateTime.UtcNow));

			var watcher = CreateWatcher();

			// Act
			watcher.Execute(_cts.Token);

			// Assert
			Assert.Equal(1, redis.ListLength(Prefix + "queue:my-queue:dequeued"));

		}

		[Fact, CleanRedis]
		public void MultiQueueOrdering()
		{
			using (var canceltoken = new CancellationTokenSource())
			{
				var properties = new Dictionary<string, object> { { "host.OnAppDisposing", canceltoken.Token } };
				var app = Substitute.For<IAppBuilder>();
				app.Properties.Returns(properties);

				var redisoptions = new RedisStorageOptions()
				{
					Db = Redis.Storage.Db,
					Prefix = Redis.Storage.Prefix
				};
				GlobalConfiguration.Configuration.UseRedisStorage(Redis.ServerInfo, redisoptions);

				var options = new BackgroundJobServerOptions
				{
					Queues = new string[] { "critical", "important", "default" }
				};

				using (new BackgroundJobServer(options, Redis.Storage))
				{
					BackgroundJob.Enqueue<Jobs>(x => x.Critical());
					BackgroundJob.Enqueue<Jobs>(x => x.Default());

					var conn = Redis.Storage.GetDatabase();
					var queues = conn.SortedSetRangeByScore("hangfire:queues");
					Assert.Equal("critical", queues[0]);
					Assert.Equal("default", queues[1]);
					Assert.Throws<IndexOutOfRangeException>(() => queues[2]);
				}
			}
		}

		private FetchedJobsWatcher CreateWatcher()
		{
			return new FetchedJobsWatcher(Redis.Storage, FetchedOptions);
		}
	}
}
