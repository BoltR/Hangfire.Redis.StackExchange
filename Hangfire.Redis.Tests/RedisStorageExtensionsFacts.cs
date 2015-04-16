using StackExchange.Redis;
using System;
using Xunit;

namespace Hangfire.Redis.StackExchange.Tests
{
    public class RedisStorageExtensionsFacts
    {

        [Fact]
        public void String()
        {
            JobStorage.Current = null;
            Assert.Throws<InvalidOperationException>(() => JobStorage.Current);
            GlobalConfiguration.Configuration.UseRedisStorage("localhost:6379");
            var Storage = JobStorage.Current as RedisStorage;
            Assert.NotNull(Storage);
            Assert.Equal("hangfire:", Storage.Prefix);
        }

        [Fact]
        public void String_Database()
        {
            JobStorage.Current = null;
            Assert.Throws<InvalidOperationException>(() => JobStorage.Current);
            GlobalConfiguration.Configuration.UseRedisStorage("localhost:6379", 6);
            var Storage = JobStorage.Current as RedisStorage;
            Assert.NotNull(Storage);
            Assert.Equal("hangfire:", Storage.Prefix);
        }

        [Fact]
        public void String_DatabaseWithPrefix()
        {
            JobStorage.Current = null;
            Assert.Throws<InvalidOperationException>(() => JobStorage.Current);
            GlobalConfiguration.Configuration.UseRedisStorage("localhost:6379", 6, "test:");
            var Storage = JobStorage.Current as RedisStorage;
            Assert.NotNull(Storage);
            Assert.Equal("test:", Storage.Prefix);
        }

        [Fact]
        public void String_Empty()
        {
            JobStorage.Current = null;
            Assert.Throws<InvalidOperationException>(() => JobStorage.Current);
            Assert.Throws<ArgumentException>(() => GlobalConfiguration.Configuration.UseRedisStorage(""));
        }

        [Fact]
        public void Options()
        {
            JobStorage.Current = null;
            Assert.Throws<InvalidOperationException>(() => JobStorage.Current);
            GlobalConfiguration.Configuration.UseRedisStorage(ConfigurationOptions.Parse("localhost:6379"));
            var Storage = JobStorage.Current as RedisStorage;
            Assert.NotNull(Storage);
            Assert.Equal("hangfire:", Storage.Prefix);
        }

        [Fact]
        public void Options_Database()
        {
            JobStorage.Current = null;
            Assert.Throws<InvalidOperationException>(() => JobStorage.Current);
            GlobalConfiguration.Configuration.UseRedisStorage(ConfigurationOptions.Parse("localhost:6379"), 6);
            var Storage = JobStorage.Current as RedisStorage;
            Assert.NotNull(Storage);
            Assert.Equal("hangfire:", Storage.Prefix);
        }

        [Fact]
        public void Options_DatabasePrefix()
        {
            JobStorage.Current = null;
            Assert.Throws<InvalidOperationException>(() => JobStorage.Current);
            GlobalConfiguration.Configuration.UseRedisStorage(ConfigurationOptions.Parse("localhost:6379"), 6, "test:");
            var Storage = JobStorage.Current as RedisStorage;
            Assert.NotNull(Storage);
            Assert.Equal("test:", Storage.Prefix);
        }

        [Fact]
        public void Options_NoEndPoints()
        {
            JobStorage.Current = null;
            Assert.Throws<InvalidOperationException>(() => JobStorage.Current);
            var options = new ConfigurationOptions();
            Assert.Throws<ArgumentException>(() => GlobalConfiguration.Configuration.UseRedisStorage(options));
        }
    }
}
