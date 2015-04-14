using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Hangfire;
using Xunit;
using StackExchange.Redis;

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
            Assert.NotNull(JobStorage.Current as RedisStorage);
        }

        [Fact]
        public void String_Database()
        {
            JobStorage.Current = null;
            Assert.Throws<InvalidOperationException>(() => JobStorage.Current);
            GlobalConfiguration.Configuration.UseRedisStorage("localhost:6379", 6);
            Assert.NotNull(JobStorage.Current as RedisStorage);
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
            Assert.NotNull(JobStorage.Current as RedisStorage);
        }

        [Fact]
        public void Options_Database()
        {
            JobStorage.Current = null;
            Assert.Throws<InvalidOperationException>(() => JobStorage.Current);
            GlobalConfiguration.Configuration.UseRedisStorage(ConfigurationOptions.Parse("localhost:6379"), 6);
            Assert.NotNull(JobStorage.Current as RedisStorage);
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
