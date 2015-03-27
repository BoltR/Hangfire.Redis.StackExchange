using System.Linq;
using Xunit;

namespace Hangfire.Redis.StackExchange.Tests
{
    [Collection("Redis")]
    public class RedisStorageFacts
    {

        private readonly RedisFixture Redis;
        public RedisStorageFacts(RedisFixture Redis) 
        {
            this.Redis = Redis;
        } 

        [Fact]
        public void DefaultCtor_InitializesCorrectDefaultValues()
        {
            Assert.Equal(1, Redis.Storage.Db);
        }

        [Fact]
        public void GetStateHandlers_ReturnsAllHandlers()
        {
            var handlers = Redis.Storage.GetStateHandlers();

            var handlerTypes = handlers.Select(x => x.GetType()).ToArray();
            Assert.Contains(typeof(FailedStateHandler), handlerTypes);
            Assert.Contains(typeof(ProcessingStateHandler), handlerTypes);
            Assert.Contains(typeof(SucceededStateHandler), handlerTypes);
            Assert.Contains(typeof(DeletedStateHandler), handlerTypes);
        }

    }
}
