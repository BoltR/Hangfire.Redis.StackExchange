using System.Linq;
using Xunit;

namespace Hangfire.Redis.StackExchange.Tests
{
    public class RedisStorageFacts
    {
        [Fact]
        public void DefaultCtor_InitializesCorrectDefaultValues()
        {
            var storage = new RedisStorage("localhost:6379", 0); //TODO: Revert to empty constructor

            Assert.Equal(0, storage.Db);
        }

        [Fact]
        public void GetStateHandlers_ReturnsAllHandlers()
        {
            var storage = new RedisStorage("localhost:6379", 0);

            var handlers = storage.GetStateHandlers();

            var handlerTypes = handlers.Select(x => x.GetType()).ToArray();
            Assert.Contains(typeof(FailedStateHandler), handlerTypes);
            Assert.Contains(typeof(ProcessingStateHandler), handlerTypes);
            Assert.Contains(typeof(SucceededStateHandler), handlerTypes);
            Assert.Contains(typeof(DeletedStateHandler), handlerTypes);
        }

    }
}
