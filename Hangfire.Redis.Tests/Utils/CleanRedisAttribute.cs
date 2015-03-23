using StackExchange.Redis;
using System.Reflection;
using System.Threading;
using Xunit.Sdk;

namespace Hangfire.Redis.StackExchange.Tests
{
    public class CleanRedisAttribute : BeforeAfterTestAttribute
    {
        private static readonly object GlobalLock = new object();
        public static IServer Server { get; set; }
        public static int Db { get; set; }

        public override void Before(MethodInfo methodUnderTest)
        {
            Monitor.Enter(GlobalLock);
            Server.FlushDatabase(Db);
        }

        public override void After(MethodInfo methodUnderTest)
        {
            Monitor.Exit(GlobalLock);
        }
    }
}
