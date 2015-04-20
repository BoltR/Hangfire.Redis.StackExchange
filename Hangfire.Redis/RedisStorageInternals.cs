using System;

namespace Hangfire.Redis.StackExchange
{
    internal class RedisStorageInternals : IDisposable
    {
       
        internal string StorageLockID { get; private set; }
        internal string Prefix { get; private set; }
        internal RedisSubscribe Sub { get; private set; }

        public RedisStorageInternals(string Prefix, string StorageLockID, RedisSubscribe Sub)
        {
            this.StorageLockID = StorageLockID;
            this.Prefix = Prefix;
            this.Sub = Sub;
        }

        public void Dispose()
        {
            Sub.Dispose();
        }
    }
}
