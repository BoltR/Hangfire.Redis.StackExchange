// Copyright © 2015 Daniel Chernis.
//
// Hangfire.Redis.StackExchange is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation, either version 3 
// of the License, or any later version.
// 
// Hangfire.Redis.StackExchange is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public 
// License along with Hangfire.Redis.StackExchange. If not, see <http://www.gnu.org/licenses/>.

using StackExchange.Redis;
using System;
using System.Threading;

namespace Hangfire.Redis.StackExchange
{
    internal class RedisLock : IDisposable
    {
        private readonly string Key;
        private readonly IDatabase Redis;
        private readonly string LockID;
        private readonly string LockCounter;

        public RedisLock(IDatabase Redis, string Key, string LockID, TimeSpan Timeout)
        {
            this.Redis = Redis;
            this.Key = Key;
            this.LockID = LockID;
            LockCounter = Key + ":c";

            int i = 1;
            var LockTimeout = DateTime.UtcNow + Timeout;

            while (DateTime.UtcNow < LockTimeout)
            {
                if (TakeLock(Timeout))
                {
                    return;
                }
                else
                {
                    var LockToken = Redis.LockQuery(Key);
                    if (LockToken.Equals(LockID))
                    {
                        if (ExtendLock(GetLockExpireTime(LockTimeout)))
                        {
                            return;
                        }
                    }
                }
                Thread.Sleep(GetSleepTimeout(i++));
            }
            throw new TimeoutException("Failed to get lock within timeout period");
        }

        private bool TakeLock(TimeSpan Expiry)
        {
            var trans = Redis.CreateTransaction();
            trans.AddCondition(Condition.KeyNotExists(Key));
            trans.StringSetAsync(Key, LockID, Expiry);
            trans.StringIncrementAsync(LockCounter);
            return trans.Execute();
        }

        private bool ExtendLock(TimeSpan ExtendTo)
        {
            var trans = Redis.CreateTransaction();
            trans.AddCondition(Condition.StringEqual(Key, LockID));
            if (ExtendTo != TimeSpan.Zero)
            {
                trans.KeyExpireAsync(Key, ExtendTo);
            }
            trans.StringIncrementAsync(LockCounter);
            return trans.Execute();
        }

        private TimeSpan GetLockExpireTime(DateTime NewExpireTime)
        {
            var CurrentExpireSpan = Redis.KeyTimeToLive(Key) ?? TimeSpan.Zero;
            var NewExpireSpan = NewExpireTime - DateTime.UtcNow;
            if (NewExpireSpan > CurrentExpireSpan)
            {
                return NewExpireSpan - CurrentExpireSpan;
            }
            else
            {
                return TimeSpan.Zero;
            }

        }

        private static int Seed = Environment.TickCount;
        private static ThreadLocal<Random> rnd = new ThreadLocal<Random>(() => new Random(Interlocked.Increment(ref Seed)));
        private static int GetSleepTimeout(int i)
        {
            var log = Math.Log(i) * 2;
            return rnd.Value.Next((int)Math.Pow(log, 3), (int)Math.Pow(log + 1, 3) + 10) + 1;
        }

        public void Dispose()
        {
            var trans = Redis.CreateTransaction();
            trans.AddCondition(Condition.StringEqual(Key, LockID));
            trans.StringDecrementAsync(LockCounter);
            trans.Execute();

            trans = Redis.CreateTransaction();
            trans.AddCondition(Condition.StringEqual(Key, LockID));
            trans.AddCondition(Condition.StringEqual(LockCounter, 0));
            trans.KeyDeleteAsync(Key, CommandFlags.FireAndForget);
            trans.KeyDeleteAsync(LockCounter, CommandFlags.FireAndForget);
            trans.Execute();
        }
    }
}
