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

namespace Hangfire.Redis.StackExchange
{
    internal class RedisLock : IDisposable
    {
        private readonly string Key;
        private readonly string Token;
        private readonly IDatabase Redis;

        public RedisLock(IDatabase Redis, string Key, TimeSpan Timeout)
        {
            this.Redis = Redis;
            this.Key = Key;
            Token = Guid.NewGuid().ToString();
            if (!Redis.LockTake(Key, Token, Timeout))
            {
                throw new AccessViolationException("Unable to take lock on key");
            }
        }

        public void Dispose()
        {
            Redis.LockRelease(Key, Token);
        }
    }
}
