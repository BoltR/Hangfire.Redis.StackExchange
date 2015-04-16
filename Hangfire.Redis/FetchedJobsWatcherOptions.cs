// Copyright © 2013-2014 Sergey Odinokov.
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

using System;

namespace Hangfire.Redis.StackExchange
{
    internal class FetchedJobsWatcherOptions
    {
        public FetchedJobsWatcherOptions() : this(RedisStorage.DefaultPrefix) {}
        public FetchedJobsWatcherOptions(string Prefix)
        {
            FetchedLockTimeout = TimeSpan.FromMinutes(1);
            CheckedTimeout = TimeSpan.FromMinutes(1);
            SleepTimeout = TimeSpan.FromMinutes(1);
            this.Prefix = Prefix;
        }

        public TimeSpan FetchedLockTimeout { get; set; }
        public TimeSpan CheckedTimeout { get; set; }
        public TimeSpan SleepTimeout { get; set; }
        public string Prefix { get; set; }
    }
}