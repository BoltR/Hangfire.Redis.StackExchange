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

    public class RedisSubscribe : IDisposable
    {
        private ISubscriber _Subscriber;
        private ManualResetEvent _Event;

        private const string Channel = "Hangfire:announce";

        public RedisSubscribe(ISubscriber Subscriber)
        {
            _Subscriber = Subscriber;
            _Event = new ManualResetEvent(false);

            _Subscriber.Subscribe(Channel, delegate
            {
                _Event.Set();
            });
        }

        public void JobClaimed()
        {
            _Event.Reset();
        }

        public void WaitForJob(CancellationToken token)
        {
            WaitHandle.WaitAny(new WaitHandle[] { _Event, token.WaitHandle });
            token.ThrowIfCancellationRequested();
        }

        public void AnnounceJob()
        {
            _Subscriber.Publish(Channel, "1");
        }

        public void Dispose()
        {
            _Subscriber.Unsubscribe(Channel);
            _Event.Dispose();
        }
}
}
