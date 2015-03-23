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

using Hangfire.Storage;
using StackExchange.Redis;
using System;

namespace Hangfire.Redis.StackExchange
{
    internal class RedisFetchedJob : IFetchedJob
    {
        private bool _disposed;
        private bool _removedFromQueue;
        private bool _requeued;
        private IDatabase _client;

        public RedisFetchedJob(IDatabase client, string jobId, string queue)
        {
            if (client == null) throw new ArgumentNullException("client");
            if (jobId == null) throw new ArgumentNullException("jobId");
            if (queue == null) throw new ArgumentNullException("queue");

            _client = client;
            JobId = jobId;
            Queue = queue;
        }

        public string JobId { get; private set; }
        public string Queue { get; private set; }

        public void RemoveFromQueue()
        {
            var transaction = _client.CreateTransaction();
            RemoveFromFetchedList(transaction);
            transaction.Execute();

            _removedFromQueue = true;
        }

        public void Requeue()
        {
            var transaction = _client.CreateTransaction();
            transaction.ListRightPushAsync(String.Format(RedisStorage.Prefix + "queue:{0}", Queue), JobId);
            RemoveFromFetchedList(transaction);
            transaction.Execute();

            _requeued = true;
        }

        public void Dispose()
        {
            if (_disposed) return;

            if (!_removedFromQueue && !_requeued)
            {
                Requeue();
            }

            _disposed = true;
        }

        private void RemoveFromFetchedList(ITransaction transaction)
        {
            transaction.ListRemoveAsync(String.Format(RedisStorage.Prefix + "queue:{0}:dequeued", Queue), JobId, 1);
            transaction.HashDeleteAsync(String.Format(RedisStorage.Prefix + "job:{0}", JobId), "Fetched");
            transaction.HashDeleteAsync(String.Format(RedisStorage.Prefix + "job:{0}", JobId), "Checked");
        }
    }
}
