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

using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Hangfire.Redis.StackExchange
{
    internal class RedisWriteOnlyTransaction : IWriteOnlyTransaction
    {
        private readonly ITransaction _transaction;
        private readonly RedisSubscribe _sub;
        public RedisWriteOnlyTransaction(ITransaction transaction, RedisSubscribe sub)
        {
            if (transaction == null) throw new ArgumentNullException("transaction");

            _transaction = transaction;
            _sub = sub;
        }

        public void Dispose()
        {
        }

        public void Commit()
        {
            if (!_transaction.Execute())
            {
                // RedisTransaction.Commit returns false only when
                // WATCH condition has been failed. So, we should 
                // re-play the transaction.

                int replayCount = 1;
                const int maxReplayCount = 3;

                while (!_transaction.Execute())
                {
                    if (replayCount++ >= maxReplayCount)
                    {
                        throw new Exception(); //TODO: What? RedisException("Transaction commit was failed due to WATCH condition failure. Retry attempts exceeded.");
                    }
                }
            }
        }

        public void ExpireJob(string jobId, TimeSpan expireIn)
        {
            _transaction.KeyExpireAsync(String.Format(RedisStorage.Prefix + "job:{0}", jobId), expireIn);

            _transaction.KeyExpireAsync(String.Format(RedisStorage.Prefix + "job:{0}:history", jobId), expireIn);

            _transaction.KeyExpireAsync(String.Format(RedisStorage.Prefix + "job:{0}:state", jobId), expireIn);
        }

        public void PersistJob(string jobId)
        {
            _transaction.KeyPersistAsync(String.Format(RedisStorage.Prefix + "job:{0}", jobId));
            _transaction.KeyPersistAsync(String.Format(RedisStorage.Prefix + "job:{0}:history", jobId));
            _transaction.KeyPersistAsync(String.Format(RedisStorage.Prefix + "job:{0}:state", jobId));
        }

        public void SetJobState(string jobId, IState state)
        {
            _transaction.HashSetAsync(String.Format(RedisStorage.Prefix + "job:{0}", jobId), "State", state.Name);

            _transaction.KeyDeleteAsync(String.Format(RedisStorage.Prefix + "job:{0}:state", jobId));

            var Serialized = state.SerializeData();
            if (state.Reason != null)
            {
                Serialized.Add("Reason", state.Reason);
            }
            var storedData = new HashEntry[Serialized.Count];
            int i = 0;
            foreach (var item in Serialized)
            {
                storedData[i++] = new HashEntry(item.Key, item.Value);
            }

            _transaction.HashSetAsync(String.Format(RedisStorage.Prefix + "job:{0}:state", jobId), storedData);

            AddJobState(jobId, state);
        }

        public void AddJobState(string jobId, IState state)
        {
            var Serialized = state.SerializeData();
            Serialized.Add("State", state.Name);
            Serialized.Add("Reason", state.Reason);
            Serialized.Add("CreatedAt", JobHelper.SerializeDateTime(DateTime.UtcNow));

            _transaction.ListLeftPushAsync(String.Format(RedisStorage.Prefix + "job:{0}:history", jobId), JobHelper.ToJson(Serialized));
        }

        public void AddToQueue(string queue, string jobId)
        {
            _transaction.SetAddAsync(RedisStorage.Prefix + "queues", queue);

            _transaction.ListLeftPushAsync(String.Format(RedisStorage.Prefix + "queue:{0}", queue), jobId);
            _sub.AnnounceJob();
        }

        public void IncrementCounter(string key)
        {
            _transaction.StringIncrementAsync(RedisStorage.Prefix + key);
        }

        public void IncrementCounter(string key, TimeSpan expireIn)
        {
            _transaction.StringIncrementAsync(RedisStorage.Prefix + key);
            _transaction.KeyExpireAsync(RedisStorage.Prefix + key, expireIn);
        }

        public void DecrementCounter(string key)
        {
            _transaction.StringDecrementAsync(RedisStorage.Prefix + key);
        }

        public void DecrementCounter(string key, TimeSpan expireIn)
        {
            _transaction.StringDecrementAsync(RedisStorage.Prefix + key);
            _transaction.KeyExpireAsync(RedisStorage.Prefix + key, expireIn);
        }

        public void AddToSet(string key, string value)
        {
            _transaction.SortedSetAddAsync(RedisStorage.Prefix + key, value, 0); //TODO: What is ServiceStack exactly doing with no 3rd parameter?
        }

        public void AddToSet(string key, string value, double score)
        {
            _transaction.SortedSetAddAsync(RedisStorage.Prefix + key, value, score);
        }

        public void RemoveFromSet(string key, string value)
        {
            _transaction.SortedSetRemoveAsync(RedisStorage.Prefix + key, value);
        }

        public void InsertToList(string key, string value)
        {
            _transaction.ListLeftPushAsync(RedisStorage.Prefix + key, value);
        }

        public void RemoveFromList(string key, string value)
        {
            _transaction.ListRemoveAsync(RedisStorage.Prefix + key, value);
        }

        public void TrimList(
            string key, int keepStartingFrom, int keepEndingAt)
        {
            _transaction.ListTrimAsync(RedisStorage.Prefix + key, keepStartingFrom, keepEndingAt);
        }

        public void SetRangeInHash(
            string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs) //TODO: Possible to convert to array Hashitems earlier?
        {
            if (key == null) throw new ArgumentNullException("key");
            if (keyValuePairs == null) throw new ArgumentNullException("keyValuePairs");

            var count = keyValuePairs.Count();
            var items = new HashEntry[count];
            int i = 0;
            foreach (var item in keyValuePairs)
            {
                items[i++] = new HashEntry(item.Key, item.Value);
            }

            _transaction.HashSetAsync(RedisStorage.GetRedisKey(key), items);
        }

        public void RemoveHash(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            _transaction.KeyDeleteAsync(RedisStorage.GetRedisKey(key));
        }
    }
}
