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

namespace Hangfire.Redis.StackExchange
{
    internal class RedisWriteOnlyTransaction : IWriteOnlyTransaction
    {
        private readonly ITransaction Transaction;
        private readonly RedisSubscribe _sub;
        private readonly string Prefix;

        public RedisWriteOnlyTransaction(ITransaction transaction, RedisSubscribe sub, string prefix)
        {
            if (transaction == null) throw new ArgumentNullException("transaction");

            Transaction = transaction;
            _sub = sub;
            Prefix = prefix;
        }

        public void Dispose()
        {
        }

        public void Commit()
        {
            if (!Transaction.Execute())
            {
                // RedisTransaction.Commit returns false only when
                // WATCH condition has been failed. So, we should 
                // re-play the transaction.

                int replayCount = 1;
                const int maxReplayCount = 3;

                while (!Transaction.Execute())
                {
                    if (replayCount++ >= maxReplayCount)
                    {
                        throw new TimeoutException("Transaction commit was failed due to WATCH condition failure. Retry attempts exceeded.");
                    }
                }
            }
        }

        public void ExpireJob(string jobId, TimeSpan expireIn)
        {
            Transaction.KeyExpireAsync(String.Format(Prefix + "job:{0}", jobId), expireIn);
            Transaction.KeyExpireAsync(String.Format(Prefix + "job:{0}:history", jobId), expireIn);
            Transaction.KeyExpireAsync(String.Format(Prefix + "job:{0}:state", jobId), expireIn);
        }

        public void PersistJob(string jobId)
        {
            Transaction.KeyPersistAsync(String.Format(Prefix + "job:{0}", jobId));
            Transaction.KeyPersistAsync(String.Format(Prefix + "job:{0}:history", jobId));
            Transaction.KeyPersistAsync(String.Format(Prefix + "job:{0}:state", jobId));
        }

        public void SetJobState(string jobId, IState state)
        {
            Transaction.HashSetAsync(String.Format(Prefix + "job:{0}", jobId), "State", state.Name);

            Transaction.KeyDeleteAsync(String.Format(Prefix + "job:{0}:state", jobId));

            var Serialized = new Dictionary<string,string>(state.SerializeData());
            Serialized.Add("State", state.Name);
            if (state.Reason != null)
            {
                Serialized.Add("Reason", state.Reason);
            }
            
            Transaction.HashSetAsync(String.Format(Prefix + "job:{0}:state", jobId), Serialized.ToHashEntryArray());

            AddJobState(jobId, state);
        }

        public void AddJobState(string jobId, IState state)
        {
            var Serialized = new Dictionary<string,string>(state.SerializeData());
            Serialized.Add("State", state.Name);
            Serialized.Add("Reason", state.Reason);
            Serialized.Add("CreatedAt", JobHelper.SerializeDateTime(DateTime.UtcNow));

            Transaction.ListRightPushAsync(String.Format(Prefix + "job:{0}:history", jobId), JobHelper.ToJson(Serialized));
        }

        public void AddToQueue(string queue, string jobId)
        {
            Transaction.SortedSetAddAsync(Prefix + "queues", queue, 0);
            Transaction.ListLeftPushAsync(String.Format(Prefix + "queue:{0}", queue), jobId);
            _sub.AnnounceJob();
        }

        public void IncrementCounter(string key)
        {
            Transaction.StringIncrementAsync(GetRedisKey(key));
        }

        public void IncrementCounter(string key, TimeSpan expireIn)
        {
            var fullkey = GetRedisKey(key);
            Transaction.StringIncrementAsync(fullkey);
            Transaction.KeyExpireAsync(fullkey, expireIn);
        }

        public void DecrementCounter(string key)
        {
            Transaction.StringDecrementAsync(GetRedisKey(key));
        }

        public void DecrementCounter(string key, TimeSpan expireIn)
        {
            var fullkey = GetRedisKey(key);
            Transaction.StringDecrementAsync(fullkey);
            Transaction.KeyExpireAsync(fullkey, expireIn);
        }

        public void AddToSet(string key, string value)
        {
            Transaction.SortedSetAddAsync(GetRedisKey(key), value, 0); //TODO: What is ServiceStack exactly doing with no 3rd parameter?
        }

        public void AddToSet(string key, string value, double score)
        {
            Transaction.SortedSetAddAsync(GetRedisKey(key), value, score);
        }

        public void RemoveFromSet(string key, string value)
        {
            Transaction.SortedSetRemoveAsync(GetRedisKey(key), value);
        }

        public void InsertToList(string key, string value)
        {
            Transaction.ListLeftPushAsync(GetRedisKey(key), value);
        }

        public void RemoveFromList(string key, string value)
        {
            Transaction.ListRemoveAsync(GetRedisKey(key), value);
        }

        public void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            Transaction.ListTrimAsync(GetRedisKey(key), keepStartingFrom, keepEndingAt);
        }

        public void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (keyValuePairs == null) throw new ArgumentNullException("keyValuePairs");

            Transaction.HashSetAsync(GetRedisKey(key), keyValuePairs.ToHashEntryArray());
        }

        public void RemoveHash(string key)
        {
            Transaction.KeyDeleteAsync(GetRedisKey(key));
        }

        private string GetRedisKey(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            return Prefix + key;
        }
    }
}
