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
using Hangfire.Server;
using Hangfire.Storage;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Hangfire.Redis.StackExchange
{
    internal class RedisConnection : JobStorageConnection
    {
        private static readonly TimeSpan FetchTimeout = TimeSpan.FromSeconds(1);
        public RedisConnection(IDatabase redis, RedisSubscribe sub, string StorageLockID)
        {
            Redis = redis;
            Sub = sub;
            LockID = StorageLockID;
        }

        public IDatabase Redis { get; private set; }
        public RedisSubscribe Sub {get; private set;}
        private string LockID;
        public override void Dispose()
        {
        }

        public override IWriteOnlyTransaction CreateWriteTransaction()
        {
            return new RedisWriteOnlyTransaction(Redis.CreateTransaction(), Sub);
        }

        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            string jobId;
            string queueName;
            var queueIndex = 0;

            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                queueIndex = (queueIndex + 1) % queues.Length;
                queueName = queues[queueIndex];

                var queueKey = RedisStorage.Prefix + String.Format("queue:{0}", queueName);
                var fetchedKey = RedisStorage.Prefix + String.Format("queue:{0}:dequeued", queueName);

                //Always assume that there might be a job to do on first check
                jobId = Redis.ListRightPopLeftPush(queueKey, fetchedKey);

                if (jobId == null)
                {
                    Sub.WaitForJob(cancellationToken);
                    jobId = Redis.ListRightPopLeftPush(queueKey, fetchedKey);
                    Sub.JobClaimed();
                }
            } while (jobId == null);

            // The job was fetched by the server. To provide reliability,
            // we should ensure, that the job will be performed and acquired
            // resources will be disposed even if the server will crash 
            // while executing one of the subsequent lines of code.

            // The job's processing is splitted into a couple of checkpoints.
            // Each checkpoint occurs after successful update of the 
            // job information in the storage. And each checkpoint describes
            // the way to perform the job when the server was crashed after
            // reaching it.

            // Checkpoint #1-1. The job was fetched into the fetched list,
            // that is being inspected by the FetchedJobsWatcher instance.
            // Job's has the implicit 'Fetched' state.
            Redis.HashSet(String.Format(RedisStorage.Prefix + "job:{0}", jobId), "Fetched", JobHelper.SerializeDateTime(DateTime.UtcNow));

            // Checkpoint #2. The job is in the implicit 'Fetched' state now.
            // This state stores information about fetched time. The job will
            // be re-queued when the JobTimeout will be expired.

            return new RedisFetchedJob(Redis, jobId, queueName);
        }

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            return new RedisLock(Redis, RedisStorage.Prefix + resource, LockID, timeout);
        }

        public override string CreateExpiredJob(
            Job job,
            IDictionary<string, string> parameters, 
            DateTime createdAt,
            TimeSpan expireIn)
        {

            var jobId = Guid.NewGuid().ToString();

            var invocationData = InvocationData.Serialize(job);

            // Do not modify the original parameters.
            var test = new HashEntry[parameters.Count + 5];
            int i = 0;
            foreach (var item in parameters)
            {
                test[i++] = new HashEntry(item.Key, item.Value);

            }
            test[i++] = new HashEntry("Type", invocationData.Type);
            test[i++] = new HashEntry("Method", invocationData.Method);
            test[i++] = new HashEntry("ParameterTypes", invocationData.ParameterTypes);
            test[i++] = new HashEntry("Arguments", invocationData.Arguments);
            test[i] = new HashEntry("CreatedAt", JobHelper.SerializeDateTime(createdAt));

            var Key = String.Format(RedisStorage.Prefix + "job:{0}", jobId);
            var transaction = Redis.CreateTransaction();
           
            transaction.HashSetAsync(Key, test);
            transaction.KeyExpireAsync(Key, expireIn);
            transaction.Execute();
            return jobId;
        }

        public override JobData GetJobData(string id)
        {
            var storedData = Redis.HashGetAll(String.Format(RedisStorage.Prefix + "job:{0}", id)).ToStringDictionary();

            if (storedData.Count == 0) return null;

            string type = null;
            string method = null;
            string parameterTypes = null;
            string arguments = null;
            string createdAt = null;

            if (storedData.ContainsKey("Type"))
            {
                type = storedData["Type"];
            }
            if (storedData.ContainsKey("Method"))
            {
                method = storedData["Method"];
            }
            if (storedData.ContainsKey("ParameterTypes"))
            {
                parameterTypes = storedData["ParameterTypes"];
            }
            if (storedData.ContainsKey("Arguments"))
            {
                arguments = storedData["Arguments"];
            }
            if (storedData.ContainsKey("CreatedAt"))
            {
                createdAt = storedData["CreatedAt"];
            }

            Job job = null;
            JobLoadException loadException = null;

            var invocationData = new InvocationData(type, method, parameterTypes, arguments);

            try
            {
                job = invocationData.Deserialize();
            }
            catch (JobLoadException ex)
            {
                loadException = ex;
            }
            
            return new JobData
            {
                Job = job,
                State = storedData.ContainsKey("State") ? storedData["State"] : null,
                CreatedAt = JobHelper.DeserializeNullableDateTime(createdAt) ?? DateTime.MinValue,
                LoadException = loadException
            };
        }

        public override StateData GetStateData(string jobId)
        {
            if (jobId == null) throw new ArgumentNullException("jobId");
            var entries = Redis.HashGetAll(RedisStorage.Prefix + String.Format("job:{0}:state", jobId)).ToStringDictionary();

            if (entries.Count == 0) return null;

            var stateData = new Dictionary<string, string>(entries);
            stateData.Remove("State");
            stateData.Remove("Reason");

            return new StateData
            {
                Name = entries.ContainsKey("State") ? entries["State"] : null,
                Reason = entries.ContainsKey("Reason") ? entries["Reason"] : null,
                Data = stateData
            };
        }

        public override void SetJobParameter(string id, string name, string value)
        {
            Redis.HashSet(String.Format(RedisStorage.Prefix + "job:{0}", id), name, value);
        }

        public override string GetJobParameter(string id, string name)
        {
            return Redis.HashGet(String.Format(RedisStorage.Prefix + "job:{0}", id), name);
        }

        public override long GetCounter(string key)
        {
            return Convert.ToInt64(Redis.StringGet(RedisStorage.GetRedisKey(key)));
        }

        public override long GetSetCount(string key)
        {
            return Redis.SortedSetLength(RedisStorage.GetRedisKey(key));
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            var result = Redis.SortedSetScan(RedisStorage.GetRedisKey(key));
            var set = new HashSet<string>();
            foreach (var value in result)
            {
                set.Add(value.Element);
            }
            return set;
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            return Redis.SortedSetRangeByRank(RedisStorage.GetRedisKey(key), startingFrom, endingAt).ToStringList();
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            return Redis.SortedSetRangeByScore(RedisStorage.GetRedisKey(key), fromScore, toScore, take: 1).FirstOrDefault();
        }

        public override TimeSpan GetSetTtl(string key)
        {
            return Redis.KeyTimeToLive(RedisStorage.GetRedisKey(key)) ?? TimeSpan.FromSeconds(-1);
        }

        public override long GetHashCount(string key)
        {
            return Redis.HashLength(RedisStorage.GetRedisKey(key));
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException("key");
            if (keyValuePairs == null) throw new ArgumentNullException("keyValuePairs");

            Redis.HashSet(RedisStorage.GetRedisKey(key), keyValuePairs.ToHashEntryArray());
        }

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            var result = Redis.HashGetAll(RedisStorage.GetRedisKey(key)).ToStringDictionary();

            return result.Count != 0 ? result : null;
        }

        public override TimeSpan GetHashTtl(string key)
        {
            return Redis.KeyTimeToLive(RedisStorage.GetRedisKey(key)) ?? TimeSpan.FromSeconds(-1);
        }

        public override string GetValueFromHash(string key, string name)
        {
            return Redis.HashGet(RedisStorage.GetRedisKey(key), name);
        }

        public override long GetListCount(string key)
        {
            return Redis.ListLength(RedisStorage.GetRedisKey(key));
        }

        public override TimeSpan GetListTtl(string key)
        {
            return Redis.KeyTimeToLive(RedisStorage.GetRedisKey(key)) ?? TimeSpan.FromSeconds(-1);
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            return Redis.ListRange(RedisStorage.GetRedisKey(key), startingFrom, endingAt).ToStringList();
        }

        public override List<string> GetAllItemsFromList(string key)
        {
            return Redis.ListRange(RedisStorage.GetRedisKey(key)).ToStringList();
        }

        public override void AnnounceServer(string serverId, ServerContext context)
        {

            var transaction = Redis.CreateTransaction();

            transaction.SetAddAsync(RedisStorage.Prefix + "servers", serverId);
            
            transaction.HashSetAsync(String.Format(RedisStorage.Prefix + "server:{0}", serverId),
                new HashEntry[2] {
                    new HashEntry("WorkerCount", context.WorkerCount.ToString(CultureInfo.InvariantCulture)),
                    new HashEntry("StartedAt", JobHelper.SerializeDateTime(DateTime.UtcNow))
            });


            foreach (var queue in context.Queues)
            {
                var queue1 = queue;
                transaction.ListRightPushAsync(String.Format(RedisStorage.Prefix + "server:{0}:queues", serverId), queue1);
            }

            transaction.Execute();
        }

        public override void RemoveServer(string serverId)
        {
            RemoveServer(Redis, serverId);
        }

        public static void RemoveServer(IDatabase redis, string serverId)
        {
            var transaction = redis.CreateTransaction();
            transaction.SetRemoveAsync(RedisStorage.Prefix + "servers", serverId);
            transaction.KeyDeleteAsync(String.Format(RedisStorage.Prefix + "server:{0}", serverId));
            transaction.KeyDeleteAsync(String.Format(RedisStorage.Prefix + "server:{0}:queues", serverId));
            transaction.Execute();
        }

        public override void Heartbeat(string serverId)
        {
            Redis.HashSet(String.Format(RedisStorage.Prefix + "server:{0}", serverId), "Heartbeat", JobHelper.SerializeDateTime(DateTime.UtcNow));
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            var serverNames = Redis.SetMembers(RedisStorage.Prefix + "servers");

            var utcNow = DateTime.UtcNow;

            var heartbeats = new List<KeyValuePair<string, RedisValue[]>>();
            foreach (var serverName in serverNames)
            {
                var name = serverName;

                heartbeats.Add(new KeyValuePair<string,RedisValue[]>(name, Redis.HashGet(String.Format(RedisStorage.Prefix + "server:{0}", name), new RedisValue[] { "StartedAt", "Heartbeat" })));
            }

            var removedServerCount = 0;
            foreach (var heartbeat in heartbeats)
            {
                var maxTime = new DateTime(
                    Math.Max(JobHelper.DeserializeDateTime(heartbeat.Value[0]).Ticks, (JobHelper.DeserializeNullableDateTime(heartbeat.Value[1]) ?? DateTime.MinValue).Ticks));

                if (utcNow > maxTime.Add(timeOut))
                {
                    RemoveServer(Redis, heartbeat.Key);
                    removedServerCount++;
                }
            }

            return removedServerCount;
        }
    }
}