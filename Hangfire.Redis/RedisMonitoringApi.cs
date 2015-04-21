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
using Hangfire.Storage.Monitoring;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Hangfire.Redis.StackExchange
{
    internal class RedisMonitoringApi : IMonitoringApi
    {
        private RedisStorage Redis;
        public RedisMonitoringApi(RedisStorage Redis)
        {
            this.Redis = Redis;
        }

        public long ScheduledCount()
        {
            return UseConnection(redis => redis.SortedSetLength(Redis.Prefix + "schedule"));
        }

        public long EnqueuedCount(string queue)
        {
            return UseConnection(redis => redis.ListLength(String.Format(Redis.Prefix + "queue:{0}", queue)));
        }

        public long FetchedCount(string queue)
        {
            return UseConnection(redis => redis.ListLength(String.Format(Redis.Prefix + "queue:{0}:dequeued", queue)));
        }

        public long FailedCount()
        {
            return UseConnection(redis => redis.SortedSetLength(Redis.Prefix + "failed"));
        }

        public long ProcessingCount()
        {
            return UseConnection(redis => redis.SortedSetLength(Redis.Prefix + "processing"));
        }

        public long DeletedListCount()
        {
            return UseConnection(redis => redis.ListLength(Redis.Prefix + "deleted"));
        }

        public JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
        {
            return UseConnection(redis =>
            {
                var jobIds = redis.SortedSetRangeByRank(
                    Redis.Prefix + "processing",
                    from,
                    from + count - 1);

                return new JobList<ProcessingJobDto>(GetJobsWithProperties(redis,
                    jobIds,
                    null,
                    new RedisValue[] { "StartedAt", "ServerName", "ServerId", "State" },
                    (job, jobData, state) => new ProcessingJobDto
                    {
                        ServerId = state[2] ?? state[1],
                        Job = job,
                        StartedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        InProcessingState = ProcessingState.StateName.Equals(state[3], StringComparison.OrdinalIgnoreCase),
                    }).OrderBy(x => x.Value.StartedAt).ToList());
            });
        }

        public JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
        {
            return UseConnection(redis =>
            {
                var scheduledJobs = redis.SortedSetRangeByRankWithScores(
                    Redis.Prefix + "schedule",
                    from,
                    from + count - 1);

                if (scheduledJobs.Length == 0)
                {
                    return new JobList<ScheduledJobDto>(new List<KeyValuePair<string, ScheduledJobDto>>());
                }

                var jobs = new Dictionary<string, Task<RedisValue[]>>();
                var states = new Dictionary<string, Task<RedisValue[]>>();
                var Tasks = new Task[scheduledJobs.Length * 2];
                int i = 0;

                var batch = redis.CreateBatch();
                foreach (var scheduledJob in scheduledJobs)
                {
                    var JobTask = batch.HashGetAsync(String.Format(Redis.Prefix + "job:{0}", scheduledJob.Element), new RedisValue[] { "Type", "Method", "ParameterTypes", "Arguments" });
                    Tasks[i++] = JobTask;
                    jobs.Add(scheduledJob.Element, JobTask);

                    var StatesTask = batch.HashGetAsync(String.Format(Redis.Prefix + "job:{0}:state", scheduledJob.Element), new RedisValue[] { "State", "ScheduledAt" });
                    Tasks[i++]  = StatesTask;
                    states.Add(scheduledJob.Element, StatesTask);
                }
                batch.Execute();
                Task.WaitAll(Tasks);

                return new JobList<ScheduledJobDto>(scheduledJobs
                    .Select(job => new KeyValuePair<string, ScheduledJobDto>(
                        job.Element,
                        new ScheduledJobDto
                        {
                            EnqueueAt = JobHelper.FromTimestamp((long) job.Score),
                            Job = TryToGetJob(jobs[job.Element].Result[0], jobs[job.Element].Result[1], jobs[job.Element].Result[2], jobs[job.Element].Result[3]),
                            ScheduledAt =
                                states[job.Element].Result.Length > 1
                                    ? JobHelper.DeserializeNullableDateTime(states[job.Element].Result[1])
                                    : null,
                            InScheduledState =
                                ScheduledState.StateName.Equals(states[job.Element].Result[0], StringComparison.OrdinalIgnoreCase)
                        }))
                    .ToList());
            });
        }

        public IDictionary<DateTime, long> SucceededByDatesCount()
        {
            return UseConnection(redis => GetTimelineStats(redis, "succeeded"));
        }

        public IDictionary<DateTime, long> FailedByDatesCount()
        {
            return UseConnection(redis => GetTimelineStats(redis, "failed"));
        }

        public IList<ServerDto> Servers()
        {
            return UseConnection(redis =>
            {
                var serverNames = redis.SetMembers(Redis.Prefix + "servers");

                if (serverNames.Length == 0)
                {
                    return new List<ServerDto>();
                }

                var servers = new Dictionary<string, Task<RedisValue[]>>();
                var queues = new Dictionary<string, Task<RedisValue[]>>();
                var Tasks = new Task[serverNames.Length * 2];
                int i = 0;
                var batch = redis.CreateBatch();
                foreach (var serverName in serverNames)
                {
                    var ServersTask = batch.HashGetAsync(String.Format(Redis.Prefix + "server:{0}", serverName), new RedisValue[] { "WorkerCount", "StartedAt", "Heartbeat" });
                    Tasks[i++] = ServersTask;
                    servers.Add(serverName, ServersTask);

                    var QueuesTask = batch.ListRangeAsync(String.Format(Redis.Prefix + "server:{0}:queues", serverName));
                    Tasks[i++] = QueuesTask;
                    queues.Add(serverName, QueuesTask);
                }

                batch.Execute();
                Task.WaitAll(Tasks);

                return serverNames.Select(x => new ServerDto
                {
                    Name = x,
                    WorkersCount = int.Parse(servers[x].Result[0]),
                    Queues = queues[x].Result.ToStringList(),
                    StartedAt = JobHelper.DeserializeDateTime(servers[x].Result[1]),
                    Heartbeat = JobHelper.DeserializeNullableDateTime(servers[x].Result[2])
                }).ToList();
            });
        }

        public JobList<FailedJobDto> FailedJobs(int from, int count)
        {
            return UseConnection(redis =>
            {
                var failedJobIds = redis.SortedSetRangeByRank(
                    Redis.Prefix + "failed",
                    from,
                    from + count - 1,
                    Order.Descending);

                return GetJobsWithProperties(
                    redis,
                    failedJobIds,
                    null,
                    new RedisValue[] { "FailedAt", "ExceptionType", "ExceptionMessage", "ExceptionDetails", "Reason", "State" },
                    (job, jobData, state) => new FailedJobDto
                    {
                        Job = job,
                        Reason = state[4],
                        FailedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        ExceptionType = state[1],
                        ExceptionMessage = state[2],
                        ExceptionDetails = state[3],
                        InFailedState = FailedState.StateName.Equals(state[5], StringComparison.OrdinalIgnoreCase)
                    });
            });
        }

        public JobList<SucceededJobDto> SucceededJobs(int from, int count)
        {
            return UseConnection(redis =>
            {
                var succeededJobIds = redis.ListRange(
                    Redis.Prefix + "succeeded",
                    from,
                    from + count - 1);

                return GetJobsWithProperties(
                    redis,
                    succeededJobIds,
                    null,
                    new RedisValue[] { "SucceededAt", "PerformanceDuration", "Latency", "Result", "State", },
                    (job, jobData, state) => new SucceededJobDto
                    {
                        Job = job,
                        Result = state[3],
                        SucceededAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        TotalDuration = state[1] != null && state[2] != null
                            ? (long?) long.Parse(state[1]) + (long?) long.Parse(state[2])
                            : null,
                        InSucceededState = SucceededState.StateName.Equals(state[4], StringComparison.OrdinalIgnoreCase)
                    });
            });
        }

        public JobList<DeletedJobDto> DeletedJobs(int @from, int count)
        {
            return UseConnection(redis =>
            {
                var deletedJobIds = redis.ListRange(
                    Redis.Prefix + "deleted",
                    from,
                    from + count - 1);

                return GetJobsWithProperties(
                    redis,
                    deletedJobIds,
                    null,
                    new RedisValue[] { "DeletedAt", "State" },
                    (job, jobData, state) => new DeletedJobDto
                    {
                        Job = job,
                        DeletedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        InDeletedState = DeletedState.StateName.Equals(state[1], StringComparison.OrdinalIgnoreCase)
                    });
            });
        }

        public IList<QueueWithTopEnqueuedJobsDto> Queues()
        {
            return UseConnection(redis =>
            {
                var queues = redis.SetMembers(Redis.Prefix + "queues");
                var result = new List<QueueWithTopEnqueuedJobsDto>(queues.Length);

                foreach (var queue in queues)
                {
                    var batch = redis.CreateBatch();
                    Task<RedisValue[]> firstJobIds = batch.ListRangeAsync(String.Format(Redis.Prefix + "queue:{0}", queue), -5, -1);
                    Task<long> length = batch.ListLengthAsync(String.Format(Redis.Prefix + "queue:{0}", queue));
                    Task<long> fetched = batch.ListLengthAsync(String.Format(Redis.Prefix + "queue:{0}:dequeued", queue));

                    batch.Execute();
                    Task.WaitAll(firstJobIds, length, fetched);

                    var jobs = GetJobsWithProperties(
                        redis,
                        firstJobIds.Result,
                        new RedisValue[] { "State" },
                        new RedisValue[] { "EnqueuedAt", "State" },
                        (job, jobData, state) => new EnqueuedJobDto
                        {
                            Job = job,
                            State = jobData[0],
                            EnqueuedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                            InEnqueuedState = jobData[0].Equals(state[1], StringComparison.OrdinalIgnoreCase)
                        });

                    result.Add(new QueueWithTopEnqueuedJobsDto
                    {
                        Name = queue,
                        FirstJobs = jobs,
                        Length = length.Result,
                        Fetched = fetched.Result
                    });
                }

                return result;
            });
        }

        public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int perPage)
        {
            return UseConnection(redis =>
            {
                var jobIds = redis.ListRange(
                    String.Format(Redis.Prefix + "queue:{0}", queue),
                    from,
                    from + perPage - 1);

                return GetJobsWithProperties(
                    redis,
                    jobIds,
                    new RedisValue[] { "State" },
                    new RedisValue[] { "EnqueuedAt", "State" },
                    (job, jobData, state) => new EnqueuedJobDto
                    {
                        Job = job,
                        State = jobData[0],
                        EnqueuedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        InEnqueuedState = jobData[0].Equals(state[1], StringComparison.OrdinalIgnoreCase)
                    });
            });
        }

        public JobList<FetchedJobDto> FetchedJobs(string queue, int from, int perPage)
        {
            return UseConnection(redis =>
            {
                var jobIds = redis.ListRange(
                    String.Format(Redis.Prefix + "queue:{0}:dequeued", queue),
                    from, from + perPage - 1);

                return GetJobsWithProperties(
                    redis,
                    jobIds,
                    new RedisValue[] { "State", "Fetched" },
                    null,
                    (job, jobData, state) => new FetchedJobDto
                    {
                        Job = job,
                        State = jobData[0],
                        FetchedAt = JobHelper.DeserializeNullableDateTime(jobData[1])
                    });
            });
        }

        public IDictionary<DateTime, long> HourlySucceededJobs()
        {
            return UseConnection(redis => GetHourlyTimelineStats(redis, "succeeded"));
        }

        public IDictionary<DateTime, long> HourlyFailedJobs()
        {
            return UseConnection(redis => GetHourlyTimelineStats(redis, "failed"));
        }

        public JobDetailsDto JobDetails(string jobId)
        {
            return UseConnection(redis =>
            {
                var job = redis.HashGetAll(String.Format(Redis.Prefix + "job:{0}", jobId)).ToStringDictionary();
                if (job.Count == 0) return null;

                var hiddenProperties = new[] { "Type", "Method", "ParameterTypes", "Arguments", "State", "CreatedAt" };

                var historyList = redis.ListRange(String.Format(Redis.Prefix + "job:{0}:history", jobId)).ToStringArray();

                var history = historyList
                    .Select(JobHelper.FromJson<Dictionary<string, string>>)
                    .ToList();

                var stateHistory = new List<StateHistoryDto>(history.Count);
                foreach (var entry in history)
                {
                    var dto = new StateHistoryDto
                    {
                        StateName = entry["State"],
                        Reason = entry.ContainsKey("Reason") ? entry["Reason"] : null,
                        CreatedAt = JobHelper.DeserializeDateTime(entry["CreatedAt"]),
                    };

                    // Each history item contains all of the information,
                    // but other code should not know this. We'll remove
                    // unwanted keys.
                    var stateData = new Dictionary<string, string>(entry);
                    stateData.Remove("State");
                    stateData.Remove("Reason");
                    stateData.Remove("CreatedAt");

                    dto.Data = stateData;
                    stateHistory.Add(dto);
                }

                // For compatibility
                if (!job.ContainsKey("Method")) job.Add("Method", null);
                if (!job.ContainsKey("ParameterTypes")) job.Add("ParameterTypes", null);

                return new JobDetailsDto
                {
                    Job = TryToGetJob(job["Type"], job["Method"], job["ParameterTypes"], job["Arguments"]),
                    CreatedAt =
                        job.ContainsKey("CreatedAt")
                            ? JobHelper.DeserializeDateTime(job["CreatedAt"])
                            : (DateTime?) null,
                    Properties =
                        job.Where(x => !hiddenProperties.Contains(x.Key)).ToDictionary(x => x.Key, x => x.Value),
                    History = stateHistory
                };
            });
        }

        private Dictionary<DateTime, long> GetHourlyTimelineStats(IDatabase redis, string type)
        {
            var endDate = DateTime.UtcNow;
            var dates = new List<DateTime>();
            for (var i = 0; i < 24; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddHours(-1);
            }
            var keys = dates.Select(x => (RedisKey)String.Format(Redis.Prefix + "stats:{0}:{1}", type, x.ToString("yyyy-MM-dd-HH"))).ToArray();
            var valuesMap = redis.StringGet(keys);

            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < dates.Count; i++)
            {
                long value;
                if (!long.TryParse(valuesMap[i], out value))
                {
                    value = 0;
                }

                result.Add(dates[i], value);
            }

            return result;
        }

        private Dictionary<DateTime, long> GetTimelineStats(IDatabase redis, string type)
        {
            var endDate = DateTime.UtcNow.Date;
            var startDate = endDate.AddDays(-7);
            var dates = new List<DateTime>();

            while (startDate <= endDate)
            {
                dates.Add(endDate);
                endDate = endDate.AddDays(-1);
            }

            var stringDates = dates.Select(x => x.ToString("yyyy-MM-dd")).ToList();
            var keys = stringDates.Select(x => (RedisKey)String.Format(Redis.Prefix + "stats:{0}:{1}", type, x)).ToArray();

            var valuesMap = redis.StringGet(keys);

            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < stringDates.Count; i++)
            {
                long value;
                if (!long.TryParse(valuesMap[i], out value))
                {
                    value = 0;
                }
                result.Add(dates[i], value);
            }

            return result;
        }

        private JobList<T> GetJobsWithProperties<T>(
            IDatabase redis,
            RedisValue[] jobIds,
            RedisValue[] properties,
            RedisValue[] stateProperties,
            Func<Job, List<string>, List<string>, T> selector)
        {
            if (jobIds.Length == 0) return new JobList<T>(new List<KeyValuePair<string, T>>());

            var jobs = new Dictionary<string, Task<RedisValue[]>>(jobIds.Length);
            var states = new Dictionary<string, Task<RedisValue[]>>(jobIds.Length);

            properties = properties ?? new RedisValue[0];

            var Tasks = new List<Task>(jobIds.Length);
            var batch = redis.CreateBatch();

            foreach (var jobId in jobIds)
            {
                var JobsTask = batch.HashGetAsync(String.Format(Redis.Prefix + "job:{0}", jobId), properties.Union(new RedisValue[] { "Type", "Method", "ParameterTypes", "Arguments" }).ToArray());
                Tasks.Add(JobsTask);
                jobs.Add(jobId, JobsTask);

                if (stateProperties != null)
                {
                    var StatesTask = batch.HashGetAsync(String.Format(Redis.Prefix + "job:{0}:state", jobId), stateProperties);
                    Tasks.Add(StatesTask);
                    states.Add(jobId, StatesTask);
                }

            }

            batch.Execute();
            Task.WaitAll(Tasks.ToArray());

            return new JobList<T>(jobIds
                .Select(x => new
                {
                    JobId = (string)x,
                    Job = jobs[x].Result.Select(y => (string)y).ToList(),
                    Method = TryToGetJob(
                        jobs[x].Result[properties.Length],
                        jobs[x].Result[properties.Length + 1],
                        jobs[x].Result[properties.Length + 2],
                        jobs[x].Result[properties.Length + 3]),
                    State = states.ContainsKey(x) ? states[x].Result.ToStringList() : null
                })
                .Select(x => new KeyValuePair<string, T>(
                    x.JobId,
                    x.Job.TrueForAll(y => y == null)
                        ? default(T)
                        : selector(x.Method, x.Job, x.State)))
                .ToList());
        }

        public long SucceededListCount()
        {
            return UseConnection(redis => redis.ListLength(Redis.Prefix + "succeeded"));
        }

        public StatisticsDto GetStatistics()
        {
            return UseConnection(redis =>
            {
                var stats = new StatisticsDto();

                var queues = redis.SetMembers(Redis.Prefix + "queues");
                var Tasks = new Task[queues.Length + 8];

                var batch = redis.CreateBatch();
                Tasks[0] = batch.SetLengthAsync(Redis.Prefix + "servers").ContinueWith(x => stats.Servers = x.Result);
                Tasks[1] = batch.SetLengthAsync(Redis.Prefix + "queues").ContinueWith(x => stats.Queues = x.Result);
                Tasks[2] = batch.SortedSetLengthAsync(Redis.Prefix + "schedule").ContinueWith(x => stats.Scheduled = x.Result);
                Tasks[3] = batch.SortedSetLengthAsync(Redis.Prefix + "processing").ContinueWith(x => stats.Processing = x.Result);
                Tasks[4] = batch.StringGetAsync(Redis.Prefix + "stats:succeeded").ContinueWith(x => stats.Succeeded = long.Parse(x.Result == RedisValue.Null ? "0" : (string)x.Result));
                Tasks[5] = batch.SortedSetLengthAsync(Redis.Prefix + "failed").ContinueWith(x => stats.Failed = x.Result);
                Tasks[6] = batch.StringGetAsync(Redis.Prefix + "stats:deleted").ContinueWith(x => stats.Deleted = long.Parse(x.Result == RedisValue.Null ? "0" : (string)x.Result));
                Tasks[7] = batch.SortedSetLengthAsync(Redis.Prefix + "recurring-jobs").ContinueWith(x => stats.Recurring = x.Result);

                long QueueItems = 0;
                int i = 8;
                foreach (var queue in queues)
                {
                    Tasks[i++] = batch.ListLengthAsync(String.Format(Redis.Prefix + "queue:{0}", queue)).ContinueWith(x => Interlocked.Add(ref QueueItems, x.Result));
                }

                batch.Execute();
                Task.WaitAll(Tasks);
                stats.Enqueued = QueueItems;

                return stats;
            });
        }

        private T UseConnection<T>(Func<IDatabase, T> action)
        {
            return action(Redis.GetDatabase());
        }

        private static Job TryToGetJob(string type, string method, string parameterTypes, string arguments)
        {
            try
            {
                return new InvocationData(
                    type,
                    method,
                    parameterTypes,
                    arguments).Deserialize();
            }
            catch (Exception)
            {
                return null;
            }
        }
    }
}
