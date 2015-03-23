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
            return UseConnection(redis => redis.SortedSetLength("hangfire:schedule"));
        }

        public long EnqueuedCount(string queue)
        {
            return UseConnection(redis => redis.ListLength(String.Format("hangfire:queue:{0}", queue)));
        }

        public long FetchedCount(string queue)
        {
            return UseConnection(redis => redis.ListLength(String.Format("hangfire:queue:{0}:dequeued", queue)));
        }

        public long FailedCount()
        {
            return UseConnection(redis => redis.SortedSetLength("hangfire:failed"));
        }

        public long ProcessingCount()
        {
            return UseConnection(redis => redis.SortedSetLength("hangfire:processing"));
        }

        public long DeletedListCount()
        {
            return UseConnection(redis => redis.ListLength("hangfire:deleted"));
        }

        public JobList<ProcessingJobDto> ProcessingJobs(
            int from, int count)
        {
            return UseConnection(redis =>
            {
                var jobIds = redis.SortedSetRangeByRank(
                    "hangfire:processing",
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
                        InProcessingState = ProcessingState.StateName.Equals(
                            state[3], StringComparison.OrdinalIgnoreCase),
                    }).OrderBy(x => x.Value.StartedAt).ToList());
            });
        }

        public JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
        {
            return UseConnection(redis =>
            {
                var scheduledJobs = redis.SortedSetRangeByRankWithScores(
                    "hangfire:schedule",
                    from,
                    from + count - 1);

                if (scheduledJobs.Length == 0)
                {
                    return new JobList<ScheduledJobDto>(new List<KeyValuePair<string, ScheduledJobDto>>());
                }

                var jobs = new Dictionary<string, Task<RedisValue[]>>();
                var states = new Dictionary<string, Task<RedisValue[]>>();

                var batch = redis.CreateBatch();
                foreach (var scheduledJob in scheduledJobs)
                {
                    var job = scheduledJob;

                    jobs.Add(job.Element,  batch.HashGetAsync(String.Format("hangfire:job:{0}", job.Element), new RedisValue[]
                        { "Type", "Method", "ParameterTypes", "Arguments" }));

                    states.Add(job.Element, batch.HashGetAsync(String.Format("hangfire:job:{0}:state", job.Element), new RedisValue[]
                        {"State", "ScheduledAt"} ));
                }
                batch.Execute();
                Task.WaitAll(GetTaskWaiter(jobs, states));

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

        private Task[] GetTaskWaiter<T>(params Dictionary<string, Task<T>>[] Dictionaries)
        {
            int Total = Dictionaries.Sum(x => x.Count);
            var Items = new Task[Total];
            int i = 0;
            foreach (var Dict in Dictionaries)
            {
                foreach (var item in Dict)
                {
                    Items[i++] = item.Value;
                }
            }

            return Items;

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
                var serverNames = redis.SetMembers("hangfire:servers");

                if (serverNames.Length == 0)
                {
                    return new List<ServerDto>();
                }

                var servers = new Dictionary<string, Task<RedisValue[]>>();
                var queues = new Dictionary<string, Task<RedisValue[]>>();

                var batch = redis.CreateBatch();
                foreach (var serverName in serverNames)
                {
                    var name = serverName;

                    servers.Add(name, batch.HashGetAsync(String.Format("hangfire:server:{0}", name), 
                        new RedisValue[] { "WorkerCount", "StartedAt", "Heartbeat" }));

                    queues.Add(name, batch.ListRangeAsync(String.Format("hangfire:server:{0}:queues", name)));
                }

                batch.Execute();
                Task.WaitAll(GetTaskWaiter(servers, queues));

                return serverNames.Select(x => new ServerDto
                {
                    Name = x,
                    WorkersCount = int.Parse(servers[x].Result[0]),
                    Queues = RedisToList(queues[x].Result),
                    StartedAt = JobHelper.DeserializeDateTime(servers[x].Result[1]),
                    Heartbeat = JobHelper.DeserializeNullableDateTime(servers[x].Result[2])
                }).ToList();
            });
        }

        private IList<string> RedisToList(RedisValue[] Values)
        {
            var Result = new List<string>();
            foreach (var item in Values)
            {
                Result.Add(item);
            }
            return Result;
        }

        public JobList<FailedJobDto> FailedJobs(int from, int count)
        {
            return UseConnection(redis =>
            {
                var failedJobIds = redis.SortedSetRangeByRank(
                    "hangfire:failed",
                    from,
                    from + count - 1,
                    Order.Descending);

                return GetJobsWithProperties(
                    redis,
                    failedJobIds,
                    null,
                    new RedisValue[] { "FailedAt", "ExceptionType", "ExceptionMessage", "ExceptionDetails", "State", "Reason" },
                    (job, jobData, state) => new FailedJobDto
                    {
                        Job = job,
                        Reason = state[5],
                        FailedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        ExceptionType = state[1],
                        ExceptionMessage = state[2],
                        ExceptionDetails = state[3],
                        InFailedState = FailedState.StateName.Equals(state[4], StringComparison.OrdinalIgnoreCase)
                    });
            });
        }

        public JobList<SucceededJobDto> SucceededJobs(int from, int count)
        {
            return UseConnection(redis =>
            {
                var succeededJobIds = redis.ListRange(
                    "hangfire:succeeded",
                    from,
                    from + count - 1);

                return GetJobsWithProperties(
                    redis,
                    succeededJobIds,
                    null,
                    new RedisValue[] { "SucceededAt", "PerformanceDuration", "Latency", "State", "Result" },
                    (job, jobData, state) => new SucceededJobDto
                    {
                        Job = job,
                        Result = state[4],
                        SucceededAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        TotalDuration = state[1] != null && state[2] != null
                            ? (long?) long.Parse(state[1]) + (long?) long.Parse(state[2])
                            : null,
                        InSucceededState = SucceededState.StateName.Equals(state[3], StringComparison.OrdinalIgnoreCase)
                    });
            });
        }

        public JobList<DeletedJobDto> DeletedJobs(int @from, int count)
        {
            return UseConnection(redis =>
            {
                var deletedJobIds = redis.ListRange(
                    "hangfire:deleted",
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
                var queues = redis.SetMembers("hangfire:queues");
                var result = new List<QueueWithTopEnqueuedJobsDto>(queues.Length);

                foreach (var queue in queues)
                {
                    var batch = redis.CreateBatch();
                    Task<RedisValue[]> firstJobIds = batch.ListRangeAsync(String.Format("hangfire:queue:{0}", queue), -5, -1);
                    Task<long> length = batch.ListLengthAsync(String.Format("hangfire:queue:{0}", queue));
                    Task<long> fetched = batch.ListLengthAsync(String.Format("hangfire:queue:{0}:dequeued", queue));

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

        public JobList<EnqueuedJobDto> EnqueuedJobs(
            string queue, int from, int perPage)
        {
            return UseConnection(redis =>
            {
                var jobIds = redis.ListRange(
                    String.Format("hangfire:queue:{0}", queue),
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

        public JobList<FetchedJobDto> FetchedJobs(
            string queue, int from, int perPage)
        {
            return UseConnection(redis =>
            {
                var jobIds = redis.ListRange(
                    String.Format("hangfire:queue:{0}:dequeued", queue),
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
                //TODO: Remove dictionary?
                var job = redis.HashGetAll(String.Format("hangfire:job:{0}", jobId)).ToDictionary(x => (string)x.Name, x => (string)x.Value);
                if (job.Count == 0) return null;

                var hiddenProperties = new[] { "Type", "Method", "ParameterTypes", "Arguments", "State", "CreatedAt" };

                var thistoryList = redis.ListRange(
                    String.Format("hangfire:job:{0}:history", jobId));

                string[] historyList = new string[thistoryList.Length];
                for (int i = 0; i < historyList.Length; i++)
                {
                    historyList[i] = (string)thistoryList[i];
                }

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

        private Dictionary<DateTime, long> GetHourlyTimelineStats(
            IDatabase redis, string type)
        {
            var endDate = DateTime.UtcNow;
            var dates = new List<DateTime>();
            for (var i = 0; i < 24; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddHours(-1);
            }
            var keys = dates.Select(x => (RedisKey)String.Format("hangfire:stats:{0}:{1}", type, x.ToString("yyyy-MM-dd-HH"))).ToArray();
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

        private Dictionary<DateTime, long> GetTimelineStats(
            IDatabase redis, string type)
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
            var keys = stringDates.Select(x => (RedisKey)String.Format("hangfire:stats:{0}:{1}", type, x)).ToArray();

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
            var UsedKeys = 

            properties = properties ?? new RedisValue[0];

            var batch = redis.CreateBatch();

            foreach (var jobId in jobIds)
            {
                var id = jobId;

                jobs.Add(id, batch.HashGetAsync(String.Format("hangfire:job:{0}", id), properties.Union(new RedisValue[] { "Type", "Method", "ParameterTypes", "Arguments" }).ToArray()));

                if (stateProperties != null)
                {
                    states.Add(id, batch.HashGetAsync(String.Format("hangfire:job:{0}:state", id), stateProperties));
                }

            }

            batch.Execute();
            Task.WaitAll(GetTaskWaiter(jobs, states));

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
                    State = states.ContainsKey(x) ? states[x].Result.Select(y => (string)y).ToList() : null
                })
                .Select(x => new KeyValuePair<string, T>(
                    x.JobId,
                    x.Job.TrueForAll(y => y == RedisValue.Null)
                        ? default(T)
                        : selector(x.Method, x.Job, x.State)))
                .ToList());
        }

        public long SucceededListCount()
        {
            return UseConnection(redis => redis.ListLength("hangfire:succeeded"));
        }

        public StatisticsDto GetStatistics()
        {
            return UseConnection(redis =>
            {
                var stats = new StatisticsDto();

                var queues = redis.SetMembers("hangfire:queues");


                var batch = redis.CreateBatch();
                var Server = batch.SetLengthAsync("hangfire:servers");
                var Queues = batch.SetLengthAsync("hangfire:queues");
                var Scheduled = batch.SortedSetLengthAsync("hangfire:schedule");
                var Processing = batch.SortedSetLengthAsync("hangfire:processing");
                var Succeeded = batch.StringGetAsync("hangfire:stats:succeeded");
                var Failed = batch.SortedSetLengthAsync("hangfire:failed");
                var Deleted = batch.StringGetAsync("hangfire:stats:deleted");
                var Recurring = batch.SortedSetLengthAsync("hangfire:recurring-jobs");


                var QueueItems = new List<Task<long>>();
                foreach (var queue in queues)
                {
                    var queueName = queue;
                    QueueItems.Add(batch.ListLengthAsync(String.Format("hangfire:queue:{0}", queueName)));
                }

                batch.Execute();

                Task.WaitAll(Server, Queues, Scheduled, Processing, Succeeded, Failed, Deleted, Recurring);
                Task.WaitAll(QueueItems.ToArray());
                stats.Servers = Server.Result;
                stats.Queues = Queues.Result;
                stats.Scheduled = Scheduled.Result;
                stats.Processing = Processing.Result;
                stats.Succeeded = long.Parse(Succeeded.Result == RedisValue.Null ? "0" : (string)Succeeded.Result);
                stats.Failed = Failed.Result;
                stats.Deleted = long.Parse(Deleted.Result == RedisValue.Null ? "0" : (string)Deleted.Result);
                stats.Recurring = Recurring.Result;
                
                foreach( var item in QueueItems)
                {
                    stats.Enqueued += item.Result;
                }

                return stats;
            });
        }

        private T UseConnection<T>(Func<IDatabase, T> action)
        {
            return action(Redis.GetDatabase());
        }

        private static Job TryToGetJob(
            string type, string method, string parameterTypes, string arguments)
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
