using System;
using Hangfire.Common;

namespace Hangfire.Redis.StackExchange.Tests
{
    public class BackgroundJobMock
    {
        public string Id { get; set; }
        public Job Job { get; set; }
        public DateTime CreatedAt { get; set; }
        public BackgroundJob BackgroundJob
        {
            get { return _backgroundJob.Value; }
        }

        private readonly Lazy<BackgroundJob> _backgroundJob;

        public BackgroundJobMock()
        {
            Id = "JobId";
            Job = Job.FromExpression(() => Console.WriteLine("Test"));
            CreatedAt = DateTime.UtcNow;

            _backgroundJob = new Lazy<BackgroundJob>(() => new BackgroundJob(Id, Job, CreatedAt));
        }
    }
}
