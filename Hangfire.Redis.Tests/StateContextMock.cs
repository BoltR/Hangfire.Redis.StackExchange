using System;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using NSubstitute;

namespace Hangfire.Redis.StackExchange.Tests
{
    public class StateContextMock
    {
        private readonly Lazy<StateContext> _context;

        public StateContextMock()
        {
            JobIdValue = "job-id";
            JobValue = Job.FromExpression(() => Console.WriteLine());
            CreatedAtValue = DateTime.UtcNow;
            ConnectionValue = Substitute.For<IStorageConnection>();

            _context = new Lazy<StateContext>(
                () => new StateContext(JobIdValue, JobValue, CreatedAtValue, ConnectionValue));
        }

        public string JobIdValue { get; set; }
        public Job JobValue { get; set; }
        public DateTime CreatedAtValue { get; set; }

        public IStorageConnection ConnectionValue { get; set; }

        public StateContext Object
        {
            get { return _context.Value; }
        }
    }
}
