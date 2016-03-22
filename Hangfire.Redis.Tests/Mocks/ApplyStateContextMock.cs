using System;
using Hangfire.States;
using NSubstitute;
using System.Linq;
using Hangfire.Storage;

namespace Hangfire.Redis.StackExchange.Tests
{
    public class ApplyStateContextMock
    {
        public IState NewStateValue { get; set; }
        public string OldStateName { get; set; }
        public BackgroundJobMock Job { get; set; }
        public IWriteOnlyTransaction Transaction { get; set; }
        public ApplyStateContext Object
        {
            get { return _context.Value; }
        }

        private readonly Lazy<ApplyStateContext> _context;

        public ApplyStateContextMock()
        {
            NewStateValue = Substitute.For<IState>();
            OldStateName = null;
            Job = new BackgroundJobMock();
            Transaction = Substitute.For<IWriteOnlyTransaction>();

            _context = new Lazy<ApplyStateContext>(
                () => new ApplyStateContext(Substitute.For<JobStorage>(),
                Substitute.For<IStorageConnection>(),
                Transaction,
                Job.BackgroundJob,
                NewStateValue,
                OldStateName));
        }
    }
}
