using System;
using Hangfire.States;
using Hangfire.Storage;
using NSubstitute;
using Xunit;

namespace Hangfire.Redis.StackExchange.Tests
{
    public class FailedStateHandlerFacts
    {
        private const string JobId = "1";

        private readonly ApplyStateContextMock _context;
        private readonly IWriteOnlyTransaction _transaction;

        public FailedStateHandlerFacts()
        {
            _context = new ApplyStateContextMock();
            _context.StateContextValue.JobIdValue = JobId;
            _context.NewStateValue = new FailedState(new InvalidOperationException());

            _transaction = Substitute.For<IWriteOnlyTransaction>();
        }

        [Fact]
        public void StateName_ShouldBeEqualToFailedState()
        {
            var handler = new FailedStateHandler();
            Assert.Equal(FailedState.StateName, handler.StateName);
        }

        [Fact]
        public void Apply_ShouldAddTheJob_ToTheFailedSet()
        {
            var handler = new FailedStateHandler();
            handler.Apply(_context.Object, _transaction);

            _transaction.Received().AddToSet("failed", JobId, Arg.Any<double>());
        }

        [Fact]
        public void Unapply_ShouldRemoveTheJob_FromTheFailedSet()
        {
            var handler = new FailedStateHandler();
            handler.Unapply(_context.Object, _transaction);

            _transaction.Received().RemoveFromSet("failed", JobId);
        }
    }
}
