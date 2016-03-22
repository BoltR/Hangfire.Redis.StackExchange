using System;
using Hangfire.States;
using NSubstitute;
using Xunit;

namespace Hangfire.Redis.StackExchange.Tests
{
    public class FailedStateHandlerFacts
    {
        private const string JobId = "1";

        private readonly ApplyStateContextMock _context;

        public FailedStateHandlerFacts()
        {
            _context = new ApplyStateContextMock();

            _context.Job.Id = JobId;
            _context.NewStateValue = new FailedState(new InvalidOperationException());
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
            handler.Apply(_context.Object, _context.Transaction);

            _context.Transaction.Received().AddToSet("failed", JobId, Arg.Any<double>());
        }

        [Fact]
        public void Unapply_ShouldRemoveTheJob_FromTheFailedSet()
        {
            var handler = new FailedStateHandler();
            handler.Unapply(_context.Object, _context.Transaction);

            _context.Transaction.Received().RemoveFromSet("failed", JobId);
        }
    }
}
