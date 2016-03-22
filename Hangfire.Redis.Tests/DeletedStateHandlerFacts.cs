using Hangfire.States;
using Hangfire.Storage;
using NSubstitute;
using Xunit;

namespace Hangfire.Redis.StackExchange.Tests
{
    public class DeletedStateHandlerFacts
    {
        private const string JobId = "1";

        private readonly ApplyStateContextMock _context;
        private readonly IWriteOnlyTransaction _transaction;

        public DeletedStateHandlerFacts()
        {
            _context = new ApplyStateContextMock();
            _context.Job.Id = JobId;
            _context.NewStateValue = new DeletedState();

            _transaction = Substitute.For<IWriteOnlyTransaction>();
        }

        [Fact]
        public void StateName_ShouldBeEqualToSucceededState()
        {
            var handler = new DeletedStateHandler();
            Assert.Equal(DeletedState.StateName, handler.StateName);
        }

        [Fact]
        public void Apply_ShouldInsertTheJob_ToTheBeginningOfTheSucceededList_AndTrimIt()
        {
            var handler = new DeletedStateHandler();
            handler.Apply(_context.Object, _transaction);


            _transaction.Received().InsertToList("deleted", JobId);
            _transaction.Received().TrimList("deleted", 0, 99);
        }

        [Fact]
        public void Unapply_ShouldRemoveTheJob_FromTheSucceededList()
        {
            var handler = new DeletedStateHandler();
            handler.Unapply(_context.Object, _transaction);

            _transaction.Received().RemoveFromList("deleted", JobId);
        }
    }
}
