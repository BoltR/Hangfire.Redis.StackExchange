using System;
using Hangfire.States;
using NSubstitute;
using System.Linq;

namespace Hangfire.Redis.StackExchange.Tests
{
    public class ApplyStateContextMock
    {
        private readonly Lazy<ApplyStateContext> _context;

        public ApplyStateContextMock()
        {
            StateContextValue = new StateContextMock();
            NewStateValue = Substitute.For<IState>();
            OldStateValue = null;

            _context = new Lazy<ApplyStateContext>(
                () => new ApplyStateContext(
                    StateContextValue.Object,
                    NewStateValue,
                    OldStateValue,
                    Enumerable.Empty<IState>()));
        }

        public StateContextMock StateContextValue { get; set; }
        public IState NewStateValue { get; set; }
        public string OldStateValue { get; set; }

        public ApplyStateContext Object
        {
            get { return _context.Value; }
        }
    }
}
