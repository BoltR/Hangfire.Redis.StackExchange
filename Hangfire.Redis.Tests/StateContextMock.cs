﻿using System;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Moq;

namespace Hangfire.Redis.Tests
{
    public class StateContextMock
    {
        private readonly Lazy<StateContext> _context;

        public StateContextMock()
        {
            JobIdValue = "job-id";
            JobValue = Job.FromExpression(() => Console.WriteLine());
            CreatedAtValue = DateTime.UtcNow;
            ConnectionValue = new Mock<IStorageConnection>();

            _context = new Lazy<StateContext>(
                () => new StateContext(JobIdValue, JobValue, CreatedAtValue, ConnectionValue.Object));
        }

        public string JobIdValue { get; set; }
        public Job JobValue { get; set; }
        public DateTime CreatedAtValue { get; set; }

        public Mock<IStorageConnection> ConnectionValue { get; set; }

        public StateContext Object
        {
            get { return _context.Value; }
        }
    }
}
