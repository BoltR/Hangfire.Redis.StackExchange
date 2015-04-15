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

using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Hangfire.Redis.StackExchange
{
    public class RedisStorage : JobStorage, IDisposable
    {
        internal static readonly string Prefix = "hangfire:";
        public readonly string StorageLockID = Guid.NewGuid().ToString();
        private const string ClientName = "Hangfire";
        private readonly ConnectionMultiplexer ServerPool;
        private readonly RedisSubscribe Sub;
        private const string DefaultHost = "localhost";
        private const string DefaultPort = "6379";
        private const int DefaultDatabase = 0;

        public TimeSpan InvisibilityTimeout { get; set; }

        public int Db { get; private set; }

        private static Regex reg = new Regex("^Unspecified/", RegexOptions.Compiled);

        public RedisStorage() : this(String.Format("{0}:{1}", DefaultHost, DefaultPort))
        {}

        public RedisStorage(string OptionString) : this(OptionString, DefaultDatabase)
        {}

        public RedisStorage(string OptionString, int db) : this(ConfigurationOptions.Parse(OptionString), db)
        {}

        public RedisStorage(ConfigurationOptions Options) : this(Options, DefaultDatabase)
        {}

        public RedisStorage(ConfigurationOptions Options, int db)
        {
            if (Options == null) throw new ArgumentNullException("Options");
            Db = db;
            InvisibilityTimeout = TimeSpan.FromMinutes(30);
            Options.AbortOnConnectFail = false;
            Options.ClientName = ClientName;
            ServerPool = ConnectionMultiplexer.Connect(Options);
            Sub = new RedisSubscribe(ServerPool.GetSubscriber());
        }

        public override IMonitoringApi GetMonitoringApi()
        {
            return new RedisMonitoringApi(this);
        }

        public IDatabase GetDatabase()
        {
            return ServerPool.GetDatabase(Db);
        }

        public RedisSubscribe GetSubscribe()
        {
            return Sub;
        }

        public override IStorageConnection GetConnection()
        {
            return new RedisConnection(ServerPool.GetDatabase(Db), Sub, StorageLockID);
        }

        public override IEnumerable<IServerComponent> GetComponents()
        {
            yield return new FetchedJobsWatcher(this, InvisibilityTimeout);
        }

        public override IEnumerable<IStateHandler> GetStateHandlers()
        {
            yield return new FailedStateHandler();
            yield return new ProcessingStateHandler();
            yield return new SucceededStateHandler();
            yield return new DeletedStateHandler();
        }

        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for Redis job storage:" + ServerPool.Configuration.ToString());
        }

        public override string ToString()
        {
            return String.Format("redis://{0}/{1}", String.Join(",", ServerPool.GetEndPoints().Select(x => reg.Replace(x.ToString(), String.Empty))), Db);
        }

        internal static string GetRedisKey(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            return Prefix + key;
        }

        public void Dispose()
        {
            Sub.Dispose();
        }
    }
}