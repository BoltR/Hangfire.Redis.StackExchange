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

using Hangfire.Dashboard;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;

namespace Hangfire.Redis.StackExchange
{
    public class RedisStorage : JobStorage, IDisposable
    {
        private const string ClientName = "Hangfire";
        private readonly ConnectionMultiplexer ServerPool;
        private const string DefaultHost = "localhost";
        private const string DefaultPort = "6379";

        private static Regex reg = new Regex("^Unspecified/", RegexOptions.Compiled);
        private static readonly string[] SplitString = new string[] { "\r\n" };
        private static Dictionary<string, string> RedisInfo;
        private int LastUpdate;
        private static readonly object Locker = new object();

        private RedisStorageInternals StorageInternals;
        private FetchedJobsWatcherOptions FetchedJobsOptions;
        public int Db { get; private set; }

        public string Prefix
        {
            get
            {
                return StorageInternals.Prefix;
            }
        }

        [Obsolete("Please configure with `RedisStorageOptions` instead. Will be removed in version 2.0.0.")]
        public RedisStorage(string OptionString, int db) : this(ConfigurationOptions.Parse(OptionString), db, RedisStorageOptions.DefaultPrefix) {}
        [Obsolete("Please configure with `RedisStorageOptions` instead. Will be removed in version 2.0.0.")]
        public RedisStorage(string OptionString, int db, string prefix) : this(ConfigurationOptions.Parse(OptionString), db, prefix) {}
        [Obsolete("Please configure with `RedisStorageOptions` instead. Will be removed in version 2.0.0.")]
        public RedisStorage(ConfigurationOptions Options, int db) : this(Options, db, RedisStorageOptions.DefaultPrefix) { }
        [Obsolete("Please configure with `RedisStorageOptions` instead. Will be removed in version 2.0.0.")]
        public RedisStorage(ConfigurationOptions Options, int db, string prefix)
        {
            if (Options == null) throw new ArgumentNullException("Options");

            var HangfireOptions = new RedisStorageOptions()
            {
                Prefix = prefix
            };

            Db = db;
            Options.AbortOnConnectFail = false;
            Options.ClientName = ClientName;
            ServerPool = ConnectionMultiplexer.Connect(Options);

            var Sub = new RedisSubscribe(ServerPool.GetSubscriber(), prefix);
            var LockID = Guid.NewGuid().ToString();
            StorageInternals = new RedisStorageInternals(prefix, LockID, Sub);

            FetchedJobsOptions = new FetchedJobsWatcherOptions(HangfireOptions);
        }

        public RedisStorage() : this(String.Format("{0}:{1}", DefaultHost, DefaultPort)) { }
        public RedisStorage(string OptionString) : this(ConfigurationOptions.Parse(OptionString), new RedisStorageOptions()) { }
        public RedisStorage(string OptionString, RedisStorageOptions HangfireOptions) : this(ConfigurationOptions.Parse(OptionString), HangfireOptions) { }
        public RedisStorage(ConfigurationOptions Options) : this(Options, new RedisStorageOptions()) { }
        public RedisStorage(ConfigurationOptions RedisOptions, RedisStorageOptions HangfireOptions)
        {
            if (RedisOptions == null) throw new ArgumentNullException("RedisOptions");
            if (HangfireOptions == null) throw new ArgumentNullException("HangfireOptions");

            Db = HangfireOptions.Db;
            RedisOptions.AbortOnConnectFail = false;
            RedisOptions.ClientName = ClientName;
            ServerPool = ConnectionMultiplexer.Connect(RedisOptions);

            var Sub = new RedisSubscribe(ServerPool.GetSubscriber(), HangfireOptions.Prefix);
            var LockID = Guid.NewGuid().ToString();
            StorageInternals = new RedisStorageInternals(HangfireOptions.Prefix, LockID, Sub);
            FetchedJobsOptions = new FetchedJobsWatcherOptions(HangfireOptions);


        }

        public override IMonitoringApi GetMonitoringApi()
        {
            return new RedisMonitoringApi(this);
        }

        public IDatabase GetDatabase()
        {
            return ServerPool.GetDatabase(Db);
        }

        
        public DashboardMetric GetDashboardInfo(string title, string key)
        {
            UpdateInfoFromRedis();
            string Value;
            Metric Info;
            if (RedisInfo.TryGetValue(key, out Value))
            {
                Info = new Metric(Value);
            }
            else
            {
                Info = new Metric("Key not found");
                Info.Style = MetricStyle.Danger;
            }
            return new DashboardMetric("redis:" + key, title, (RazorPage) => Info);
        }

        private void UpdateInfoFromRedis()
        {
            if (RedisInfo == null || unchecked(Environment.TickCount - LastUpdate) > 1000)
            {
                lock (Locker)
                {
                    if (RedisInfo == null || unchecked(Environment.TickCount - LastUpdate) > 1000)
                    {
                        RedisInfo = new Dictionary<string, string>();
                        var RawInfo = ServerPool.GetServer(ServerPool.GetDatabase(Db).IdentifyEndpoint()).InfoRaw();
                        foreach (var item in RawInfo.Split(SplitString, StringSplitOptions.RemoveEmptyEntries))
                        {
                            var InfoPair = item.Split(':');
                            if (InfoPair.Length > 1)
                            {
                                RedisInfo.Add(InfoPair[0], InfoPair[1]);
                            }
                        }
                        LastUpdate = Environment.TickCount;
                    }
                }
            }
        }

        internal RedisSubscribe GetSubscribe()
        {
            return StorageInternals.Sub;
        }

        public override IStorageConnection GetConnection()
        {
            return new RedisConnection(ServerPool.GetDatabase(Db), StorageInternals);
        }

        public override IEnumerable<IServerComponent> GetComponents()
        {
            yield return new FetchedJobsWatcher(this, FetchedJobsOptions);
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

        public void Dispose()
        {
            StorageInternals.Dispose();
        }
    }
}