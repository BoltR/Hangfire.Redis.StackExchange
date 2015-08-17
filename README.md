Hangfire.Redis.StackExchange
==============

A quick port of the old Hangfire.Redis library to use StackExchange.Redis

## Documentation

Basic configuration mimics [stock Hangfire behaviour](http://docs.hangfire.io/en/latest/configuration/using-redis.html). However, it's possible to pass your StackExchange.Redis ConfigurationOptions in directly.

## Configuration

There are many ways to configure Hangfire to use Hangfire.Redis.StackExchange. To get going in a hurry you can add something as simple as this to your OWIN Startup class:

```c#
using Hangfire;
using Hangfire.Redis.StackExchange;

// ...

public void Configuration(IAppBuilder app)
{
    GlobalConfiguration.Configuration.UseRedisStorage("<connection string>");

    app.UseHangfireDashboard();
    app.UseHangfireServer();
}
```

If you already have a StackExchange.Redis `ConfigurationOptions` class that your project uses you are able to substitute it for the connection string:

```c#
GlobalConfiguration.Configuration.UseRedisStorage(ConfigurationOptions);
```

Further options for configuration can be found in the `RedisStorageOptions` class:
```c#
GlobalConfiguration.Configuration.UseRedisStorage("localhost:6379", new RedisStorageOptions()
{
	Db = 0,
	Prefix = "hangfire:"
});
```

You can include extra stats from your Redis server on your dashboard in the following way:
```c#
        GlobalConfiguration.Configuration.UseDashboardMetric(((RedisStorage)JobStorage.Current).GetDashboardInfo("Version", "redis_version"));
```
**Note:** For this to work, you must create your Redis connection with ```allowAdmin=true```. You will recieve an exception otherwise.

A list of avaiable INFO keys can be found at [here](http://redis.io/commands/INFO).



## NuGet

Can be found on NuGet at https://www.nuget.org/packages/HangFire.Redis.SE/

## Changelog

### 1.4.0
* Fixed issues with multiple queues

### 1.2.0
* Added support for changing the hangfire prefix
* Fixed issue where job status was not always being reported to the dashboard

### 1.1.1
* Fix function name in [Obsolete] sections

### 1.1.0
* Updated to support for Hangfire 1.4
* Fixed job state always being null on the dashboard

### 1.0.0
* Initial Version