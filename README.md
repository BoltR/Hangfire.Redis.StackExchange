Hangfire.Redis.StackExchange
==============

A quick port of the old Hangfire.Redis library to use StackExchange.Redis

## Documentation

Basic configuration mimics [stock Hangfire behaviour](http://docs.hangfire.io/en/latest/configuration/using-redis.html). However, it's possible to pass your StackExchange.Redis ConfigurationOptions in directly.

## NuGet

Can be found on NuGet at https://www.nuget.org/packages/HangFire.Redis.SE/

## Using Redis

To install Hangfire into your **ASP.NET application** with **Redis** storage, type the following command into the Package Manager Console Window:

```powershell
PM> Install-Package Hangfire.Redis.SE
```

Hangfire with Redis job storage processes jobs much faster than with SQL Server storage.

Running the Redis server on a Windows platform means using the Microsoft Open Technology fork available on their [GitHub page](https://github.com/MSOpenTech/Redis).

## Configuration

After installing the package, add or update the OWIN Startup class with the following lines:

```c#
using Hangfire;
using Hangfire.Redis.StackExchange;

// ...

public void Configuration(IAppBuilder app)
{
    GlobalConfiguration.Configuration.UseRedisStorage("<connection string or its name>");

    app.UseHangfireDashboard();
    app.UseHangfireServer();
}
```

The Hangfire.Redis.StackExchange package contains some extension methods for the `GlobalConfiguration` class:

```C#
GlobalConfiguration.Configuration
    // Use hostname only and default port 6379
    .UseRedisStorage("localhost");
    // or add a port
    .UseRedisStorage("localhost:6379");
    // or add a db number
    .UseRedisStorage("localhost:6379", 0);
    // or use a password
    .UseRedisStorage("password@localhost:6379", 0);

// or with options
var options = StackExchange.Redis.ConfigurationOptions.Parse("localhost");
GlobalConfiguration.Configuration.UseRedisStorage(options);
```

## Using key prefixes

If you are using a shared Redis server for multiple environments, you can specify unique prefix for each environment:

```c#
GlobalConfiguration.Configuration
    .UseRedisStorage("localhost:6379", 0, "hangfire:");
```

## Changelog

###1.2.0
* Added support for changing the hangfire prefix
* Fixed issue where job status was not always being reported to the dashboard

###1.1.1
* Fix function name in [Obsolete] sections

###1.1.0
* Updated to support for Hangfire 1.4
* Fixed job state always being null on the dashboard

###1.0.0
* Initial Version
