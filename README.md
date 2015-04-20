Hangfire.Redis.StackExchange
==============

A quick port of the old Hangfire.Redis library to use StackExchange.Redis

## Documentation

Basic configuration mimics [stock Hangfire behaviour](http://docs.hangfire.io/en/latest/configuration/using-redis.html). However, it's possible to pass your StackExchange.Redis ConfigurationOptions in directly.


## NuGet

Can be found on NuGet at https://www.nuget.org/packages/HangFire.Redis.SE/

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