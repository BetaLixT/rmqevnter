# rmqevnter
Package to fire and forget events

## Description
Library to fire and forget events, built for an event based microservices
environment

This is the second iteration of the more generic
[evnter](https://github.com/BetaLixT/evnter) project, this is more specifically
a RabbitMQ event publisher. This iteration has lessser moving parts since it's
less generic and is hence more robust and faster

## Installation
1. Install module
```bash
go get github.com/BetaLixT/rmqevnter
```
2. Import
```go
import "github.com/BetaLixT/rmqevnter"
```

## Usage

