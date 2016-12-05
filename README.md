# aws-drq-consumer v1.0.0-beta.2

An AWS Lambda that will consume unusable, "dead" records from a Dead Record Queue (DRQ) Kinesis stream and save them
to DynamoDB. Alternatively, can be used as a module to configure another more-customised version of a DRQ consumer
Lambda.

## Modules:
- `drq-consumer.js` module
  - An AWS Lambda that will consume unusable, "dead" records from a Dead Record Queue (DRQ) Kinesis stream and save them to DynamoDB

## Dependencies:
- `aws-stream-consumer`
- `task-utils`
- `aws-core-utils`
- `logging-utils`
- `core-functions`

## Purpose

The goal of the AWS Dead Record Queue (DRQ) consumer functions is to robustly consume unusable/dead records from an AWS 
Kinesis DeadRecordQueue stream and save them to a DynamoDB DeadRecord table. 

## Installation
This module is exported as a [Node.js](https://nodejs.org/) module.

Using npm:
```bash
$ {sudo -H} npm i -g npm
$ npm i --save aws-drq-consumer
```

## Usage 

* Install it as an AWS Lambda function with its event source set to your DeadRecordQueue Kinesis stream.

* Alternatively, use the `aws-drq-consumer` module as a source of functions to be possibly re-used in your own custom 
  DRQ consumer implementation.

## Unit tests
This module's unit tests were developed with and must be run with [tape](https://www.npmjs.com/package/tape). The unit tests have been tested on [Node.js v4.3.2](https://nodejs.org/en/blog/release/v4.3.2/).  

Install tape globally if you want to run multiple tests at once:
```bash
$ npm install tape -g
```

Run all unit tests with:
```bash
$ npm test
```
or with tape:
```bash
$ tape test/*.js
```

See the [package source](https://github.com/byron-dupreez/aws-drq-consumer) for more details.

## Changes

### 1.0.0-beta.2
- Major refactoring to synchronize with changes to `aws-stream-consumer` and other dependencies
- Major refactoring & clean-up of DRQ stream consumer configuration API and added appropriate 
  typedefs for configuration options & settings
- Updated `core-functions` dependency to version 2.0.10
- Updated `logging-utils` dependency to version 3.0.5
- Updated `aws-core-utils` dependency to version 5.0.5
- Updated `task-utils` dependency to version 4.0.3
- Updated `aws-stream-consumer` dependency to version 1.0.0-beta.11
- Updated `tape` dependency to 4.6.3

### 1.0.0-beta.1
- Initial commit
