# aws-drq-consumer v2.0.0-beta.2

An AWS Lambda that will consume unusable, "dead" records from a Dead Record Queue (DRQ) Kinesis stream and save them
to DynamoDB. Alternatively, can be used as a module to configure another more-customised version of a DRQ consumer
Lambda.

## Modules:
- `drq-consumer.js` module
  - An AWS Lambda that will consume unusable, "dead" records from a Dead Record Queue (DRQ) Kinesis stream and save them to DynamoDB

## Purpose

The goal of the AWS Dead Record Queue (DRQ) consumer functions is to robustly consume unusable/dead records from an AWS 
Kinesis DeadRecordQueue stream and save them to a DynamoDB DeadRecord table. 

## Installation
This module is exported as a [Node.js](https://nodejs.org) module.

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
This module's unit tests were developed with and must be run with [tape](https://www.npmjs.com/package/tape). The unit tests have been tested on [Node.js v6.10.3](https://nodejs.org/en/blog/release/v6.10.3).  

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
See [CHANGES.md](CHANGES.md)