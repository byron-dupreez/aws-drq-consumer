'use strict';

/**
 * Unit tests for aws-stream-consumer/stream-consumer.js
 * @author Byron du Preez
 */

const test = require("tape");

// The test subject
const drqConsumer = require('../drq-consumer');

function noop() {
}

const streamConsumer = require('aws-stream-consumer/stream-consumer');
const streamProcessing = require('aws-stream-consumer/stream-processing');

const taskUtils = require('task-utils');
const TaskDef = taskUtils.TaskDef;
const Task = taskUtils.Task;
const taskStates = taskUtils;

const regions = require("aws-core-utils/regions");
const stages = require("aws-core-utils/stages");
const kinesisCache = require("aws-core-utils/kinesis-cache");
const dynamoDBDocClientCache = require("aws-core-utils/dynamodb-doc-client-cache");

require("core-functions/promises");

const strings = require("core-functions/strings");
const stringify = strings.stringify;

const base64 = require("core-functions/base64");

const logging = require("logging-utils");

const samples = require("./samples");

function setRegionStageAndDeleteCachedInstances(region, stage) {
  // Set up region
  process.env.AWS_REGION = region;
  // Set up stage
  process.env.STAGE = stage;
  // Remove any cached entries before configuring
  deleteCachedInstances();
  return region;
}

function deleteCachedInstances() {
  const region = regions.getRegion();
  kinesisCache.deleteKinesis(region);
  dynamoDBDocClientCache.deleteDynamoDBDocClient(region);
}

function sampleKinesisEvent(streamName, partitionKey, data, omitEventSourceARN) {
  const region = process.env.AWS_REGION;
  const eventSourceArn = omitEventSourceARN ? undefined : samples.sampleKinesisEventSourceArn(region, streamName);
  return samples.sampleKinesisEventWithSampleRecord(partitionKey, data, eventSourceArn, region);
}

function sampleAwsContext(functionVersion, functionAlias, maxTimeInMillis) {
  const region = process.env.AWS_REGION;
  const functionName = 'sampleFunctionName';
  const invokedFunctionArn = samples.sampleInvokedFunctionArn(region, functionName, functionAlias);
  return samples.sampleAwsContext(functionName, functionVersion, invokedFunctionArn, maxTimeInMillis);
}

function configureKinesisAndDynamoDB(t, context, kinesisError, dynamoDBError, dynamoDBDelayMs) {
  context.kinesis = dummyKinesis(t, 'DRQ consumer', kinesisError);
  context.dynamoDBDocClient = dummyDynamoDBDocClient(t, 'DRQ consumer', dynamoDBError, dynamoDBDelayMs);
}

function configureDRQConsumer(context, event, awsContext) {
  drqConsumer.configureDefaultDeadRecordQueueConsumer(context, undefined, event, awsContext);
}

function dummyKinesis(t, prefix, error) {
  return {
    putRecord(request) {
      return {
        promise() {
          return new Promise((resolve, reject) => {
            t.pass(`${prefix} simulated putRecord to Kinesis with request (${stringify(request)})`);
            if (error)
              reject(error);
            else
              resolve({});
          })
        }
      }
    }
  };
}

function dummyDynamoDBDocClient(t, prefix, error, delayMs) {
  const ms = delayMs ? delayMs : 1;
  return {
    put(request) {
      return {
        promise() {
          return Promise.delay(ms).then(() => {
            return new Promise((resolve, reject) => {
              t.pass(`${prefix} simulated put to DynamoDB.DocumentClient with request (${stringify(request)})`);
              if (error)
                reject(error);
              else
                resolve({});
            });
          });
        }
      };
    }
  };
}

let messageNumber = 0;

function sampleUnusableRecord(i) {
  ++messageNumber;
  const region = process.env.AWS_REGION;
  // Create plain string data, which cannot be extracted into an object
  const badData = base64.toBase64FromUtf8(`BadData (${i}) (${messageNumber})`);
  const origStreamName = 'TestStream_DEV2';
  const eventSourceArn = samples.sampleKinesisEventSourceArn(region, origStreamName);
  return samples.sampleKinesisRecord(undefined, badData, eventSourceArn, region);
}

// =====================================================================================================================
// consumeDeadRecords
// =====================================================================================================================

function checkMessagesTasksStates(t, messages, oneStateType, allStateType, context) {
  for (let i = 0; i < messages.length; ++i) {
    checkMessageTasksStates(t, messages[i], oneStateType, allStateType, context)
  }
}

function checkMessageTasksStates(t, message, oneStateType, allStateType, context) {
  if (oneStateType) {
    const oneTasksByName = streamConsumer.getProcessOneTasksByName(message, context);
    const ones = taskUtils.getTasksAndSubTasks(oneTasksByName);
    t.ok(ones.every(t => t.state instanceof oneStateType), `message ${message.id} every process one task state must be instance of ${oneStateType.name}`);
  }
  if (allStateType) {
    const allTasksByName = streamConsumer.getProcessAllTasksByName(message, context);
    const alls = taskUtils.getTasksAndSubTasks(allTasksByName);
    t.ok(alls.every(t => t.state instanceof allStateType), `message ${message.id} every process all task state must be instance of ${allStateType.name}`);
  }
}

// =====================================================================================================================
// consumeDeadRecords with successful message(s)
// =====================================================================================================================

test('consumeDeadRecords with 1 message that succeeds all tasks', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2');

    const context = {};

    const n = 1;

    // Generate a sample AWS event
    const streamName = 'DeadRecordQueue_DEV2';
    const event = sampleKinesisEvent(streamName, undefined, sampleUnusableRecord(1), false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    configureKinesisAndDynamoDB(t, context, undefined, undefined);

    // Process the event
    try {
      configureDRQConsumer(context, event, awsContext);
      console.log(`######################## context = ${stringify(context)}`);
      const promise = drqConsumer.consumeDeadRecords(event, context);

      if (Promise.isPromise(promise)) {
        t.pass(`consumeDeadRecords returned a promise`);
      } else {
        t.fail(`consumeDeadRecords should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);
      t.equal(context.streamProcessing.deadRecordTableName, 'DeadRecord', `context.streamProcessing.deadRecordTableName must be DeadRecord`);

      promise
        .then(results => {
          t.pass(`consumeDeadRecords must resolve`);
          const messages = results.messages;
          t.equal(messages.length, n, `consumeDeadRecords results must have ${n} messages`);
          checkMessagesTasksStates(t, messages, taskStates.CompletedState, taskStates.CompletedState, context);
          t.equal(messages[0].drqTaskTracking.ones.saveDeadRecord.attempts, 1, `saveDeadRecord attempts must be 1`);

          t.ok(results.processing.completed, `consumeDeadRecords processing must be completed`);
          t.notOk(results.processing.failed, `consumeDeadRecords processing must not be failed`);
          t.notOk(results.processing.timedOut, `consumeDeadRecords processing must not be timed-out`);

          t.equal(results.unusableRecords.length, 0, `consumeDeadRecords results must have ${0} unusable records`);
          t.equal(results.discardedUnusableRecords.length, 0, `consumeDeadRecords results must have ${0} discarded unusable records`);
          t.equal(results.handledIncompleteMessages.length, 0, `consumeDeadRecords results must have ${0} handled incomplete records`);
          t.equal(results.discardedRejectedMessages.length, 0, `consumeDeadRecords results must have ${0} discarded rejected messages`);

          t.end();
        })
        .catch(err => {
          t.fail(`consumeDeadRecords should NOT have failed (${stringify(err)})`, err.stack);
          t.end(err);
        });

    } catch (err) {
      t.fail(`consumeDeadRecords should NOT have failed (${err})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});


test('consumeDeadRecords with 1 message that succeeds all tasks (despite broken Kinesis, i.e. no unusable/rejected/incomplete)', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2');

    const context = {};

    const n = 1;

    // Generate a sample AWS event
    const streamName = 'DeadRecordQueue_DEV2';
    const event = sampleKinesisEvent(streamName, undefined, sampleUnusableRecord(1), false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    configureKinesisAndDynamoDB(t, context, new Error('Disabling Kinesis'), undefined);

    // Process the event
    try {
      configureDRQConsumer(context, event, awsContext);
      const promise = drqConsumer.consumeDeadRecords(event, context);

      if (Promise.isPromise(promise)) {
        t.pass(`consumeDeadRecords returned a promise`);
      } else {
        t.fail(`consumeDeadRecords should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, 'context.awsContext must be given awsContext');

      promise
        .then(results => {
          t.pass(`consumeDeadRecords must resolve`);
          const messages = results.messages;
          t.equal(messages.length, n, `consumeDeadRecords results must have ${n} messages`);
          checkMessagesTasksStates(t, messages, taskStates.CompletedState, taskStates.CompletedState, context);
          t.equal(messages[0].drqTaskTracking.ones.saveDeadRecord.attempts, 1, `saveDeadRecord attempts must be 1`);

          t.ok(results.processing.completed, `consumeDeadRecords processing must be completed`);
          t.notOk(results.processing.failed, `consumeDeadRecords processing must not be failed`);
          t.notOk(results.processing.timedOut, `consumeDeadRecords processing must not be timed-out`);

          t.equal(results.unusableRecords.length, 0, `consumeDeadRecords results must have ${0} unusable records`);
          t.equal(results.discardedUnusableRecords.length, 0, `consumeDeadRecords results must have ${0} discarded unusable records`);
          t.equal(results.handledIncompleteMessages.length, 0, `consumeDeadRecords results must have ${0} handled incomplete records`);
          t.equal(results.discardedRejectedMessages.length, 0, `consumeDeadRecords results must have ${0} discarded rejected messages`);

          t.end();
        })

    } catch (err) {
      t.fail(`consumeDeadRecords should NOT have failed (${err})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('consumeDeadRecords with 10 messages that succeed all tasks (despite broken Kinesis, i.e. no unusable/rejected/incomplete)', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2');

    const context = {};

    const n = 10;

    // Generate a sample AWS event
    const streamName = 'DeadRecordQueue_DEV2';
    const records = [];
    for (let i = 0; i < n; ++i) {
      const eventSourceArn = samples.sampleKinesisEventSourceArn(region, streamName);
      const record = samples.sampleKinesisRecord(undefined, sampleUnusableRecord(i + 1), eventSourceArn, region);
      records.push(record);
    }
    const event = samples.sampleKinesisEventWithRecords(records);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    //configureDefaults(t, context, undefined);
    configureKinesisAndDynamoDB(t, context, new Error('Disabling Kinesis'), undefined);

    // Process the event
    try {
      configureDRQConsumer(context, event, awsContext);
      const promise = drqConsumer.consumeDeadRecords(event, context);

      if (Promise.isPromise(promise)) {
        t.pass(`consumeDeadRecords returned a promise`);
      } else {
        t.fail(`consumeDeadRecords should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, 'context.awsContext must be given awsContext');

      promise
        .then(results => {
          t.pass(`consumeDeadRecords must resolve`);
          const messages = results.messages;
          t.equal(messages.length, n, `consumeDeadRecords results must have ${n} messages`);
          checkMessagesTasksStates(t, messages, taskStates.CompletedState, taskStates.CompletedState, context);
          for (let i = 0; i < messages.length; ++i) {
            t.equal(messages[i].drqTaskTracking.ones.saveDeadRecord.attempts, 1, `saveDeadRecord attempts must be 1`);
          }
          t.ok(results.processing.completed, `consumeDeadRecords processing must be completed`);
          t.notOk(results.processing.failed, `consumeDeadRecords processing must not be failed`);
          t.notOk(results.processing.timedOut, `consumeDeadRecords processing must not be timed-out`);

          t.equal(results.unusableRecords.length, 0, `consumeDeadRecords results must have ${0} unusable records`);
          t.equal(results.discardedUnusableRecords.length, 0, `consumeDeadRecords results must have ${0} discarded unusable records`);
          t.equal(results.handledIncompleteMessages.length, 0, `consumeDeadRecords results must have ${0} handled incomplete records`);
          t.equal(results.discardedRejectedMessages.length, 0, `consumeDeadRecords results must have ${0} discarded rejected messages`);

          t.end();
        })
        .catch(err => {
          t.fail(`consumeDeadRecords should NOT have failed (${stringify(err)})`, err.stack);
          t.end(err);
        });

    } catch (err) {
      t.fail(`consumeDeadRecords should NOT have failed (${err})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// consumeDeadRecords with unusable record(s)
// =====================================================================================================================

test('consumeDeadRecords with 1 unusable record', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2');

    const context = {};

    // Generate a sample AWS event
    const streamName = 'DeadRecordQueue_DEV2';
    const event = sampleKinesisEvent(streamName, undefined, undefined, false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    configureKinesisAndDynamoDB(t, context, undefined);

    // Process the event
    try {
      configureDRQConsumer(context, event, awsContext);
      const promise = drqConsumer.consumeDeadRecords(event, context);

      if (Promise.isPromise(promise)) {
        t.pass(`consumeDeadRecords returned a promise`);
      } else {
        t.fail(`consumeDeadRecords should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, 'context.awsContext must be given awsContext');

      promise
        .then(results => {
          const n = 0;
          t.pass(`consumeDeadRecords must resolve`);
          const messages = results.messages;
          t.equal(messages.length, n, `consumeDeadRecords results must have ${n} messages`);

          t.ok(results.processing.completed, `consumeDeadRecords processing must be completed`);
          t.notOk(results.processing.failed, `consumeDeadRecords processing must not be failed`);
          t.notOk(results.processing.timedOut, `consumeDeadRecords processing must not be timed-out`);

          t.equal(results.unusableRecords.length, 1, `consumeDeadRecords results must have ${1} unusable records`);
          t.equal(results.discardedUnusableRecords.length, 1, `consumeDeadRecords results must have ${1} discarded unusable records`);
          t.equal(results.handledIncompleteMessages.length, 0, `consumeDeadRecords results must have ${0} handled incomplete records`);
          t.equal(results.discardedRejectedMessages.length, 0, `consumeDeadRecords results must have ${0} discarded rejected messages`);

          t.end();
        })
        .catch(err => {
          t.fail(`consumeDeadRecords should NOT have failed (${stringify(err)})`, err.stack);
          t.end(err);
        });

    } catch (err) {
      t.fail(`consumeDeadRecords should NOT have failed (${err})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// consumeDeadRecords with failing saveDeadRecord task on message(s)
// =====================================================================================================================

test('consumeDeadRecords with 1 message that fails its saveDeadRecord task, resubmits', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2');

    const context = {};

    // Generate a sample AWS event
    const streamName = 'DeadRecordQueue_DEV2';
    const event = sampleKinesisEvent(streamName, undefined, sampleUnusableRecord(1), false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    configureKinesisAndDynamoDB(t, context, undefined, new Error(`Planned DynamoDB failure`));

    // Process the event
    try {
      configureDRQConsumer(context, event, awsContext);
      const promise = drqConsumer.consumeDeadRecords(event, context);

      if (Promise.isPromise(promise)) {
        t.pass(`consumeDeadRecords returned a promise`);
      } else {
        t.fail(`consumeDeadRecords should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, 'context.awsContext must be given awsContext');

      promise
        .then(results => {
          t.pass(`consumeDeadRecords must resolve`);
          const n = 1;
          const messages = results.messages;
          t.equal(messages.length, n, `consumeDeadRecords results must have ${n} messages`);
          checkMessagesTasksStates(t, messages, taskStates.Failed, taskStates.CompletedState, context);
          t.equal(messages[0].drqTaskTracking.ones.saveDeadRecord.attempts, 1, `saveDeadRecord attempts must be 1`);

          t.ok(results.processing.completed, `consumeDeadRecords processing must be completed`);
          t.notOk(results.processing.failed, `consumeDeadRecords processing must not be failed`);
          t.notOk(results.processing.timedOut, `consumeDeadRecords processing must not be timed-out`);

          t.equal(results.unusableRecords.length, 0, `consumeDeadRecords results must have ${0} unusable records`);
          t.equal(results.discardedUnusableRecords.length, 0, `consumeDeadRecords results must have ${0} discarded unusable records`);
          t.equal(results.handledIncompleteMessages.length, 1, `consumeDeadRecords results must have ${1} handled incomplete records`);
          t.equal(results.discardedRejectedMessages.length, 0, `consumeDeadRecords results must have ${0} discarded rejected messages`);

          t.end();
        })
        .catch(err => {
          t.fail(`consumeDeadRecords should NOT have failed (${stringify(err)})`, err.stack);
          t.end(err);
        });

    } catch (err) {
      t.fail(`consumeDeadRecords should NOT have failed (${err})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('consumeDeadRecords with 1 message that fails its processOne task, but cannot resubmit must fail', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2');

    const context = {};

    // Generate a sample AWS event
    const streamName = 'DeadRecordQueue_DEV2';
    const event = sampleKinesisEvent(streamName, undefined, sampleUnusableRecord(1), false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    const fatalError = new Error('Disabling Kinesis');
    configureKinesisAndDynamoDB(t, context, fatalError, new Error(`Planned DynamoDB failure`));

    // Process the event
    try {
      configureDRQConsumer(context, event, awsContext);
      const promise = drqConsumer.consumeDeadRecords(event, context);

      if (Promise.isPromise(promise)) {
        t.pass(`consumeDeadRecords returned a promise`);
      } else {
        t.fail(`consumeDeadRecords should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, 'context.awsContext must be given awsContext');

      promise
        .then(messages => {
          const n = messages.length;
          t.fail(`consumeDeadRecords must NOT resolve with ${n} message(s)`);
          t.end();
        })
        .catch(err => {
          t.pass(`consumeDeadRecords must reject with error (${stringify(err)})`);
          t.equal(err, fatalError, `consumeDeadRecords error must be ${fatalError}`);

          streamConsumer.awaitStreamConsumerResults(err.streamConsumerResults).then(results => {
            const messages = results.messages;

            t.equal(messages.length, 1, `consumeDeadRecords results must have ${1} messages`);
            t.equal(results.unusableRecords.length, 0, `consumeDeadRecords results must have ${0} unusable records`);

            t.ok(results.processing.completed, `consumeDeadRecords processing must be completed`);
            t.notOk(results.processing.failed, `consumeDeadRecords processing must not be failed`);
            t.notOk(results.processing.timedOut, `consumeDeadRecords processing must not be timed-out`);

            if (!results.discardedUnusableRecords || results.discardUnusableRecordsError) {
              t.fail(`discardUnusableRecords must not fail with ${results.discardUnusableRecordsError}`);
            }
            if (results.discardedUnusableRecords) {
              t.equal(results.discardedUnusableRecords.length, 0, `discardUnusableRecords must have ${0} discarded unusable records`);
            }

            if (results.handledIncompleteMessages || !results.handleIncompleteMessagesError) {
              t.fail(`handleIncompleteMessages must fail with ${results.handleIncompleteMessagesError}`);
            }
            t.equal(results.handleIncompleteMessagesError, fatalError, `handleIncompleteMessages must fail with ${fatalError}`);

            if (!results.discardedRejectedMessages || results.discardRejectedMessagesError) {
              t.fail(`discardRejectedMessages must not fail with ${results.discardRejectedMessagesError}`);
            }
            if (results.discardedRejectedMessages) {
              t.equal(results.discardedRejectedMessages.length, 0, `discardedRejectedMessages must have ${0} discarded rejected messages`);
            }

            t.end();
          });
        });

    } catch (err) {
      t.fail(`consumeDeadRecords should NOT have failed (${err})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// consumeDeadRecords with successful message(s) with inactive tasks, must discard abandoned message(s)
// =====================================================================================================================

test('consumeDeadRecords with 1 message that succeeds, but has 1 abandoned task - must discard rejected message', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2');

    const context = {};

    // Generate a sample AWS event
    const streamName = 'DeadRecordQueue_DEV2';
    const msg = sampleUnusableRecord(1);

    // Add some "history" to this message to give it a no-longer active task that will trigger abandonment of this task
    const taskX = Task.createTask(TaskDef.defineTask('TaskX', noop));
    taskX.fail(new Error('Previously failed'));
    for (let i = 0; i < 100; ++i) {
      taskX.incrementAttempts();
    }
    const taskXLike = JSON.parse(JSON.stringify(taskX));

    msg.drqTaskTracking = {alls: {'TaskX': taskXLike}};
    const m = JSON.parse(JSON.stringify(msg));
    t.ok(m, 'Message with tasks is parsable');
    const taskXRevived = m.drqTaskTracking.alls.TaskX;
    t.ok(Task.isTaskLike(taskXRevived), `TaskX must be task-like (${stringify(taskXRevived)})`);
    t.deepEqual(taskXRevived, taskXLike, `TaskX revived must be original TaskX task-like (${stringify(taskXRevived)})`);

    const event = sampleKinesisEvent(streamName, undefined, msg, false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    configureKinesisAndDynamoDB(t, context, undefined, undefined);

    // Process the event
    try {
      configureDRQConsumer(context, event, awsContext);
      const promise = drqConsumer.consumeDeadRecords(event, context);

      if (Promise.isPromise(promise)) {
        t.pass(`consumeDeadRecords returned a promise`);
      } else {
        t.fail(`consumeDeadRecords should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, 'context.awsContext must be given awsContext');

      promise
        .then(results => {
          t.pass(`consumeDeadRecords must resolve`);
          const n = 1;
          const messages = results.messages;
          t.equal(messages.length, n, `consumeDeadRecords results must have ${n} messages`);
          checkMessagesTasksStates(t, messages, taskStates.CompletedState, taskStates.Abandoned, context);
          t.equal(messages[0].drqTaskTracking.ones.saveDeadRecord.attempts, 1, `saveDeadRecord attempts must be 1`);
          t.equal(messages[0].drqTaskTracking.alls.TaskX.attempts, 100, `TaskX attempts must be 100`);

          t.ok(results.processing.completed, `consumeDeadRecords processing must be completed`);
          t.notOk(results.processing.failed, `consumeDeadRecords processing must not be failed`);
          t.notOk(results.processing.timedOut, `consumeDeadRecords processing must not be timed-out`);

          t.equal(results.unusableRecords.length, 0, `consumeDeadRecords results must have ${0} unusable records`);
          t.equal(results.discardedUnusableRecords.length, 0, `consumeDeadRecords results must have ${0} discarded unusable records`);
          t.equal(results.handledIncompleteMessages.length, 0, `consumeDeadRecords results must have ${0} handled incomplete records`);
          t.equal(results.discardedRejectedMessages.length, 1, `consumeDeadRecords results must have ${1} discarded rejected messages`);

          t.end();
        })
        .catch(err => {
          t.fail(`consumeDeadRecords should NOT have failed (${stringify(err)})`, err.stack);
          t.end(err);
        });

    } catch (err) {
      t.fail(`consumeDeadRecords should NOT have failed (${err})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('consumeDeadRecords with 1 message that succeeds, but has 1 abandoned task - must fail if cannot discard rejected message', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2');

    const context = {};

    // Generate a sample AWS event
    const streamName = 'DeadRecordQueue_DEV2';
    const msg = sampleUnusableRecord(1);

    // Add some "history" to this message to give it a no-longer active task that will trigger abandonment of this task
    const taskX = Task.createTask(TaskDef.defineTask('TaskX', noop));
    taskX.fail(new Error('Previously failed'));
    msg.drqTaskTracking = {alls: {'TaskX': JSON.parse(JSON.stringify(taskX))}};

    const event = sampleKinesisEvent(streamName, undefined, msg, false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    const fatalError = new Error('Disabling Kinesis');
    configureKinesisAndDynamoDB(t, context, fatalError, undefined);

    // Process the event
    try {
      configureDRQConsumer(context, event, awsContext);
      const promise = drqConsumer.consumeDeadRecords(event, context);

      if (Promise.isPromise(promise)) {
        t.pass(`consumeDeadRecords returned a promise`);
      } else {
        t.fail(`consumeDeadRecords should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, 'context.awsContext must be given awsContext');

      promise
        .then(messages => {
          const n = messages.length;
          t.fail(`consumeDeadRecords must NOT resolve with ${n} message(s)`);
          t.end();
        })
        .catch(err => {
          t.pass(`consumeDeadRecords must reject with error (${stringify(err)})`);
          t.equal(err, fatalError, `consumeDeadRecords error must be ${fatalError}`);

          streamConsumer.awaitStreamConsumerResults(err.streamConsumerResults).then(results => {
            const messages = results.messages;
            t.equal(messages.length, 1, `consumeDeadRecords results must have ${1} messages`);
            t.equal(results.unusableRecords.length, 0, `consumeDeadRecords results must have ${0} unusable records`);

            t.ok(results.processing.completed, `consumeDeadRecords processing must be completed`);
            t.notOk(results.processing.failed, `consumeDeadRecords processing must not be failed`);
            t.notOk(results.processing.timedOut, `consumeDeadRecords processing must not be timed-out`);

            if (!results.discardedUnusableRecords || results.discardUnusableRecordsError) {
              t.fail(`discardUnusableRecords must not fail with ${results.discardUnusableRecordsError}`);
            }
            if (results.discardedUnusableRecords) {
              t.equal(results.discardedUnusableRecords.length, 0, `discardUnusableRecords must have ${0} discarded unusable records`);
            }

            if (!results.handledIncompleteMessages || results.handleIncompleteMessagesError) {
              t.fail(`handleIncompleteMessages must not fail with ${results.handleIncompleteMessagesError}`);
            }
            if (results.handledIncompleteMessages) {
              t.equal(results.handledIncompleteMessages.length, 0, `handleIncompleteMessages must have ${0} handled incomplete messages`);
            }

            if (results.discardedRejectedMessages || !results.discardRejectedMessagesError) {
              t.fail(`discardRejectedMessages must fail with ${results.discardRejectedMessagesError}`);
            }
            t.equal(results.discardRejectedMessagesError, fatalError, `discardedRejectedMessages must fail with ${fatalError}`);

            t.end();
          });
        });
    } catch (err) {
      t.fail(`consumeDeadRecords should NOT have failed (${err})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// consumeDeadRecords with 1 message(s) with previously rejected task(s), must discard rejected message(s)
// =====================================================================================================================

test('consumeDeadRecords with 1 message that succeeds, but has 1 old rejected task - must discard rejected message', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2');

    const context = {};

    // Generate a sample AWS event
    const streamName = 'DeadRecordQueue_DEV2';
    const msg = sampleUnusableRecord(1);

    // Add some "history" to this message to give it a no-longer active task that will trigger abandonment of this task
    const taskX = Task.createTask(TaskDef.defineTask('TaskX', noop));
    for (let i = 0; i < 99; ++i) {
      taskX.incrementAttempts();
    }
    taskX.reject('Rejected deliberately', new Error('Previously rejected'));
    const taskXLike = JSON.parse(JSON.stringify(taskX));

    msg.drqTaskTracking = {alls: {'TaskX': taskXLike}};
    const m = JSON.parse(JSON.stringify(msg));
    t.ok(m, 'Message with tasks is parsable');
    const taskXRevived = m.drqTaskTracking.alls.TaskX;
    t.ok(Task.isTaskLike(taskXRevived), `TaskX must be task-like (${stringify(taskXRevived)})`);
    t.deepEqual(taskXRevived, taskXLike, `TaskX revived must be original TaskX task-like (${stringify(taskXRevived)})`);

    const event = sampleKinesisEvent(streamName, undefined, msg, false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    configureKinesisAndDynamoDB(t, context, undefined, undefined);

    // Process the event
    try {
      configureDRQConsumer(context, event, awsContext);
      const promise = drqConsumer.consumeDeadRecords(event, context);

      if (Promise.isPromise(promise)) {
        t.pass(`consumeDeadRecords returned a promise`);
      } else {
        t.fail(`consumeDeadRecords should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, 'context.awsContext must be given awsContext');

      promise
        .then(results => {
          t.pass(`consumeDeadRecords must resolve`);
          const n = 1;
          const messages = results.messages;
          t.equal(messages.length, n, `consumeDeadRecords results must have ${n} messages`);
          checkMessagesTasksStates(t, messages, taskStates.CompletedState, taskStates.Rejected, context);
          t.equal(messages[0].drqTaskTracking.ones.saveDeadRecord.attempts, 1, `saveDeadRecord attempts must be 1`);
          t.equal(messages[0].drqTaskTracking.alls.TaskX.attempts, 99, `TaskX attempts must be 99`);

          t.ok(results.processing.completed, `consumeDeadRecords processing must be completed`);
          t.notOk(results.processing.failed, `consumeDeadRecords processing must not be failed`);
          t.notOk(results.processing.timedOut, `consumeDeadRecords processing must not be timed-out`);

          t.equal(results.unusableRecords.length, 0, `consumeDeadRecords results must have ${0} unusable records`);
          t.equal(results.discardedUnusableRecords.length, 0, `consumeDeadRecords results must have ${0} discarded unusable records`);
          t.equal(results.handledIncompleteMessages.length, 0, `consumeDeadRecords results must have ${0} handled incomplete records`);
          t.equal(results.discardedRejectedMessages.length, 1, `consumeDeadRecords results must have ${1} discarded rejected messages`);

          t.end();
        })
        .catch(err => {
          t.fail(`consumeDeadRecords should NOT have failed (${stringify(err)})`, err.stack);
          t.end(err);
        });

    } catch (err) {
      t.fail(`consumeDeadRecords should NOT have failed (${err})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('consumeDeadRecords with 1 message that succeeds, but has 1 old rejected task and cannot discard must fail', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2');

    const context = {};

    // Generate a sample AWS event
    const streamName = 'DeadRecordQueue_DEV2';
    const msg = sampleUnusableRecord(1);

    // Add some "history" to this message to give it a no-longer active task that will trigger abandonment of this task
    const taskX = Task.createTask(TaskDef.defineTask('TaskX', noop));
    for (let i = 0; i < 98; ++i) {
      taskX.incrementAttempts();
    }
    taskX.reject('Rejected deliberately', new Error('Previously rejected'));
    const taskXLike = JSON.parse(JSON.stringify(taskX));

    msg.drqTaskTracking = {alls: {'TaskX': taskXLike}};
    const m = JSON.parse(JSON.stringify(msg));
    t.ok(m, 'Message with tasks is parsable');
    const taskXRevived = m.drqTaskTracking.alls.TaskX;
    t.ok(Task.isTaskLike(taskXRevived), `TaskX must be task-like (${stringify(taskXRevived)})`);
    t.deepEqual(taskXRevived, taskXLike, `TaskX revived must be original TaskX task-like (${stringify(taskXRevived)})`);

    const event = sampleKinesisEvent(streamName, undefined, msg, false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    const fatalError = new Error('Disabling Kinesis');
    configureKinesisAndDynamoDB(t, context, fatalError, undefined);

    // Process the event
    try {
      configureDRQConsumer(context, event, awsContext);
      const promise = drqConsumer.consumeDeadRecords(event, context);

      if (Promise.isPromise(promise)) {
        t.pass(`consumeDeadRecords returned a promise`);
      } else {
        t.fail(`consumeDeadRecords should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, 'context.awsContext must be given awsContext');

      promise
        .then(messages => {
          const n = messages.length;
          t.fail(`consumeDeadRecords must NOT resolve with ${n} message(s)`);
          t.end();
        })
        .catch(err => {
          t.pass(`consumeDeadRecords must reject with error (${stringify(err)})`);
          t.equal(err, fatalError, `consumeDeadRecords error must be ${fatalError}`);

          streamConsumer.awaitStreamConsumerResults(err.streamConsumerResults).then(results => {
            const messages = results.messages;
            t.equal(messages.length, 1, `consumeDeadRecords results must have ${1} messages`);
            t.equal(results.unusableRecords.length, 0, `consumeDeadRecords results must have ${0} unusable records`);

            t.ok(results.processing.completed, `consumeDeadRecords processing must be completed`);
            t.notOk(results.processing.failed, `consumeDeadRecords processing must not be failed`);
            t.notOk(results.processing.timedOut, `consumeDeadRecords processing must not be timed-out`);

            if (!results.discardedUnusableRecords || results.discardUnusableRecordsError) {
              t.fail(`discardUnusableRecords must not fail with ${results.discardUnusableRecordsError}`);
            }
            if (results.discardedUnusableRecords) {
              t.equal(results.discardedUnusableRecords.length, 0, `discardUnusableRecords must have ${0} discarded unusable records`);
            }

            if (!results.handledIncompleteMessages || results.handleIncompleteMessagesError) {
              t.fail(`handleIncompleteMessages must not fail with ${results.handleIncompleteMessagesError}`);
            }
            if (results.handledIncompleteMessages) {
              t.equal(results.handledIncompleteMessages.length, 0, `handleIncompleteMessages must have ${0} handled incomplete messages`);
            }

            if (results.discardedRejectedMessages || !results.discardRejectedMessagesError) {
              t.fail(`discardRejectedMessages must fail with ${results.discardRejectedMessagesError}`);
            }
            t.equal(results.discardRejectedMessagesError, fatalError, `discardedRejectedMessages must fail with ${fatalError}`);

            t.end();
          });
        });


    } catch (err) {
      t.fail(`consumeDeadRecords should NOT have failed (${err})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// consumeDeadRecords with message(s) exceeding max number of attempts on its saveDeadRecord task, must discard Discarded message(s)
// =====================================================================================================================

test('consumeDeadRecords with 1 message that exceeds max number of attempts on its saveDeadRecord task - must discard Discarded message', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2');

    const context = {};

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    configureKinesisAndDynamoDB(t, context, undefined, new Error('Planned DynamoDB error'));
    drqConsumer.configureDefaultDeadRecordQueueConsumer(context);

    // Generate a sample AWS event
    const streamName = 'DeadRecordQueue_DEV2';
    const msg = sampleUnusableRecord(1);

    const maxNumberOfAttempts = streamProcessing.getMaxNumberOfAttempts(context);

    // Add some "history" to this message to give it a no-longer active task that will trigger discard of this task
    const task1Before = Task.createTask(TaskDef.defineTask('saveDeadRecord', drqConsumer.saveDeadRecord));
    task1Before.fail(new Error('Previously failed saveDeadRecord'));

    // Push saveDeadRecord task's number of attempts to the brink
    for (let a = 0; a < maxNumberOfAttempts - 1; ++a) {
      task1Before.incrementAttempts();
    }
    t.equal(task1Before.attempts, maxNumberOfAttempts - 1, `BEFORE saveDeadRecord attempts must be ${maxNumberOfAttempts - 1}`);

    const task1Like = JSON.parse(JSON.stringify(task1Before));

    msg.drqTaskTracking = {ones: {'saveDeadRecord': task1Like}};

    const m = JSON.parse(JSON.stringify(msg));
    t.ok(m, 'Message with tasks is parsable');

    const task1Revived = m.drqTaskTracking.ones.saveDeadRecord;
    t.ok(Task.isTaskLike(task1Revived), `saveDeadRecord must be task-like (${stringify(task1Revived)})`);
    t.deepEqual(task1Revived, task1Like, `saveDeadRecord revived must be original saveDeadRecord task-like (${stringify(task1Revived)})`);

    t.equal(task1Revived.attempts, maxNumberOfAttempts - 1, `REVIVED saveDeadRecord attempts must be ${maxNumberOfAttempts - 1}`);


    const event = sampleKinesisEvent(streamName, undefined, msg, false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Process the event
    try {
      configureDRQConsumer(context, event, awsContext);
      const promise = drqConsumer.consumeDeadRecords(event, context);

      if (Promise.isPromise(promise)) {
        t.pass(`consumeDeadRecords returned a promise`);
      } else {
        t.fail(`consumeDeadRecords should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, 'context.awsContext must be given awsContext');

      promise
        .then(results => {
          t.pass(`consumeDeadRecords must resolve`);
          const n = 1;
          const messages = results.messages;
          t.equal(messages.length, n, `consumeDeadRecords results must have ${n} messages`);
          checkMessagesTasksStates(t, messages, taskStates.Discarded, taskStates.Discarded, context);
          t.equal(messages[0].drqTaskTracking.ones.saveDeadRecord.attempts, maxNumberOfAttempts, `saveDeadRecord attempts must be ${maxNumberOfAttempts}`);

          t.ok(results.processing.completed, `consumeDeadRecords processing must be completed`);
          t.notOk(results.processing.failed, `consumeDeadRecords processing must not be failed`);
          t.notOk(results.processing.timedOut, `consumeDeadRecords processing must not be timed-out`);

          t.equal(results.unusableRecords.length, 0, `consumeDeadRecords results must have ${0} unusable records`);
          t.equal(results.discardedUnusableRecords.length, 0, `consumeDeadRecords results must have ${0} discarded unusable records`);
          t.equal(results.handledIncompleteMessages.length, 0, `consumeDeadRecords results must have ${0} handled incomplete records`);
          t.equal(results.discardedRejectedMessages.length, 1, `consumeDeadRecords results must have ${1} discarded rejected messages`);

          t.end();
        })
        .catch(err => {
          t.fail(`consumeDeadRecords should NOT have failed (${stringify(err)})`, err.stack);
          t.end(err);
        });

    } catch (err) {
      t.fail(`consumeDeadRecords should NOT have failed (${err})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('consumeDeadRecords with 1 message that exceeds max number of attempts on its saveDeadRecord task, but cannot discard must fail', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2');

    const context = {};

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    const fatalError = new Error('Disabling Kinesis');
    configureKinesisAndDynamoDB(t, context, fatalError, new Error('Planned DynamoDB error'));
    drqConsumer.configureDefaultDeadRecordQueueConsumer(context);

    // Generate a sample AWS event
    const streamName = 'DeadRecordQueue_DEV2';
    const msg = sampleUnusableRecord(1);

    const maxNumberOfAttempts = streamProcessing.getMaxNumberOfAttempts(context);

    // Add some "history" to this message to give it a no-longer active task that will trigger discard of this task
    const task1Before = Task.createTask(TaskDef.defineTask('saveDeadRecord', drqConsumer.saveDeadRecord));
    task1Before.fail(new Error('Previously failed saveDeadRecord'));

    // Push saveDeadRecord task's number of attempts to the brink
    for (let a = 0; a < maxNumberOfAttempts - 1; ++a) {
      task1Before.incrementAttempts();
    }
    t.equal(task1Before.attempts, maxNumberOfAttempts - 1, `BEFORE saveDeadRecord attempts must be ${maxNumberOfAttempts - 1}`);

    const task1Like = JSON.parse(JSON.stringify(task1Before));

    msg.drqTaskTracking = {ones: {'saveDeadRecord': task1Like}};

    const m = JSON.parse(JSON.stringify(msg));
    t.ok(m, 'Message with tasks is parsable');

    const task1Revived = m.drqTaskTracking.ones.saveDeadRecord;
    t.ok(Task.isTaskLike(task1Revived), `saveDeadRecord must be task-like (${stringify(task1Revived)})`);
    t.deepEqual(task1Revived, task1Like, `saveDeadRecord revived must be original saveDeadRecord task-like (${stringify(task1Revived)})`);

    t.equal(task1Revived.attempts, maxNumberOfAttempts - 1, `REVIVED saveDeadRecord attempts must be ${maxNumberOfAttempts - 1}`);


    const event = sampleKinesisEvent(streamName, undefined, msg, false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Process the event
    try {
      configureDRQConsumer(context, event, awsContext);
      const promise = drqConsumer.consumeDeadRecords(event, context);

      if (Promise.isPromise(promise)) {
        t.pass(`consumeDeadRecords returned a promise`);
      } else {
        t.fail(`consumeDeadRecords should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, 'context.awsContext must be given awsContext');

      promise
        .then(messages => {
          const n = messages.length;
          t.fail(`consumeDeadRecords must NOT resolve with ${n} message(s)`);
          t.end();
        })
        .catch(err => {
          t.pass(`consumeDeadRecords must reject with error (${stringify(err)})`);
          t.equal(err, fatalError, `consumeDeadRecords error must be ${fatalError}`);

          streamConsumer.awaitStreamConsumerResults(err.streamConsumerResults).then(results => {
            const messages = results.messages;
            t.equal(messages.length, 1, `consumeDeadRecords results must have ${1} messages`);
            t.equal(results.unusableRecords.length, 0, `consumeDeadRecords results must have ${0} unusable records`);

            t.ok(results.processing.completed, `consumeDeadRecords processing must be completed`);
            t.notOk(results.processing.failed, `consumeDeadRecords processing must not be failed`);
            t.notOk(results.processing.timedOut, `consumeDeadRecords processing must not be timed-out`);

            if (!results.discardedUnusableRecords || results.discardUnusableRecordsError) {
              t.fail(`discardUnusableRecords must not fail with ${results.discardUnusableRecordsError}`);
            }
            if (results.discardedUnusableRecords) {
              t.equal(results.discardedUnusableRecords.length, 0, `discardUnusableRecords must have ${0} discarded unusable records`);
            }

            if (!results.handledIncompleteMessages || results.handleIncompleteMessagesError) {
              t.fail(`handleIncompleteMessages must not fail with ${results.handleIncompleteMessagesError}`);
            }
            if (results.handledIncompleteMessages) {
              t.equal(results.handledIncompleteMessages.length, 0, `handleIncompleteMessages must have ${0} handled incomplete messages`);
            }

            if (results.discardedRejectedMessages || !results.discardRejectedMessagesError) {
              t.fail(`discardRejectedMessages must fail with ${results.discardRejectedMessagesError}`);
            }
            t.equal(results.discardRejectedMessagesError, fatalError, `discardedRejectedMessages must fail with ${fatalError}`);

            t.end();
          });
        });

    } catch (err) {
      t.fail(`consumeDeadRecords should NOT have failed (${err})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// consumeDeadRecords with 1 message and triggered timeout promise, must resubmit incomplete message
// =====================================================================================================================

test('consumeDeadRecords with 1 message and triggered timeout promise, must resubmit incomplete message', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2');

    const context = {};

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    configureKinesisAndDynamoDB(t, context, undefined, undefined, 15);

    const n = 1;

    // Generate a sample AWS event
    const streamName = 'DeadRecordQueue_DEV2';
    const event = sampleKinesisEvent(streamName, undefined, sampleUnusableRecord(1), false);

    // Generate a sample AWS context
    const maxTimeInMillis = 10;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Process the event
    try {
      configureDRQConsumer(context, event, awsContext);
      const promise = drqConsumer.consumeDeadRecords(event, context);

      if (Promise.isPromise(promise)) {
        t.pass(`consumeDeadRecords returned a promise`);
      } else {
        t.fail(`consumeDeadRecords should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, 'context.awsContext must be given awsContext');

      promise
        .then(results => {
          t.pass(`consumeDeadRecords must resolve`);
          const n = 1;
          const messages = results.messages;
          t.equal(messages.length, n, `consumeDeadRecords results must have ${n} messages`);
          checkMessagesTasksStates(t, messages, taskStates.TimedOut, taskStates.TimedOut, context);
          t.equal(messages[0].drqTaskTracking.ones.saveDeadRecord.attempts, 1, `saveDeadRecord attempts must be 1`);

          t.notOk(results.processing.completed, `consumeDeadRecords processing must not be completed`);
          t.notOk(results.processing.failed, `consumeDeadRecords processing must not be failed`);
          t.ok(results.processing.timedOut, `consumeDeadRecords processing must be timed-out`);

          t.equal(results.unusableRecords.length, 0, `consumeDeadRecords results must have ${0} unusable records`);
          t.equal(results.discardedUnusableRecords.length, 0, `consumeDeadRecords results must have ${0} discarded unusable records`);
          t.equal(results.handledIncompleteMessages.length, 1, `consumeDeadRecords results must have ${1} handled incomplete records`);
          t.equal(results.discardedRejectedMessages.length, 0, `consumeDeadRecords results must have ${0} discarded rejected messages`);

          t.end();
        })

    } catch (err) {
      t.fail(`consumeDeadRecords should NOT have failed (${err})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('consumeDeadRecords with 1 message and triggered timeout promise, must fail if it cannot resubmit incomplete messages', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2');

    const context = {};

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    const fatalError = new Error('Disabling Kinesis');
    configureKinesisAndDynamoDB(t, context, fatalError, undefined, 15);

    const n = 1;

    // Generate a sample AWS event
    const streamName = 'DeadRecordQueue_DEV2';
    const event = sampleKinesisEvent(streamName, undefined, sampleUnusableRecord(1), false);

    // Generate a sample AWS context
    const maxTimeInMillis = 10;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Process the event
    try {
      configureDRQConsumer(context, event, awsContext);
      const promise = drqConsumer.consumeDeadRecords(event, context);

      if (Promise.isPromise(promise)) {
        t.pass(`consumeDeadRecords returned a promise`);
      } else {
        t.fail(`consumeDeadRecords should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, 'context.awsContext must be given awsContext');

      promise
        .then(messages => {
          const n = messages.length;
          t.fail(`consumeDeadRecords must NOT resolve with ${n} message(s)`);
          t.end();
        })
        .catch(err => {
          t.pass(`consumeDeadRecords must reject with error (${stringify(err)})`);
          t.equal(err, fatalError, `consumeDeadRecords error must be ${fatalError}`);

          streamConsumer.awaitStreamConsumerResults(err.streamConsumerResults).then(results => {
            const messages = results.messages;
            t.equal(messages.length, 1, `consumeDeadRecords results must have ${1} messages`);
            t.equal(results.unusableRecords.length, 0, `consumeDeadRecords results must have ${0} unusable records`);

            t.notOk(results.processing.completed, `consumeDeadRecords processing must not be completed`);
            t.notOk(results.processing.failed, `consumeDeadRecords processing must not be failed`);
            t.ok(results.processing.timedOut, `consumeDeadRecords processing must be timed-out`);

            if (!results.discardedUnusableRecords || results.discardUnusableRecordsError) {
              t.fail(`discardUnusableRecords must not fail with ${results.discardUnusableRecordsError}`);
            }
            if (results.discardedUnusableRecords) {
              t.equal(results.discardedUnusableRecords.length, 0, `discardUnusableRecords must have ${0} discarded unusable records`);
            }

            if (results.handledIncompleteMessages || !results.handleIncompleteMessagesError) {
              t.fail(`handleIncompleteMessages must fail with ${results.handleIncompleteMessagesError}`);
            }
            t.equal(results.handleIncompleteMessagesError, fatalError, `handleIncompleteMessages must fail with ${fatalError}`);

            if (!results.discardedRejectedMessages || results.discardRejectedMessagesError) {
              t.fail(`discardRejectedMessages must not fail with ${results.discardRejectedMessagesError}`);
            }
            if (results.discardedRejectedMessages) {
              t.equal(results.discardedRejectedMessages.length, 0, `discardedRejectedMessages must have ${0} discarded rejected messages`);
            }

            t.end();
          });

        });

    } catch (err) {
      t.fail(`consumeDeadRecords should NOT have failed (${err})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});