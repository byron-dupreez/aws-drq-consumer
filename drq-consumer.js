'use strict';

const regions = require('aws-core-utils/regions');

const Strings = require('core-functions/strings');
const stringify = Strings.stringify;
const trimOrEmpty = Strings.trimOrEmpty;

const taskDefs = require('task-utils/task-defs');
const TaskDef = taskDefs.TaskDef;

const logging = require('logging-utils');
const stages = require('aws-core-utils/stages');
const kinesisUtils = require('aws-core-utils/kinesis-utils');
const dynamoDbDocClients = require('aws-core-utils/dynamodb-doc-clients');
const streamEvents = require('aws-core-utils/stream-events');
const arns = require('aws-core-utils/arns');

const streamProcessing = require('aws-stream-consumer/stream-processing');
const streamConsumerConfig = require('aws-stream-consumer/stream-consumer-config');
const streamConsumer = require('aws-stream-consumer/stream-consumer');

/**
 * An AWS Lambda that will consume unusable, "dead" records from a Dead Record Queue (DRQ) Kinesis stream and save them
 * to DynamoDB. Alternatively, can be used as a module to configure another more-customised version of a DRQ consumer
 * Lambda.
 * @module aws-drq-consumer/drq-consumer
 * @author Byron du Preez
 */
exports.handler = function (event, awsContext, callback) {
  const context = {};

  try {
    const settings = undefined;
    const options = require('./config.json');

    // Configure the dead record queue processing
    configureDeadRecordQueueProcessing(context, settings, options);

    // Configure the dead record queue consumer's runtime-settings
    configureDeadRecordQueueConsumer(context, settings, options, event, awsContext);

    // Consume the dead records
    consumeDeadRecords(event, context)
      .then(results => {
        context.debug(results);
        callback(null, results);
      })
      .catch(err => {
        context.error(err);
        callback(err);
      });

  } catch (err) {
    (context.error ? context.error : console.error)(`DRQ consumer failed to consume dead records for event (${event})`, err.stack);
    callback(err);
  }
};

// Export this module's functions for customisation and testing purposes
exports.configureDeadRecordQueueProcessing = configureDeadRecordQueueProcessing;
exports.configureDeadRecordQueueConsumer = configureDeadRecordQueueConsumer;
exports.consumeDeadRecords = consumeDeadRecords;
exports.saveDeadRecord = saveDeadRecord;
exports.toDeadRecord = toDeadRecord;
exports.toDeadRecordFromKinesisRecord = toDeadRecordFromKinesisRecord;
exports.toDeadRecordFromDynamoDBStreamRecord = toDeadRecordFromDynamoDBStreamRecord;
exports.toDeadRecordFromS3Record = toDeadRecordFromS3Record;
exports.toDeadRecordFromSESRecord = toDeadRecordFromSESRecord;
exports.toDeadRecordFromSNSRecord = toDeadRecordFromSNSRecord;

/**
 * @typedef {Object} DeadRecord - represents a common structure for storing an unusable record as a dead record
 * @property {string} regionEventSourceAndName - the AWS region (if available, else "aws"), the event source (e.g.
 * kinesis, ses, ...) and a selected event name, which varies by event type
 * @property {string} keys - a string containing selected, pipe-separated ('|') key-related values to provide a range key value
 * @property {Object} record - the original unusable record
 */

/**
 * @typedef {Object} DrqOptions - DRQ configuration options to use if no corresponding settings are provided
 * @property {LoggingOptions|undefined} [loggingOptions] - optional logging options to use to configure logging
 * @property {StageHandlingOptions|undefined} [stageHandlingOptions] - optional stage handling options to use to configure stage handling
 * @property {StreamProcessingOptions|undefined} [streamProcessingOptions] - optional stream processing options to use to configure stream processing
 * @property {Object|undefined} [kinesisOptions] - optional Kinesis constructor options to use to configure an AWS.Kinesis instance
 * @property {Object|undefined} [dynamoDBDocClientOptions] - optional DynamoDB.DocumentClient constructor options to use to configure an AWS.DynamoDB.DocumentClient instance
 * @property {Object|undefined} [drqConsumerOptions] - optional DRQ consumer specific options
 * @property {string|undefined} [drqConsumerOptions.deadRecordTableName] - optional DynamoDB dead record table name (defaults to 'DeadRecord' if undefined)

 */

/**
 * Consumes all of the unusable/dead records in the given event by transforming them into dead records and writing them
 * to a DynamoDB dead record table.
 * @param {Object} event - an AWS event
 * @param {Object} context - the context
 * @returns {Promise.<StreamProcessingResults|StreamProcessingError>} a resolved promise with the full stream processing
 * results or a rejected promise with an error with partial stream processing results
 */
function consumeDeadRecords(event, context) {
  // Define the task(s) that must be executed on each unusable record
  const saveDeadRecordTaskDef = TaskDef.defineTask(saveDeadRecord.name, saveDeadRecord);

  // Finally process the stream event
  return streamConsumer.processStreamEvent(event, [saveDeadRecordTaskDef], [], context);
}

/**
 * Returns true if a dead record queue consumer's processing settings have been configured on the given context;
 * otherwise returns false.
 * @param {Object} context - the context to check
 * @returns {boolean} true if configured; false otherwise
 */
function isDeadRecordQueueProcessingConfigured(context) {
  return !!context && logging.isLoggingConfigured(context) && stages.isStageHandlingConfigured(context) &&
    streamProcessing.isStreamProcessingConfigured(context) && context.kinesis && context.dynamoDBDocClient;
}

/**
 * Configures the settings of the Dead Record Queue consumer.
 * @param {Object} context - the context
 * @param {Settings|undefined} [settings] - optional configuration settings to use
 * @param {DrqOptions|undefined} [options] - configuration options to use if no corresponding settings are provided
 */
function configureDeadRecordQueueProcessing(context, settings, options) {
  const forceConfiguration = false;

  // Configure logging
  const loggingSettings = settings && settings.loggingSettings ? settings.loggingSettings :
    logging.getDefaultLoggingSettings(options ? options.loggingOptions : undefined);

  logging.configureLogging(context, loggingSettings, forceConfiguration);

  // Configure stage-handling
  const stageHandlingSettings = settings && settings.stageHandlingSettings ? settings.stageHandlingSettings :
    stages.getDefaultStageHandlingSettings(options ? options.stageHandlingOptions : undefined);

  stages.configureStageHandling(context, stageHandlingSettings, settings, options, forceConfiguration);

  // Configure a Kinesis instance
  kinesisUtils.configureKinesis(context, options ? options.kinesisOptions : undefined);

  // Configure a DynamoDB document client instance
  dynamoDbDocClients.configureDynamoDBDocClient(context, options ? options.dynamoDBDocClientOptions : undefined);

  // Configure custom stream processing
  if (settings && typeof settings === 'object' && settings.streamProcessingSettings && typeof settings.streamProcessingSettings === 'object') {
    // Configure custom stream processing
    streamProcessing.configureStreamProcessing(context, settings.streamProcessingSettings, settings, options, forceConfiguration);
  } else {
    // Get the default Kinesis stream processing settings
    const streamProcessingSettings = streamProcessing.getDefaultKinesisStreamProcessingSettings(options ? options.streamProcessingOptions : undefined);
    // Customize the default Kinesis stream processing functions
    // streamProcessingSettings.extractMessageFromRecord = streamProcessing.DEFAULTS.extractJsonMessageFromKinesisRecord;
    streamProcessingSettings.discardUnusableRecords = eliminateUnusableRecords;
    // streamProcessingSettings.resubmitIncompleteMessages = streamProcessing.DEFAULTS.resubmitIncompleteMessagesToKinesis();
    // streamProcessingSettings.discardRejectedMessages = streamProcessing.DEFAULTS.discardRejectedMessagesToDMQ;

    // Configure custom stream processing
    streamProcessing.configureStreamProcessing(context, streamProcessingSettings, settings, options, forceConfiguration);
  }
}

/**
 * Configures the settings of the Dead Record Queue consumer.
 * @param {Object} context - the context
 * @param {Settings|undefined} [settings] - optional configuration settings to use
 * @param {DrqOptions|undefined} [options] - configuration options to use if no corresponding settings are provided
 * @param {Object} event - an AWS event
 * @param {Object} awsContext - the AWS context
 */
function configureDeadRecordQueueConsumer(context, settings, options, event, awsContext) {
  // Ensure that the DRQ processing settings are configured too
  if (!isDeadRecordQueueProcessingConfigured(context)) {
    configureDeadRecordQueueProcessing(context, settings, options);
  }

  // Configure the stream consumer's runtime settings
  streamConsumerConfig.configureStreamConsumer(context, settings, options, event, awsContext);

  // Set up the DRQ consumer specific settings on the context
  if (!context.drqConsumer) {
    context.drqConsumer = {};
  }
  // Get the dead record table name
  if (!context.drqConsumer.deadRecordTableName) {
    context.drqConsumer.deadRecordTableName = options.drqConsumerOptions && options.drqConsumerOptions.deadRecordTableName ?
      options.drqConsumerOptions.deadRecordTableName : 'DeadRecord';
  }
}

/**
 * Transforms the given unusable record into a dead record (DeadRecord) and saves it to the DynamoDB dead record table.
 * @param {Object} record - an AWS event record to save
 * @param {Object} context - the context
 * @returns {*} the DynamoDB put result
 */
function saveDeadRecord(record, context) {
  const task = this;
  context.trace(`DRQ consumer is saving dead record (${stringify(record)})`);

  const dynamoDbDocClient = context.dynamoDBDocClient;
  const tableName = stages.toStageQualifiedStreamName(context.drqConsumer.deadRecordTableName, context.stage, context);

  const deadRecord = toDeadRecord(record);
  if (!deadRecord) {
    const reason = `DRQ consumer failed to transform and save a dead record for unexpected type of record (${record})`;
    context.error(reason);
    task.reject(reason, undefined, true);
    return Promise.resolve(undefined);
  }

  const request = {
    TableName: tableName,
    Item: deadRecord
  };

  return dynamoDbDocClient.put(request).promise()
    .then(result => {
      context.trace(`DRQ consumer saved dead record (${deadRecord.regionEventSourceAndName}, ${deadRecord.keys}) to ${tableName}`);
      task.succeed(result);
      return result;
    })
    .catch(err => {
      context.error(`DRQ consumer failed to save dead record (${deadRecord.regionEventSourceAndName}, ${deadRecord.keys}) to ${tableName}`, err.stack);
      task.fail(err);
    });
}

/**
 * Attempts to convert the given record into a dead record. Currently supports the following types of record:
 * 1. Kinesis stream event record;
 * 2. DynamoDB stream event record;
 * 3. S3 event record;
 * 4. SES event record; and
 * 5. SNS event record.
 * @param {Object} record - an AWS event record
 * @returns {DeadRecord} a dead record
 */
function toDeadRecord(record) {
  if (record.eventSource === 'aws:kinesis') {
    return toDeadRecordFromKinesisRecord(record);
  }
  else if (record.eventSource === 'aws:dynamodb') {
    return toDeadRecordFromDynamoDBStreamRecord(record);
  }
  else if (record.eventSource === 'aws:s3') {
    return toDeadRecordFromS3Record(record);
  }
  else if (record.eventSource === 'aws:ses') {
    return toDeadRecordFromSESRecord(record);
  }
  else if (record.EventSource === 'aws:sns') {
    return toDeadRecordFromSNSRecord(record);
  }
  return undefined;
}

/**
 * Attempts to convert the given Kinesis stream event record into a dead record.
 * @param {Object} record - an AWS Kinesis stream event record
 * @returns {DeadRecord} a dead record
 */
function toDeadRecordFromKinesisRecord(record) {
  const streamName = streamEvents.getEventSourceStreamName(record);
  const regionEventSourceAndName = `${record.awsRegion}|kinesis|${streamName}`;
  const keys = `${record.kinesis.partitionKey}|${trimOrEmpty(record.kinesis.explicitHashKey)}|${record.eventID}`;

  return {
    regionEventSourceAndName: regionEventSourceAndName,
    keys: keys,
    record: record
  };
}

/**
 * Attempts to convert the given DynamoDB stream event record into a dead record.
 * @param {Object} record - an AWS DynamoDB stream event record
 * @returns {DeadRecord} a dead record
 */
function toDeadRecordFromDynamoDBStreamRecord(record) {
  const tableName = arns.getArnResources(record.eventSourceARN).resource;
  const regionEventSourceAndName = `${record.awsRegion}|dynamodb|${tableName}`;
  // Combine all of the record's Keys into a single string
  const Keys = record.dynamodb.Keys;
  const keysAndValues = Object.getOwnPropertyNames(Keys).map(k =>
    Object.getOwnPropertyNames(Keys[k]).map(t =>
      `${k}:${Keys[k][t]}`
    )
  );
  const keys = `[${keysAndValues.join('][')}]|${record.dynamodb.SequenceNumber}|${record.eventID}`;

  return {
    regionEventSourceAndName: regionEventSourceAndName,
    keys: keys,
    record: record
  };
}

/**
 * Attempts to convert the given S3 event record into a dead record.
 * @param {Object} record - an AWS S3 event record
 * @returns {DeadRecord} a dead record
 */
function toDeadRecordFromS3Record(record) {
  const s3 = record.s3;
  const bucket = arns.getArnResources(s3.bucket.arn).resource;
  const regionEventSourceAndName = `${record.awsRegion}|s3|${bucket}`;
  const bucketName = s3.bucket.name;
  const objectName = s3.object.key;
  const keys = `${bucketName}|${objectName}|${record.eventTime}|${record.eventName}`;

  return {
    regionEventSourceAndName: regionEventSourceAndName,
    keys: keys,
    record: record
  };
}

/**
 * Attempts to convert the given SNS event record into a dead record.
 * @param {Object} record - an AWS SNS event record
 * @returns {DeadRecord} a dead record
 */
function toDeadRecordFromSNSRecord(record) {
  const awsRegion = arns.getArnRegion(record.EventSubscriptionArn);
  const resource = arns.getArnResources(record.EventSubscriptionArn).resource; //TODO check what this contains
  // record.TopicArn ?
  const regionEventSourceAndName = `${awsRegion}|sns|${resource}`;
  const keys = `${record.Sns.Subject}|${record.Sns.Timestamp}|${record.Sns.MessageId}`;

  return {
    regionEventSourceAndName: regionEventSourceAndName,
    keys: keys,
    record: record
  };
}

/**
 * Attempts to convert the given SES event record into a dead record.
 * @param {Object} record - an AWS SES event record
 * @returns {DeadRecord} a dead record
 */
function toDeadRecordFromSESRecord(record) {
  const mail = record.ses.mail;
  const regionEventSourceAndName = `aws|ses|${mail.source}`;
  const destinations = `[${mail.destination.join('][')}]`;
  const keys = `${mail.timestamp}|${destinations}|${mail.messageId}`;

  return {
    regionEventSourceAndName: regionEventSourceAndName,
    keys: keys,
    record: record
  };
}

/**
 * Eliminates the given UNUSABLE, unusable records by simply logging them and doing nothing more with them.
 *
 * Ideally this function should NEVER be executed, since there should be NO unusable dead records at this point in the
 * flow and because this Lambda is already consuming dead records from the DRQ, there is effectively nowhere left to
 * which to discard unusable, dead records (i.e. there is no "DeaderDead Record Queue").
 *
 * @param {Object[]} unusableRecords - UNUSABLE, unusable records
 * @param {Object} context - the context
 * @returns {Promise.<Array>} the given array of unusable records
 */
function eliminateUnusableRecords(unusableRecords, context) {
  if (!unusableRecords || unusableRecords.length <= 0) {
    // Ideally should be none
    return Promise.resolve([]);
  }
  context.warn(`DRQ consumer is eliminating unusable ("deader"), dead records - ${stringify(unusableRecords)}`);
  return Promise.resolve(unusableRecords);
}

