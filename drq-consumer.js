'use strict';

const regions = require('aws-core-utils/regions');

const Objects = require('core-functions/objects');

const Strings = require('core-functions/strings');
const stringify = Strings.stringify;
const trimOrEmpty = Strings.trimOrEmpty;

const taskDefs = require('task-utils/task-defs');
const TaskDef = taskDefs.TaskDef;

const logging = require('logging-utils');
const stages = require('aws-core-utils/stages');
const dynamoDBUtils = require('aws-core-utils/dynamodb-utils');
const streamEvents = require('aws-core-utils/stream-events');
const arns = require('aws-core-utils/arns');

const streamProcessing = require('aws-stream-consumer/stream-processing');
const streamConsumer = require('aws-stream-consumer/stream-consumer');

/**
 * An AWS Lambda that will consume unusable, "dead" records from a Dead Record Queue (DRQ) Kinesis stream and save them
 * to DynamoDB. Alternatively, can be used as a module to configure another more-customised version of a DRQ consumer
 * Lambda.
 * @module aws-drq-consumer/drq-consumer
 * @author Byron du Preez
 */
module.exports = {
  // Export this module's functions for customisation and testing purposes
  // Configuration functions
  configureDeadRecordQueueConsumer: configureDeadRecordQueueConsumer,
  configureDeadRecordQueueProcessing: configureDeadRecordQueueProcessing,
  configureDefaultDeadRecordQueueProcessing: configureDefaultDeadRecordQueueProcessing,
  getDefaultDeadRecordQueueProcessingSettings: getDefaultDeadRecordQueueProcessingSettings,
  // Processing functions
  consumeDeadRecords: consumeDeadRecords,
  saveDeadRecord: saveDeadRecord,
  toDeadRecord: toDeadRecord,
  toDeadRecordFromKinesisRecord: toDeadRecordFromKinesisRecord,
  toDeadRecordFromDynamoDBStreamRecord: toDeadRecordFromDynamoDBStreamRecord,
  toDeadRecordFromS3Record: toDeadRecordFromS3Record,
  toDeadRecordFromSESRecord: toDeadRecordFromSESRecord,
  toDeadRecordFromSNSRecord: toDeadRecordFromSNSRecord,
};

module.exports.handler = function (event, awsContext, callback) {
  const context = {};

  try {
    // Configure the dead record queue consumer
    configureDeadRecordQueueConsumer(context, undefined, require('./drq-options.json'), event, awsContext);

    // Consume the dead records
    consumeDeadRecords(event, context)
      .then(() => { // results => {
        //context.info(`DRQ stream consumer results: ${JSON.stringify(results)}`);
        callback(null);
      })
      .catch(err => {
        context.error(err.message, err.stack);
        // streamConsumer.awaitStreamConsumerResults(err.streamConsumerResults).then(results =>
        //   context.error(`DRQ stream consumer partial results: ${results ? JSON.stringify(results) : 'None available'}`)
        // );
        callback(err);
      });

  } catch (err) {
    (context.error ? context.error : console.error)(`DRQ consumer failed to consume dead records for event (${stringify(event)})`, err.stack);
    callback(err);
  }
};

// =====================================================================================================================
// Configure dead record processing and consumer
// =====================================================================================================================

/**
 * @typedef {StreamProcessingOptions} DRQProcessingSettings - DRQ stream processing settings to use
 * @property {string|undefined} [deadRecordTableName] - optional DynamoDB dead record table name (defaults to 'DeadRecord' if undefined)
 */

/**
 * @typedef {StreamProcessingOptions} DRQProcessingOptions - DRQ stream processing options to use if no corresponding settings are provided
 * @property {string|undefined} [deadRecordTableName] - optional DynamoDB dead record table name (defaults to 'DeadRecord' if undefined)
 */

/**
 * @typedef {StreamConsumerSettings} DRQSettings - DRQ configuration settings to use
 * @property {DRQProcessingSettings|undefined} [streamProcessingSettings] - optional stream processing settings to use to configure stream processing
 */

/**
 * @typedef {StreamConsumerOptions} DRQOptions - DRQ configuration options to use if no corresponding settings are provided
 * @property {DRQProcessingOptions|undefined} [streamProcessingOptions] - optional stream processing options to use to configure stream processing
 */

/**
 * Configures the settings and dependencies of a Dead Record Queue consumer.
 * @param {Object} context - the context
 * @param {DRQSettings|undefined} [settings] - optional configuration settings to use
 * @param {DRQOptions|undefined} [options] - configuration options to use if no corresponding settings are provided
 * @param {Object} event - an AWS event
 * @param {Object} awsContext - the AWS context
 */
function configureDeadRecordQueueConsumer(context, settings, options, event, awsContext) {
  // Configure DRQ stream processing (plus logging, stage handling & kinesis) if not configured yet
  configureDeadRecordQueueProcessing(context, settings ? settings.streamProcessingSettings : undefined,
    options ? options.streamProcessingOptions : undefined, settings, options, false);

  // Configure the DRQ stream consumer
  streamConsumer.configureStreamConsumer(context, settings, options, event, awsContext);
}

/**
 * Configures the given context with the given DRQ stream processing settings (if any) otherwise with the default DRQ
 * stream processing settings partially overridden by the given DRQ stream processing options (if any), but only if
 * stream processing is not already configured on the given context OR if forceConfiguration is true.
 *
 * @param {Object} context - the context to configure
 * @param {DRQProcessingSettings|undefined} [settings] - optional stream processing settings to use to configure stream processing
 * @param {DRQProcessingOptions|undefined} [options] - optional stream processing options to use to override default options
 * @param {OtherSettings|undefined} [otherSettings] - optional other settings to use to configure dependencies
 * @param {OtherOptions|undefined} [otherOptions] - optional other options to use to configure dependencies if corresponding settings are not provided
 * @param {boolean|undefined} [forceConfiguration] - whether or not to force configuration of the given settings, which
 * will override any previously configured DRQ stream processing settings on the given context
 * @returns {Object} the given context
 */
function configureDeadRecordQueueProcessing(context, settings, options, otherSettings, otherOptions, forceConfiguration) {
  const settingsAvailable = settings && typeof settings === 'object';
  const optionsAvailable = options && typeof options === 'object';

  // Check if stream processing was already configured
  const streamProcessingWasConfigured = streamProcessing.isStreamProcessingConfigured(context);

  // Determine the stream processing settings to be used
  const defaultSettings = getDefaultDeadRecordQueueProcessingSettings(options);

  const streamProcessingSettings = settingsAvailable ?
    Objects.merge(defaultSettings, settings, false, false) : defaultSettings;

  // Configure stream processing with the given or derived stream processing settings
  streamProcessing.configureStreamProcessingWithSettings(context, streamProcessingSettings, otherSettings, otherOptions, forceConfiguration);

  // Log a warning if no settings and no options were provided and the default settings were applied
  if (!settingsAvailable && !optionsAvailable && (forceConfiguration || !streamProcessingWasConfigured)) {
    context.warn(`DRQ stream processing was configured without settings or options - used default DRQ stream processing configuration (${stringify(streamProcessingSettings)})`);
  }
  return context;
}

/**
 * Configures the given context with the default DRQ stream processing settings, but only if stream processing is not
 * already configured on the given context OR if forceConfiguration is true.
 *
 * @param {Object} context - the context to configure
 * @param {boolean|undefined} [forceConfiguration] - whether or not to force configuration of the given settings, which
 * will override any previously configured DRQ stream processing settings on the given context
 * @returns {Object} the given context
 */
function configureDefaultDeadRecordQueueProcessing(context, forceConfiguration) {
  const drqOptions = require('./drq-options.json');
  const drqProcessingOptions = drqOptions ? drqOptions.streamProcessingOptions : {};
  return configureDeadRecordQueueProcessing(context, undefined, drqProcessingOptions, undefined, drqOptions, forceConfiguration);
}

/**
 * Returns the default DRQ stream processing settings partially overridden by the given stream processing options (if any).
 *
 * This function is used internally by {@linkcode configureDefaultDeadRecordProcessing}, but could also be used in
 * custom configurations to get the default settings as a base to be overridden with your custom settings before calling
 * {@linkcode configureDeadRecordQueueProcessing}.
 *
 * @param {DRQProcessingOptions} [options] - optional DRQ stream processing options to use to override the default options
 * @returns {DRQProcessingSettings} a DRQ stream processing settings object (including both property and function settings)
 */
function getDefaultDeadRecordQueueProcessingSettings(options) {
  const overrideOptions = options && typeof options === 'object' ? Objects.copy(options, true) : {};

  // Load defaults from local drq-options.json file
  const defaultOptions = loadDefaultDeadRecordQueueProcessingOptions();
  Objects.merge(defaultOptions, overrideOptions, false, false);

  // Start with the default Kinesis stream processing settings
  const defaultSettings = streamProcessing.getDefaultKinesisStreamProcessingSettings(overrideOptions);

  // Customise the discardUnusableRecords function to eliminate any unusable ("deader"), dead records during DRQ stream processing
  defaultSettings.discardUnusableRecords = eliminateUnusableRecords;

  return defaultSettings;
}

/**
 * Loads the default DRQ stream processing options from the local drq-options.json file and fills in any missing
 * options with the static default options.
 * @returns {DRQProcessingOptions} the default stream processing options
 */
function loadDefaultDeadRecordQueueProcessingOptions() {
  const options = require('./drq-options.json');
  const defaultOptions = options && options.streamProcessingOptions && typeof options.streamProcessingOptions === 'object' ?
    options.streamProcessingOptions : {};

  const defaults = {
    // Generic settings
    streamType: streamProcessing.KINESIS_STREAM_TYPE,
    taskTrackingName: 'drqTaskTracking',
    timeoutAtPercentageOfRemainingTime: 0.9,
    maxNumberOfAttempts: 10,
    // Specialised settings needed by implementations using external task tracking
    // taskTrackingTableName: undefined,
    // Specialised settings needed by default implementations - e.g. DRQ and DMQ stream names
    // deadRecordQueueName: undefined,
    deadMessageQueueName: 'DeadMessageQueue',
    // Kinesis & DynamoDB.DocumentClient options
    kinesisOptions: {},
    dynamoDBDocClientOptions: {},
    deadRecordTableName: 'DeadRecord'
  };
  return Objects.merge(defaults, defaultOptions, false, false);
}

// =====================================================================================================================
// Consume dead records
// =====================================================================================================================

/**
 * @typedef {Object} DeadRecord - represents a common structure for storing an unusable record as a dead record
 * @property {string} regionEventSourceAndName - the AWS region (if available, else "aws"), the event source (e.g.
 * kinesis, ses, ...) and a selected event name, which varies by event type
 * @property {string} keys - a string containing selected, pipe-separated ('|') key-related values to provide a range key value
 * @property {Object} record - the original unusable record
 */

/**
 * Consumes all of the unusable/dead records in the given event by transforming them into dead records and writing them
 * to a DynamoDB dead record table.
 * @param {Object} event - an AWS event
 * @param {Object} context - the context
 * @returns {Promise.<StreamConsumerResults|StreamConsumerError>} a resolved promise with the full stream processing
 * results or a rejected promise with an error with partial stream processing results
 */
function consumeDeadRecords(event, context) {
  // Define the task(s) that must be executed on each unusable record
  const saveDeadRecordTaskDef = TaskDef.defineTask(saveDeadRecord.name, saveDeadRecord);

  // Finally process the stream event
  return streamConsumer.processStreamEvent(event, [saveDeadRecordTaskDef], [], context);
}

/**
 * Transforms the given unusable record into a dead record (DeadRecord) and saves it to the DynamoDB dead record table.
 * @param {Object} record - an AWS event record to save
 * @param {Object} context - the context
 * @param {Object} context.streamProcessing - the stream processing configuration on the context
 * @param {string} context.streamProcessing.deadRecordTableName - the name of the dead record table to which to save
 * @returns {*} the DynamoDB put result
 */
function saveDeadRecord(record, context) {
  const task = this;
  context.trace(`DRQ consumer is saving dead record (${stringify(record)})`);

  const dynamoDBDocClient = context.dynamoDBDocClient;
  const tableName = stages.toStageQualifiedStreamName(context.streamProcessing.deadRecordTableName, context.stage, context);

  const deadRecord = toDeadRecord(record);
  if (!deadRecord) {
    const reason = `DRQ consumer failed to transform and save a dead record for unexpected type of record (${stringify(record)})`;
    context.error(reason);
    task.reject(reason, undefined, true);
    return Promise.resolve(undefined);
  }

  const request = {
    TableName: tableName,
    Item: deadRecord
  };

  return dynamoDBDocClient.put(request).promise()
    .then(result => {
      context.info(`DRQ consumer saved dead record (${deadRecord.regionEventSourceAndName}, ${deadRecord.keys}) to ${tableName} - result (${JSON.stringify(result)})`);
      task.succeed(result, false);
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
  const streamName = streamEvents.getKinesisEventSourceStreamName(record);
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
  const tableNameAndStreamTimestamp = streamEvents.getDynamoDBEventSourceTableNameAndStreamTimestamp(record);
  const tableName = tableNameAndStreamTimestamp[0];
  const streamTimestamp = tableNameAndStreamTimestamp[0];
  const regionEventSourceAndName = `${record.awsRegion}|dynamodb|${tableName}`;
  // Combine all of the record's Keys into a single string
  const keysAndValues = dynamoDBUtils.toKeyValueStrings(record.dynamodb.Keys);
  const keys = `[${keysAndValues.join('][')}]|${record.dynamodb.SequenceNumber}|${streamTimestamp}|${record.eventID}`;

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

