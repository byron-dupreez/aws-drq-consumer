'use strict';

// const regions = require('aws-core-utils/regions');

const copying = require('core-functions/copying');
const copy = copying.copy;
const deep = copying.defaultCopyOpts.deep;

const merging = require('core-functions/merging');
const merge = merging.merge;

const Strings = require('core-functions/strings');
const stringify = Strings.stringify;
// const trimOrEmpty = Strings.trimOrEmpty;

const TaskDef = require('task-utils/task-defs');

const logging = require('logging-utils');
const LogLevel = logging.LogLevel;

const stages = require('aws-core-utils/stages');
const dynamoDBUtils = require('aws-core-utils/dynamodb-utils');
const streamEvents = require('aws-core-utils/stream-events');
const arns = require('aws-core-utils/arns');

const Settings = require('aws-stream-consumer-core/settings');
const StreamType = Settings.StreamType;

// const streamProcessing = require('kinesis-stream-consumer/kinesis-processing');
const streamConsumer = require('kinesis-stream-consumer/kinesis-consumer');

/**
 * An AWS Lambda that will consume unusable, "dead" records from a Dead Record Queue (DRQ) Kinesis stream and save them
 * to DynamoDB. Alternatively, can be used as a module to configure another more-customised version of a DRQ consumer
 * Lambda.
 * @module aws-drq-consumer/drq-consumer
 * @author Byron du Preez
 */
exports.$ = '$'; //IDE workaround

// Export this module's functions for customisation and testing purposes
// Configuration functions
exports.createSettings = createSettings;
exports.createOptions = createOptions;
exports.defineProcessOneTasks = defineProcessOneTasks;

// exports.configureDRQConsumer = configureDRQConsumer;
// exports.configureDeadRecordQueueProcessing = configureDeadRecordQueueProcessing;
// exports.getDefaultDeadRecordQueueProcessingSettings = getDefaultDRQProcessingSettings;

// Processing functions
exports.consumeDeadRecords = consumeDeadRecords;
exports.saveDeadRecord = saveDeadRecord;
exports.toDeadRecord = toDeadRecord;
exports.toDeadRecordFromKinesisRecord = toDeadRecordFromKinesisRecord;
exports.toDeadRecordFromDynamoDBStreamRecord = toDeadRecordFromDynamoDBStreamRecord;
exports.toDeadRecordFromS3Record = toDeadRecordFromS3Record;
exports.toDeadRecordFromSESRecord = toDeadRecordFromSESRecord;
exports.toDeadRecordFromSNSRecord = toDeadRecordFromSNSRecord;

function createContext() {
  return {};
}

function createSettings() {
  // Customise the discardUnusableRecord function to eliminate any unusable ("deader"), dead records during DRQ stream processing
  return {streamProcessingSettings: {discardUnusableRecord: eliminateUnusableRecord}};
}

function createOptions() {
  return copy(require('./default-drq-options.json'), deep);
}

function defineProcessOneTasks() {
  const taskDefSettings = {};
  const saveDeadRecordTaskDef = TaskDef.defineTask(saveDeadRecord.name, saveDeadRecord, taskDefSettings);
  return [saveDeadRecordTaskDef];
}

const opts = {
  logRequestResponseAtLogLevel: LogLevel.DEBUG,
  failureMsg: 'Failed to save dead record',
  successMsg: 'Saved dead record'
};

// A DEFAULT handler - ONLY use this if the default settings & options in this module are sufficient!
exports.handler = streamConsumer.generateHandlerFunction(createContext, createSettings, createOptions,
  defineProcessOneTasks, undefined, opts);


// exports.handler = function handleDRQConsumerEvent(event, awsContext, callback) {
//   const context = {};
//
//   try {
//     // Configure the dead record queue consumer
//     configureDRQConsumer(context, createSettings, createOptions, event, awsContext);
//
//     // Consume the dead records
//     consumeDeadRecords(event, context)
//       .then(() => { // results => {
//         callback(null);
//       })
//       .catch(err => {
//         context.error(err);
//         callback(err);
//       });
//
//   } catch (err) {
//     (context || console).error(`DRQ consumer failed to consume dead records for event (${stringify(event)})`, err);
//     callback(err);
//   }
// };

// =====================================================================================================================
// Configure dead record processing and consumer
// =====================================================================================================================

// /**
//  * Configures the dependencies and settings for the Dead Record Queue consumer on the given context from the given
//  * settings, the given options, the given AWS event and the given AWS context in preparation for processing of a batch
//  * of unusable/dead Kinesis stream records. Any error thrown must subsequently trigger a replay of all the records in
//  * the current batch until the Lambda can be fixed.
//  *
//  * Note that if either the given event or AWS context are undefined, then everything other than the event, AWS context &
//  * stage will be configured. This missing configuration can be configured at a later point in your code by invoking
//  * {@linkcode module:aws-core-utils/contexts#configureEventAwsContextAndStage}. This separation of configuration is
//  * primarily useful for unit testing.
//  *
//  * @param {Object|DRQConsumerContext|DRQProcessing|StandardContext} context - the context to configure with default DRQ consumer settings
//  * @param {DRQConsumerSettings|undefined} [settings] - optional DRQ consumer settings to use
//  * @param {DRQConsumerOptions|undefined} [options] - optional DRQ consumer options to use
//  * @param {Object|undefined} [event] - the AWS event, which was passed to your lambda
//  * @param {Object|undefined} [awsContext] - the AWS context, which was passed to your lambda
//  * @returns {DRQConsumerContext|DRQProcessing} the given context object configured with full or partial DRQ stream consumer settings
//  * @throws {Error} an error if event and awsContext are specified and the region and/or stage cannot be resolved
//  */
// function configureDRQConsumer(context, settings, options, event, awsContext) {
//
//   const defaultDRQConsumerOptions = require('./default-drq-options.json');
//
//   const drqConsumerOptions = options && typeof options === 'object' ?
//     merge(defaultDRQConsumerOptions, copy(options, deep)) : copy(defaultDRQConsumerOptions, deep);
//
//   // Customise the discardUnusableRecord function to eliminate any unusable ("deader"), dead records during DRQ stream processing
//   const defaultDRQConsumerSettings = {streamProcessingSettings: {discardUnusableRecord: eliminateUnusableRecord}};
//
//   const drqConsumerSettings = settings && typeof settings === 'object' ?
//     merge(defaultDRQConsumerSettings, copy(settings, deep)) : defaultDRQConsumerSettings;
//
//   return streamConsumer.configureStreamConsumer(context, drqConsumerSettings, drqConsumerOptions, event, awsContext);
// }
//
// /**
//  * Configures the dependencies and settings for the Dead Record Queue consumer on the given context with the default DRQ
//  * consumer settings partially overridden by the given options (if any), the given AWS event and the given AWS context
//  * in preparation for processing of a batch of unusable/dead Kinesis stream records. Any error thrown must subsequently
//  * trigger a replay of all the records in the current batch until the Lambda can be fixed.
//  *
//  * Note that if either the given event or AWS context are undefined, then everything other than the event, AWS context &
//  * stage will be configured. This missing configuration must be configured before invoking consumeDeadRecords by invoking
//  * {@linkcode module:aws-core-utils/contexts#configureEventAwsContextAndStage}. This separation of configuration is
//  * primarily useful for unit testing.
//  *
//  * @param {Object|DRQConsumerContext|DRQProcessing|StandardContext} context - the context to configure with default DRQ consumer settings
//  * @param {DRQConsumerOptions|undefined} [options] - optional DRQ consumer options to use
//  * @param {Object|undefined} [event] - the AWS event, which was passed to your lambda
//  * @param {Object|undefined} [awsContext] - the AWS context, which was passed to your lambda
//  * @returns {DRQConsumerContext|DRQProcessing} the given context object configured with full or partial DRQ stream consumer settings
//  * @throws {Error} an error if event and awsContext are specified and the region and/or stage cannot be resolved
//  */
// function configureDefaultDeadRecordQueueConsumer(context, options, event, awsContext) {
//   const defaultOptions = require('./default-drq-options.json');
//   const drqConsumerOptions = options && typeof options === 'object' ?
//     merge(defaultOptions, copy(options, deep)) : defaultOptions;
//   return configureDeadRecordQueueProcessing(context, undefined, drqConsumerOptions.streamProcessingOptions, undefined,
//     drqConsumerOptions, event, awsContext, false);
// }
//
// /**
//  * Configures the given context with the given DRQ stream processing settings (if any) otherwise with the default DRQ
//  * stream processing settings partially overridden by the given DRQ stream processing options (if any), but only if
//  * stream processing is not already configured on the given context OR if forceConfiguration is true, and with the given
//  * standard settings and options.
//  *
//  * Note that if either the given event or AWS context are undefined, then everything other than event, AWS context and
//  * stage will be configured. This missing configuration must be configured before invoking consumeDeadRecords by invoking
//  * {@linkcode module:aws-core-utils/contexts#configureEventAwsContextAndStage}. This separation of configuration is
//  * primarily useful for unit testing.
//  *
//  * @param {Object|DRQProcessing|DRQConsumerContext} context - the context to be configured with DRQ stream processing settings
//  * @param {DRQProcessingSettings|undefined} [settings] - optional stream processing settings to use to configure stream processing
//  * @param {DRQProcessingOptions|undefined} [options] - optional stream processing options to use to override default options
//  * @param {StandardSettings|undefined} [standardSettings] - optional standard settings to use to configure stream processing dependencies
//  * @param {StandardOptions|undefined} [standardOptions] - optional standard options to use to configure stream processing dependencies if corresponding settings are not provided
//  * @param {Object|undefined} [event] - the AWS event, which was passed to your lambda
//  * @param {Object|undefined} [awsContext] - the AWS context, which was passed to your lambda
//  * @param {boolean|undefined} [forceConfiguration] - whether or not to force configuration of the given settings, which
//  * will override any previously configured DRQ stream processing settings on the given context
//  * @return {DRQProcessing|DRQConsumerContext} the given context configured with DRQ stream processing settings
//  */
// function configureDeadRecordQueueProcessing(context, settings, options, standardSettings, standardOptions, event, awsContext, forceConfiguration) {
//   const settingsAvailable = settings && typeof settings === 'object';
//   const optionsAvailable = options && typeof options === 'object';
//
//   // Check if stream processing was already configured
//   const streamProcessingWasConfigured = streamProcessing.isStreamProcessingConfigured(context);
//
//   // Determine the stream processing settings to be used
//   const defaultSettings = getDefaultDRQProcessingSettings(options);
//
//   const streamProcessingSettings = settingsAvailable ?
//     merge(defaultSettings, settings) : defaultSettings;
//
//   // Configure stream processing with the given or derived stream processing settings
//   // streamConsumer.configureStreamConsumer(context, settings, options, event, awsContext);
//   //
//   streamProcessing.configureStreamProcessingWithSettings(context, streamProcessingSettings, standardSettings,
//     standardOptions, event, awsContext, forceConfiguration);
//
//   // Log a warning if no settings and no options were provided and the default settings were applied
//   if (!settingsAvailable && !optionsAvailable && (forceConfiguration || !streamProcessingWasConfigured)) {
//     context.warn(`DRQ stream processing was configured without settings or options - used default DRQ stream processing configuration (${stringify(streamProcessingSettings)})`);
//   }
//   return context;
// }
//
// /**
//  * Returns the default DRQ stream processing settings partially overridden by the given stream processing options (if any).
//  *
//  * This function is used internally by {@linkcode configureDefaultDeadRecordProcessing}, but could also be used in
//  * custom configurations to get the default settings as a base to be overridden with your custom settings before calling
//  * {@linkcode configureDeadRecordQueueProcessing}.
//  *
//  * @param {DRQProcessingOptions} [options] - optional DRQ stream processing options to use to override the default options
//  * @returns {DRQProcessingSettings} a DRQ stream processing settings object (including both property and function settings)
//  */
// function getDefaultDRQProcessingSettings(options) {
//   const overrideOptions = options && typeof options === 'object' ? copy(options, deep) : {};
//
//   // Load defaults from local default-drq-options.json file
//   const defaultOptions = loadDefaultDRQProcessingOptions();
//   merge(defaultOptions, overrideOptions);
//
//   // Start with the default Kinesis stream processing settings
//   const defaultSettings = streamProcessing.getDefaultKinesisStreamProcessingSettings(overrideOptions);
//
//   // Customise the discardUnusableRecord function to eliminate any unusable ("deader"), dead records during DRQ stream processing
//   defaultSettings.discardUnusableRecord = eliminateUnusableRecord;
//
//   return defaultSettings;
// }
//
// /**
//  * Loads the default DRQ stream processing options from the local default-drq-options.json file and fills in any missing
//  * options with the static default options.
//  * @returns {DRQProcessingOptions} the default stream processing options
//  */
// function loadDefaultDRQProcessingOptions() {
//   const options = require('./default-drq-options.json');
//   const defaultOptions = options && options.streamProcessingOptions && typeof options.streamProcessingOptions === 'object' ?
//     options.streamProcessingOptions : {};
//
//   const defaults = {
//     // Generic settings
//     streamType: StreamType.kinesis,
//     sequencingRequired: false,
//     sequencingPerKey: false,
//     batchKeyedOnEventID: false,
//     kplEncoded: false,
//     consumerIdSuffix: "",
//     timeoutAtPercentageOfRemainingTime: 0.9,
//     maxNumberOfAttempts: 10,
//     avoidEsmCache: false,
//
//     keyPropertyNames: [],
//     seqNoPropertyNames: [],
//     idPropertyNames: [],
//
//     // Specialised settings needed by default implementations - e.g. DRQ and DMQ stream names
//     batchStateTableName: 'DZ_StreamConsumerBatchState',
//     deadRecordQueueName: 'DZ_DeadRecordQueue',
//     deadMessageQueueName: 'DZ_DeadMessageQueue',
//     deadRecordTableName: 'DZ_DeadRecord',
//
//     taskTrackingName: "drqTaskTracking" // Legacy task tracking name
//   };
//   return merge(defaults, defaultOptions);
// }

// =====================================================================================================================
// Consume dead records
// =====================================================================================================================

/**
 /**
 * Consumes all of the unusable/dead records in the given event by transforming them into dead records and writing them
 * to a DynamoDB dead record table.
 * @param {AnyAWSEvent} event - an AWS event
 * @param {DRQConsumerContext} context - the context configured with DRQ stream consumer runtime settings
 * @returns {Promise.<Batch|BatchError>} a promise that will resolve with the batch processed or reject with an error
 */
function consumeDeadRecords(event, context) {
  // Define the task(s) that must be executed on each unusable record
  const saveDeadRecordTaskDef = TaskDef.defineTask(saveDeadRecord.name, saveDeadRecord);

  // Process the stream event
  return streamConsumer.processStreamEvent(event, [saveDeadRecordTaskDef], [], context);
}

// noinspection JSUnusedLocalSymbols
/**
 * Transforms the given unusable record into a dead record (DeadRecord) and saves it to the DynamoDB dead record table.
 * @param {AnyAWSEventRecord|UnusableRecord} record - an AWS event record to save
 * @param {Batch} batch - the current batch
 * @param {DRQConsumerContext} context - the context configured with DRQ stream consumer runtime settings
 * @returns {*} the DynamoDB put result
 */
function saveDeadRecord(record, batch, context) {
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
      task.succeed(result, {overrideTimedOut: false}, false);
      return result;
    })
    .catch(err => {
      context.error(`DRQ consumer failed to save dead record (${deadRecord.regionEventSourceAndName}, ${deadRecord.keys}) to ${tableName}`, err);
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
 * @param {AnyAWSEventRecord|UnusableRecord|*} record - an AWS event record
 * @returns {DeadRecord} a dead record
 */
function toDeadRecord(record) {
  if (record.unusableRecordWrapper) {
    // Unwrap the unusable record
    record = record.originalRecord;
  }
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
  const keys = `${record.eventID}`; // ${record.kinesis.partitionKey}|${trimOrEmpty(record.kinesis.explicitHashKey)}|


  // Structure of a Kinesis DeadRecord from kinesis-stream-consumer 2.0.x
  // const deadRecord = {
  //   streamConsumerId: batch.streamConsumerId,
  //   shardOrEventID: batch.shardOrEventID,
  //   ver: 'DR|K|2.0',
  //   // eventID: eventID,
  //   // eventSeqNo: eventSeqNo,
  //   // eventSubSeqNo: eventSubSeqNo,
  //   unusableRecord: unusableRecord,
  //   record: record !== unusableRecord ? record : undefined, // don't need BOTH unusableRecord & record if same
  //   userRecord: userRecord !== unusableRecord ? userRecord : undefined, // don't need BOTH unusableRecord & userRecord if same
  //   state: state,
  //   reasonUnusable: reasonUnusable,
  //   discardedAt: new Date().toISOString()
  // };

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
 * Eliminates the given UNUSABLE dead record by simply logging it and doing nothing more with it.
 *
 * Ideally this function should NEVER be executed, since there should be NO UNUSABLE dead records at this point in the
 * flow and because this Lambda is already consuming dead records from the DRQ, there is effectively nowhere left to
 * which to discard unusable, dead records (i.e. there is no "Deader" Dead Record Queue).
 *
 * @param {Record|UnusableRecord} unusableRecord - an UNUSABLE dead record to throw away
 * @param {Batch} batch - the current batch of records/messages being processed
 * @param {DRQConsumerContext} context - the context configured with DRQ stream consumer runtime settings
 * @returns {Promise} the given array of unusable records
 */
function eliminateUnusableRecord(unusableRecord, batch, context) {
  if (!unusableRecord) {
    // Ideally should be none
    return Promise.resolve(undefined);
  }
  context.warn(`DRQ consumer is eliminating an unusable ("deader"), dead record - ${unusableRecord.eventID} from batch (${batch.shardOrEventID})`);
  return Promise.resolve(unusableRecord);
}

