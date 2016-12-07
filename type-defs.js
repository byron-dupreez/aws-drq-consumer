'use strict';

/**
 * @typedef {StreamConsuming} DRQConsuming - an object with DRQ stream consumer configuration, including DRQ stream
 * processing settings, stage handling settings, logging functionality, a region, a stage, an AWS context, an AWS.Kinesis
 * instance and an AWS.DynamoDB.DocumentClient instance
 * @property {string} streamProcessing.deadRecordTableName - the name of the DynamoDB dead record table to which to save dead records
 * @property {AWS.DynamoDB.DocumentClient} dynamoDBDocClient - an AWS.DynamoDB.DocumentClient instance to use
 */

/**
 * @typedef {StreamConsumerSettings} DRQConsumerSettings - DRQ stream consumer configuration settings to use
 * @property {DRQProcessingSettings|undefined} [streamProcessingSettings] - optional settings to use to configure stream processing
 */

/**
 * @typedef {StreamConsumerOptions} DRQConsumerOptions - DRQ stream consumer configuration options to use if no corresponding settings are provided
 * @property {DRQProcessingOptions|undefined} [streamProcessingOptions] - optional options to use to configure DRQ stream processing
 */

/**
 * @typedef {StreamProcessing} DRQProcessing - an object configured with DRQ stream processing settings, stage handling
 * settings, logging functionality, an AWS.Kinesis instance and an AWS.DynamoDB.DocumentClient
 * @property {string} streamProcessing.deadRecordTableName - the name of the DynamoDB dead record table to which to save dead records
 * @property {AWS.DynamoDB.DocumentClient} dynamoDBDocClient - an AWS.DynamoDB.DocumentClient instance to use
 */

/**
 * @typedef {StreamProcessingSettings} DRQProcessingSettings - DRQ stream processing settings to use
 * @property {string|undefined} [deadRecordTableName] - optional DynamoDB dead record table name (defaults to 'DeadRecord' if undefined)
 */

/**
 * @typedef {StreamProcessingOptions} DRQProcessingOptions - DRQ stream processing options to use if no corresponding settings are provided
 * @property {string|undefined} [deadRecordTableName] - optional DynamoDB dead record table name (defaults to 'DeadRecord' if undefined)
 */

/**
 * @typedef {Object} DeadRecord - represents a common structure for storing an unusable record as a dead record
 * @property {string} regionEventSourceAndName - the AWS region (if available, else "aws"), the event source (e.g.
 * kinesis, ses, ...) and a selected event name, which varies by event type
 * @property {string} keys - a string containing selected, pipe-separated ('|') key-related values to provide a range key value
 * @property {Object} record - the original unusable record
 */
