## Changes

### 2.0.0-beta.1
- Switched to use new `kinesis-stream-consumer` instead of old `aws-stream-consumer`

### 1.0.0
- Updated `core-functions` dependency to version 3.0.0
- Updated `logging-utils` dependency to version 4.0.0
- Updated `task-utils` dependency to version 5.0.0

### 1.0.0-beta.6
- Changed `drq-consumer` module to use new `generateHandlerFunction` from `aws-stream-consumer/stream-consumer` module
- Updated `core-functions` dependency to version 2.0.12
- Updated `logging-utils` dependency to version 3.0.10
- Updated `task-utils` dependency to version 4.0.6
- Updated `aws-core-utils` dependency to version 5.0.16

### 1.0.0-beta.5
- Changes to `type-defs.js` module:
  - Renamed `DRQConsuming` typedef to `DRQConsumerContext` & changed it to extend from `DRQProcessing`
- Renamed `drq-options.json` to `default-drq-options.json`
- Changes to `drq-consumer.js` module:
  - Removed `configureDefaultDeadRecordQueueProcessing` function
  - Added `configureDefaultDeadRecordQueueConsumer` function
  - Changes to `configureDeadRecordQueueProcessing` function:
    - Renamed its `otherSettings` & `otherOptions` arguments to `standardSettings` & `standardOptions` respectively
    - Added optional `event` & `awsContext` arguments
  - Fixes to synchronize with changes in `aws-stream-consumer` modules & `aws-core-utils` modules  
- Updated `aws-stream-consumer` dependency to version 1.0.0-beta.16
- Updated `aws-core-utils` dependency to version 5.0.12
- Updated `logging-utils` dependency to version 3.0.9

### 1.0.0-beta.4
- Fixed broken unit tests by changing incorrect imports of `node-uuid` to `uuid`
- Added missing return type to `configureDeadRecordQueueConsumer` function
- Moved all typedefs from `drq-consumer.js` module to new `type-defs.js` module
- Added new typedefs and renamed some of existing typedefs
- Changed argument and return types of multiple functions to use new and existing typedefs
- Updated `aws-stream-consumer` dependency to version 1.0.0-beta.14
- Updated `aws-core-utils` dependency to version 5.0.10
- Updated `logging-utils` dependency to version 3.0.8

### 1.0.0-beta.3
- Changed `drq-consumer.js` module:
  - Changed `saveDeadRecord` function to NOT override an existing timed out task state when succeeding its task
- Updated `core-functions` dependency to version 2.0.11
- Updated `logging-utils` dependency to version 3.0.6
- Updated `task-utils` dependency to version 4.0.5
- Updated `aws-core-utils` dependency to version 5.0.6
- Updated `aws-stream-consumer` dependency to version 1.0.0-beta.12
- Replaced `node-uuid` dependency with `uuid` dependency in `test\package.json`

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