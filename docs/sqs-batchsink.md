# Amazon SQS Sink

Description
-----------

This sink writes messages to Amazon SQS Queue in CSV or JSON format.

Properties
----------
**Reference Name**: Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Authentication Method**: Authentication method to access SQS. The default value is Access Credentials.
IAM can only be used if the plugin is run in an AWS environment, such as on EMR.

**Access ID**: Amazon access ID required for authentication

**Access Key**: Amazon access key required for authentication

**Amazon Region**: The Amazon Region where the SQS queue is located. The default is `us-west-1`.

**Queue Name**: The name of SQS queue to persist messages from the plugin.

**SQS Endpoint**: The endpoint of the SQS server to connect to. For example, `https://sqs.us-east-1.amazonaws.com`

**Message Format**: Specifies the format of the message published to SQS. Supported formats are `CSV` and `JSON`. 
Default value is `JSON`.

**Delay**: The time, in seconds, for which a specific message is delayed. The minimum value is `0` and maximum 
value is `900`.
