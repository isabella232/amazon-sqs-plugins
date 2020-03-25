# Amazon SQS Source

Description
-----------

The source reads messages from SQS Queue using the long polling option of the API. SQS currently does provide ability 
for callbacks or streaming. The plugin will continuously issue a call to read messages off the queue. There are options 
to control the frequency of the poll interval.

Properties
----------
**Reference Name**: Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Authentication Method**: Authentication method to access SQS. The default value is Access Credentials.
IAM can only be used if the plugin is run in an AWS environment, such as on EMR.

**Access ID**: Amazon access ID required for authentication

**Access Key**: Amazon access key required for authentication

**Amazon Region**: The Amazon Region where the SQS queue is located. The default is `us-west-1`.

**Queue Name**: The name of SQS queue to read messages from the plugin.

**SQS Endpoint**: The endpoint of the SQS server to connect to. For example, `https://sqs.us-east-1.amazonaws.com`

**Delete messages after read?** Permanently delete messages from the queue after successfully reading. If set to 
`No`, the messages will stay on queue and will be received again leading to duplicates.

**Interval**: The amount of time to wait between each poll in seconds. The plugin will wait for the duration specified 
before issuing a receive message call.

**Wait Time**: The duration (in seconds) for which the call waits for a message to arrive in the queue before 
returning. This will be set as part of the ReceiveMessage API call action. The maximum wait time currently allowed by 
SQS 20 seconds.

**Number of messages to return**: The maximum number of messages to return from a single API request. The maximum 
value allowed by SQS is 10.

**Format**: Optional format of the SQS message. Any format supported by CDAP is supported. For example, a value of 
`csv` will attempt to parse SQS message as comma-separated values. If no format is given, SQS messages will be 
treated as bytes.

**Schema**: Schema of the data to read.
