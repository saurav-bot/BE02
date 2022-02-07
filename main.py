import boto3
import json
import env


#Publish message 
def sns():
    topicArn = env.topic 
    
    snsClient = boto3.client(
        'sns',
        aws_access_key_id = env.accessKey,
        aws_secret_access_key = env.secretKey,
        region_name = 'us-east-1'
    )

    publishObject = {'transactionId': 123, "amount":10000000.00}
    response = snsClient.publish(TopicArn = topicArn,
                                Message = json.dumps(publishObject),
                                Subject = "PURCHASE",
                                MessageAttributes = {"TransactionType": {"DataType" : "String", "StringValue" : "PURCHASEddddd"}})

    print(response)


def message_queue():
    sqs = boto3.resource('sqs',
    aws_access_key_id = env.accessKey,
        aws_secret_access_key = env.secretKey,
        region_name = 'us-east-1')

    queue = sqs.get_queue_by_name(QueueName = 'pacto')

    print(queue.url)

    for message in queue.receive_messages():
        print(message.body)
        message.delete()
        

# Broadcast message to every subscriber
sns()

#Fetch  message from sqs queue
message_queue()

