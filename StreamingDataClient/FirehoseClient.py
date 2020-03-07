import json
import boto3

session = boto3.Session(profile_name='default')
temperatureClient = session.client('firehose')
#
# with open("sampleTempDataForTutorial.json") as json_file:
#     observations = json.load(json_file)
#     for observation in observations:
#         print(observation)
#         response = temperatureClient.put_record(
#            DeliveryStreamName='temperatureStream',
#            Record={
#                 'Data': json.dumps(observation)
#             }
#         )
#         print(response)

records = []
with open("sampleTempDataForTutorial.json") as json_file:
    observations = json.load(json_file)
    count = 1
    for observation in observations:
        if count % 500 == 0:
            response = temperatureClient.put_record_batch(
                DeliveryStreamName='temperatureStream',
                Records= records
            )
            print(response)
            print(len(records))
            records.clear()
        record = {
            "Data": json.dumps(observation)
        }
        records.append(record)
        count = count + 1

    if len(records) > 0:
        print(len(records))
        response = temperatureClient.put_record_batch(
                DeliveryStreamName='temperatureStream',
                Records= records
            )
        print(response)