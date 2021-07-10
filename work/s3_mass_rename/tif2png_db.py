import boto3

def update_entry(table_name, key, field, new_val):
    try:
        table = dynamodb.Table(table_name)
        response = table.update_item(
            Key=key,
            UpdateExpression=f'set {field} = :r',
            ExpressionAttributeValues={
                ':r': new_val
            },
            ReturnValues="UPDATED_NEW"
        )
    except Exception as e:
        print(f'There was an issue updating {field} for {table_name} with {new_val} \n{e}')
        return 1

    return response

tables = ['EnrichmentHistoryTable', 'EnrichmentCorrectionTable']

for table_name in tables:
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    response = table.scan(
            TableName=table_name
            )
    print(response['Items'])
    data = response['Items']
    while response.get('LastEvaluatedKey'):
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])


    for item in data:
        if item["Assessing"]:
            print(f'Looking at {item["Assessing"]}')
            assessing = item['Assessing']
            assessing = assessing.split('/')
            assessing[0] = 'BrightAssessing'
            assessing[1] = assessing[1].replace('.tif','.png')

            print('/'.join(assessing))
            update_entry(table_name, {'Entry_ID': item['Entry_ID']}, "Assessing", '/'.join(assessing))
        else:
            print("Emtpy Assessing")

        print(f'Looking at {item["PseudoColor"]}')
        pseudocolor = item['PseudoColor']
        pseudocolor = pseudocolor.split('/')
        pseudocolor[0] = 'BrightPseudo'
        pseudocolor[1] = pseudocolor[1].replace('.tif','.png')

        print('/'.join(pseudocolor))
        update_entry(table_name, {'Entry_ID': item['Entry_ID']}, "PseudoColor", '/'.join(pseudocolor))
        





