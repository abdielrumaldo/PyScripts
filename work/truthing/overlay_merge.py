import io
import re
import json
import boto3
import random
import numpy as np
from PIL import Image
from io import BytesIO
from boto3.dynamodb.conditions import Key, Attr


data = {
    "PrimaryDoctor": "s3://wakeforest/sagetest/output/wakeforest-Subject02-1020Burn1ID418294/manifests/output/output.manifest",
    "SecondaryDoctor": "s3://wakeforest/sagetest/output/wakeforest-Subject02-1020Burn1ID259896/manifests/output/output.manifest"
}

# Resources and Clients
s3_client = s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')


def translate_s3_uri(uri):

    bucket = uri.split('/')[2]
    key = '/'.join(uri.split('/')[3::])

    return bucket, key


def image_from_s3(bucket, key):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    image = bucket.Object(key)
    img_data = image.get().get('Body').read()

    file_name = key.split('/')[-1]
    image_save = Image.open(BytesIO(img_data))

    file_location = f'C:/tmp/{random.randint(0, 10000)}.png'
    image_save.save(file_location)

    # return Image.open(file_location)

    return Image.open(BytesIO(img_data))


def OverlayMask(Mask, PseudoImage, output_shape=(None, None)):

    OG_colorKeyGT = {'Background': [0, 0, 0],
                     'Viable': [153, 102, 51],
                     'First_Degree': [255, 174, 201],
                     'Shallow_Second_Degree': [183, 179, 0],
                     'Deep_Second_Degree': [255, 127, 39],
                     'Third_Degree': [128, 128, 128],
                     'Silvadene': [164, 149, 215],
                     'Woundbed_Epinephrine': [237, 28, 36],
                     'Woundbed_Donor_Site': [163, 73, 164],
                     'Unknown_Category': [19, 255, 0],
                     'other': [255, 255, 255]}

    SageMaker_colorKeyGT = {'Background': [188, 189, 34],
                            'Viable': [148, 103, 189],
                            'First_Degree': [44, 160, 44],
                            'Shallow_Second_Degree': [31, 119, 180],
                            'Deep_Second_Degree': [255, 127, 14],
                            'Third_Degree': [214, 39, 40],
                            'Silvadene': [127, 127, 127],
                            'Woundbed_Epinephrine': [140, 86, 75],
                            'Woundbed_Donor_Site': [227, 119, 194],
                            'Unknown_Category': [19, 255, 0],
                            'other': [255, 152, 150]}

    r = Mask[:, :, 0]
    g = Mask[:, :, 1]
    b = Mask[:, :, 2]

    label = np.zeros((r.shape[0], r.shape[1], 3), dtype=np.float64)

    for key in SageMaker_colorKeyGT.keys():
        # find index1 & index2 & index3 overlay part
        index1 = (r == SageMaker_colorKeyGT[key][0])*1
        index2 = (g == SageMaker_colorKeyGT[key][1])*1
        index3 = (b == SageMaker_colorKeyGT[key][2])*1
        index = index1 + index2 + index3
        final_index = index == 3

        label[final_index, 0] = OG_colorKeyGT[key][0]
        label[final_index, 1] = OG_colorKeyGT[key][1]
        label[final_index, 2] = OG_colorKeyGT[key][2]

        PseudoImage[final_index, 0] = OG_colorKeyGT[key][0]
        PseudoImage[final_index, 1] = OG_colorKeyGT[key][1]
        PseudoImage[final_index, 2] = OG_colorKeyGT[key][2]
    PseudoImage = np.array(PseudoImage, dtype=np.uint8)
    label = np.array(label, dtype=np.uint8)

    return PseudoImage


def get_partition_key_by(index_name, subject_id, subject_field_name, field_names, tablename):
    # Turns 'Entry_ID, Bucket, DataLocation'
    # Into: {'#Entry_ID': 'Entry_ID', '#Bucket': 'Bucket', '#DataLocation': 'DataLocation'}
    attributes = field_names.split(', ')
    converted_field_names = dict([("#" + word, word) for word in attributes])

    table = dynamodb.Table(tablename)
    response = table.query(
        IndexName=index_name,
        ProjectionExpression=', '.join(converted_field_names.keys()),
        ExpressionAttributeNames=converted_field_names,
        KeyConditionExpression=Key(subject_field_name).eq(subject_id)
    )['Items']

    return response


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
        print(
            f'There was an issue updating {field} for {table_name} with {new_val} \n{e}')
        return 1

    return response


def lambda_handler(event, context):

    for doctor in event:
        # Set variables
        raw_location = event[doctor]

        print(f'Looking at {doctor}. Manifest: {raw_location}')

        bucket, key = translate_s3_uri(raw_location)

        extracted_data = re.findall('([0-9]+)', event[doctor])
        job_id = extracted_data[4]

        # Set Object
        manifest_object = s3_client.get_object(
            Bucket=bucket,
            Key=key)

        raw_manifest = manifest_object['Body'].read().decode('utf-8')

        for manifest_line in raw_manifest.splitlines():
            # Load json from manifest
            manifest_json = json.loads(manifest_line)

            for key in manifest_json.keys():
                if 'SageMaker' in key and 'metadata' not in key:
                    job_name = key

            pseudo_bucket, pseudo_key = translate_s3_uri(
                manifest_json['source-ref'])
            mask_bucket, mask_key = translate_s3_uri(manifest_json[job_name])

            print(f'Looking at Pseudo file: {pseudo_key}')
            print(f'Looking at Mask file: {mask_key}')

            print("Loading Pseudo Image")
            pseudo_image = np.asarray(image_from_s3(pseudo_bucket, pseudo_key))
            pseudo_image.setflags(write=1)

            print("Loading Mask Image")
            mask_image = image_from_s3(mask_bucket, mask_key)
            mask_image = np.asarray(mask_image.convert('RGB'))

            print('Generating Truth')
            TruthImage = OverlayMask(
                mask_image, pseudo_image, output_shape=(480, 360))

            TruthImage = Image.fromarray(TruthImage)

            # Upload to S3 under Bucket/Truth
            buffer = io.BytesIO()
            TruthImage.save(buffer, "PNG")
            buffer.seek(0)
            file_name = pseudo_key.split('/')[-1]
            key = f'Truth/Truth_{job_id}_{file_name}'

            s3_client.put_object(
                Bucket=pseudo_bucket,
                Key=key,
                Body=buffer,
                ContentType='image/png',
                )

            buffer.close()

            db_entry = get_partition_key_by(
                "PseudoColor-index", pseudo_key, "PseudoColor", "Entry_ID", "TestEPOCTruthTable")

            for item in db_entry:
                update_entry("TestEPOCTruthTable",
                             {"Entry_ID": item['Entry_ID']},
                             f'{doctor}Truth',
                             key)

            #TruthImage.save(f'C:/tmp/{random.randint(0, 10000)}.png')


if __name__ == "__main__":

    lambda_handler(data, None)
