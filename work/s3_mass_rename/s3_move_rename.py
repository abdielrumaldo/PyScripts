import boto3
from PIL import Image
import io
import os
import pandas as pd
import botocore

buckets = ['awcm', 'deersfp', 'medstardc', 'nolaepoc', 'ocer', 'omwa', 'wakeforest', 'whfa']
folder_names = {'Assessing' : 'BrightAssessing',
                'PseudoColor': 'BrightPseudo'}

broken_files = []
# Settin up clients
s3_resource = boto3.resource('s3')
s3_cilent = boto3.client('s3')

def get_all_s3_resource_objects(s3_resource, **base_kwargs):
    continuation_token = None
    while True:
        # Set call input
        list_kwargs = dict(MaxKeys=1000, **base_kwargs)

        # Continue call
        if continuation_token:
            list_kwargs['ContinuationToken'] = continuation_token

        # Return the file
        response = s3_resource.list_objects_v2(**list_kwargs)
        yield from response.get('Contents', [])

        # EOF
        if not response.get('IsTruncated'):  # At the end of the list?
            break

        continuation_token = response.get('NextContinuationToken')


for bucket_name in buckets:
    # For every bucket we want to set the bucket resource
    my_bucket = s3_resource.Bucket(bucket_name)
    print(f'\n\n%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Starting Bucket {bucket_name} %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% \n\n')

    for bucket_location_prefix, bucket_destination_prefix in folder_names.items():
        # for every folder input name and output name
        for file in get_all_s3_resource_objects(s3_cilent, Bucket=bucket_name, Prefix=bucket_location_prefix):
            # Break up the file name
            file_name=file['Key']
            split_file_name=file_name.split('/')

            # If it's a pseudocolor .tif image 
            if ((split_file_name[-1].endswith('.tif')) and (~split_file_name[-1].startswith('Pseudo'))):

                # Not sure what this is for
                rd=split_file_name[-1].replace('.0','')
                # Change the file name
                name=split_file_name[-1].replace('.tif','.png')
                # Check to see if the output already exists
                try:
                    print(f'Checking to see if {bucket_destination_prefix}/{name} exists')
                    my_bucket.Object(f'{bucket_destination_prefix}/{name}').load()
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        # If the file does not exists; we can perform the operation

                        # Dowload the file
                        file_stream = io.BytesIO()
                        image=my_bucket.Object(file_name)
                        image.download_fileobj(file_stream)
                        
                        # Change image settings and save as PNG
                        try:
                            im = Image.open(file_stream)
                            im.save('D:/pngs/'+name, "PNG", quality=95,compress_level=1)
                            # Clear file stream
                            file_stream.seek(0)
                            im = im
                        except Exception as e:
                            print(f"There was an issue with file: {file_name} > in {bucket_name}")
                            print(e)
                            broken_files.append(f'{bucket_name}\t{file_name}')
                            continue

                        # Upload to our output location from the above 'folder_names' mapping 
                        s3_resource.meta.client.upload_file('D:/pngs/'+name, bucket_name, bucket_destination_prefix+'/'+name)
                        print(f'File Complete:\t {name}')

                        # Remove file 
                        os.remove(f'D:/pngs/{name}')
                else:
                    print(f'File Already Exists:\t {bucket_destination_prefix}/{name}')
    print(f'\n\n%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Bucket {bucket_name} Completed %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% \n\n')

print(f'Issues with: {broken_files}')