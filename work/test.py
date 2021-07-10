import boto3
import re


bucket_name = 'awcm'
key_value = "DataTransfer/AWCM_DFU_SMD2037-004-06-24-2021/SpectralView/SoftwareData.sql"

s3 = boto3.resource('s3')
sql_data = s3.Object(bucket_name, key_value)

def exec_sql_file(sql_file):
    print(f'Executing file {sql_file}')
    statement = ""

    for line in sql_data.get()['Body']._raw_stream:
        line = line.decode("utf-8")

        if re.match(r'--', line):  # ignore sql comment lines
            print(f'Ignoring --> {line}')
            continue
        if re.match(r'[/*]', line):
            print(f'Ignoring --> {line}')
            continue

        if not re.search(r';', line):  # keep appending lines that don't end in ';'
            statement = statement + line
        else:  # when you get a line ending in ';' then exec statement and reset for next statement
            statement = statement + line
            #print "\n\n[DEBUG] Executing SQL statement:\n%s" % (statement)
            try:
                print(f'Executing statement: {statement}')
                # cursor.execute(statement)
            except Exception as e:
                print(f'\n[WARN] MySQLError during execute statement \n\tArgs: {e}')

            statement = ""

exec_sql_file(sql_data)