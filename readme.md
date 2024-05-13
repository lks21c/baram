# Baram

Python based AWS Framework which leverages `boto3` and `awswrangler`.

Baram means 'wind' in Korean which makes cloud move conveniently.

## Features

- TBD
- Convenient S3 Usage(KMS setting, delete directory ...)
- Athena Iceberg
- Athena Performance Management(cache, ctas_approach control)
- Glue Job Management

## Quick Start

```bash
> pip install awswrangler
```

## S3

To import `S3Manager`, all you have to do is just initialize `S3Manager` with your bucket name.

```python
from baram.s3_manager import S3Manager

sm = S3Manager('baram-test')
```
### Automatic KMS key Setting

Once initialized, `S3Manager` will automatically set KMS key for you.

You don't have to manually configure KMS for S3 actions such as putting object, etc.

### Delete directory with objects

Suppose your bucket has a directory with 3 objects.

![image](https://github.com/lks21c/baram/assets/3079144/81185772-7a67-43bb-83a2-a652f2a6e3d0)

You can delete the directory including all subdirectories and objects with just one line of code.

```python
# import S3Manager
from baram.s3_manager import S3Manager

sm = S3Manager('baram-test')

# delete S3 directory
sm.delete_dir('dir_path')
```

You can see output logs like below.

```bash
2024-05-03 13:10:26,512 - S3Manager - INFO - delete 3 keys.
2024-05-03 13:10:26,569 - S3Manager - INFO - delete dir
```

You can verify the deletion in the S3 console.

![image](https://github.com/lks21c/baram/assets/3079144/7d7b7c9c-a283-4b94-9b59-105ee8394946)

### Basic file upload and download

You can upload and download files like below.

```python
# import S3Manager
from baram.s3_manager import S3Manager

sm = S3Manager('baram-test')

# upload local file to S3
sm.upload_file(local_file_path='local_file_path',
               s3_file_path='s3_file_path')

# download directory to local
sm.download_dir(s3_dir_path='s3_directory_path',
                local_dir_path='local_directory_path')

# copy S3 object
sm.copy_object(from_key='from_s3_key',
               to_key='to_s3_key')
```

### Check line count of csv

It's often necessary to check how many lines are in csv file in S3.

You can easily get the line count with the following code.

```python
# import S3Manager
from baram.s3_manager import S3Manager

sm = S3Manager('baram-test')

# count the number of rows in csv
cnt = sm.count_csv_row_count('directory/filename.csv')
print(cnt)
```

Below is the output.
```bash
891
```

### Get object by lines

You can easily read csv file by lines.

```python
# import S3Manager
from baram.s3_manager import S3Manager

sm = S3Manager('baram-test')

# get S3 object line by line
for line in sm.get_object_by_lines('dir/filename.csv'):
    print(line)
```

### Manipulate csv files

You can easily manipulate csv files in S3.

```python
# import S3Manager
from baram.s3_manager import S3Manager

sm = S3Manager('baram-test')

# read csv file from S3 and return as pandas DataFrame
df = sm.read_csv_from_s3(s3_file_path='dir/train.csv')

# write pandas DataFrame as csv file to S3
sm.write_csv_to_s3(df=df, s3_file_path='dir/train2.csv')
```

### Merging csv files

```python
from baram.s3_manager import S3Manager

sm = S3Manager('baram-test')
sm.merge_datasets(source_path='source_path',
                  target_path='target_path')
```

### Rename File

If you want to rename a file in S3, you can do it like below.

![image](https://github.com/lks21c/baram/assets/3079144/81185772-7a67-43bb-83a2-a652f2a6e3d0)

```python

from baram.s3_manager import S3Manager

sm = S3Manager('baram-test')
sm.rename_file('dir/train.csv', 'dir/train2.csv')

```
`dir/train.csv` is renamed to `dir/train2.csv`

![image](https://github.com/lks21c/baram/assets/3079144/6ed7c415-3b4a-4a9e-b68a-35fb12276898)

---

## Athena
To handle AWS Athena with `baram`, you'll use `S3Manager`, `AthenaManager` and `GlueManager`. Each class needs below arguments.
#### 1. `S3Manager`
- `bucket_name`: S3 bucket name where your data is saved (Glue table, csv, etc)
#### 2. `AthenaManager`
- `query_result_bucket_name`: S3 bucket name for saving Athena query results
- `output_bucket_name`: S3 bucket name where table-related data is saved (w.r.t Glue or S3)
- `workgroup`: Athena workgroup, default is `primary`
#### 3. `GlueManager`
- `s3_bucket_name`: S3 bucket name for Glue

### List Glue catalog and table
```python
from pprint import pprint

from baram.glue_manager import GlueManager


gm = GlueManager(bucket_name='baram-test')

glue_catalogs = gm.get_glue_databases
tables = gm.get_tables(db_name=glue_catalogs[0])
```

### Check whether Athena table exists or not
```python
from baram.athena_manager import AthenaManager


am = AthenaManager(query_result_bucket_name='baram-test',
                   output_bucket_name='baram-test')
db_name = 'foo_db'
table_name = 'bar'

am.check_table_exists(db_name=db_name, table_name=table_name)
```

### Delete old table and remake it
```python
from baram.athena_manager import AthenaManager


am = AthenaManager(query_result_bucket_name='baram-test',
                   output_bucket_name='baram-test')
db_name = 'foo_db'
old_table_name = 'bar_old'
new_table_name = 'bar_new'

if am.check_table_exists(db_name=db_name, table_name=old_table_name):
    am.delete_table(db_name=db_name, table_name=old_table_name)

am.fetch_query()
```

### Fetch CTAS query to Athena
```python
from baram.athena_manager import AthenaManager


am = AthenaManager(query_result_bucket_name='baram-test',
                   output_bucket_name='baram-test')
db_name = 'foo_db'
table_name = 'bar'
sql = f'create table foo as (select * from {db_name}.{table_name})'

am.fetch_query(sql=sql, db_name=db_name)
```

### Fetch parameterized query to Athena (TBD)
```python
from baram.athena_manager import AthenaManager


am = AthenaManager(query_result_bucket_name='baram-test',
                   output_bucket_name='baram-test')
db_name = 'foo_db'
table_name = 'bar'
```

### Bring Athena table as `pandas.DataFrame`
```python
from baram.athena_manager import AthenaManager


am = AthenaManager(query_result_bucket_name='baram-test',
                   output_bucket_name='baram-test')
db_name = 'foo_db'
table_name = 'bar'
sql = f'select * from {db_name}.{table_name} where crit=foo'

df = am.from_athena_to_df(sql=sql, db_name=db_name)
```

### Read specific query from text file and fetch it to Athena
```python
from baram.athena_manager import AthenaManager


am = AthenaManager(query_result_bucket_name='baram-test',
                   output_bucket_name='baram-test')

db_name = 'foo_db'
sql = am.read_query_txt('directory/sql.txt')
am.fetch_query(sql=sql, db_name=db_name)

```

## Read The Docs

- [How to import Baram in Glue](how_to_import_baram_in_glue.md)
- [How to import Baram in SageMaker](how_to_import_baram_in_sagemaker.md)
- [S3 Usage with Baram](TBD)
- [Athena Usage with Baram](TBD)
