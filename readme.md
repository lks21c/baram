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
from baram.s3_manager import S3Manager

sm = S3Manager('baram-test')
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

# Upload local file to S3
sm.upload_file(local_file_path='local_file_path',
               s3_file_path='s3_file_path')

# Download whole directory to local
sm.download_dir(s3_dir_path='s3_directory_path',
                local_dir_path='local_directory_path')

# Copy S3 object
sm.copy_object(from_key='from_s3_key',
               to_key='to_s3_key')
```

### Check line count of csv

It's often necessary to check how many lines are in csv file in S3.

You can easily get the line count with the following code.

```python
from baram.s3_manager import S3Manager

sm = S3Manager('baram-test')
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

from baram.s3_manager import S3Manager

sm = S3Manager('baram-test')
for line in sm.get_object_by_lines('dir/filename.csv'):
    print(line)
```

### pandas EDA

```python

```

### Merging csv files

```python

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

### List Glue catalog and table
```python

```

### Check whether Athena table exists or not
```python

```

### Delete old table and remake it
```python

```

### Fetch CTAS query to Athena
```python

```

### Bring Athena table as `pandas.DataFrame`
```python

```

### Read specific query from text file and fetch it to Athena


## Read The Docs

- [How to import Baram in Glue](TBD)
- [How to import Baram in SageMaker](TBD)
- [S3 Usage with Baram](TBD)
- [Athena Usage with Baram](TBD)
