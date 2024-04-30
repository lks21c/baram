## Baram

Python based AWS Framework which leverages boto3 and awswrangler.

Baram means "wind" in Korean which makes cloud move conveniently.

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

## For Beginner

### S3 Usage

```python
# import S3Manager
from baram.s3_manager import S3Manager

sm = S3Manager("my_bucket_name")

# Upload local file to S3
sm.upload_file(local_file_path="local_file_path",
               s3_file_path="s3_file_path")

# Emphasize Directory Deletion
sm.download_dir(s3_dir_path="s3_directory_path",
                local_dir_path="local_directory_path")

# Copy S3 object
sm.copy_object(from_key="from_s3_key",
               to_key="to_s3_key")

```

## For Data Scientist

### S3 Usage

```python

# Read csv from s3

# EDA

# Merging Datasets from S3(https://aws-sdk-pandas.readthedocs.io/en/stable/tutorials/013%20-%20Merging%20Datasets%20on%20S3.html)

# write dataframe to s3

```

### Athena Usage

```python

# Read rows from Athena

# EDA

# write dataframe to Another Athena Table without schema input.

# Batching(Good for restricted memory environments), https://aws-sdk-pandas.readthedocs.io/en/stable/tutorials/006%20-%20Amazon%20Athena.html

# big table dump to s3

```

## For Data Engineer

### Manage S3

```python

# check_s3_object_exists

# count_csv_row_count

# rename_file

```

## Read The Docs

- [How to import Baram in Glue](TBD)
- [How to import Baram in SageMaker](TBD)
- [S3 Usage with Baram](TBD)
- [Athena Usage with Baram](TBD)