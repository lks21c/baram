## Baram

Cloud Framework for AWS Framework.

Baram means "wind" in Korean which makes cloud move conveniently.  

## For Framework Developer

You can build and install the package as below.

```commandline

$ ./build.sh
```

## For Data Scientist
### Manage S3
```python
# import S3Manager
from baram.s3_manager import S3Manager

sm = S3Manager("my_bucket_name")

# Upload local file to S3
sm.upload_file(local_file_path="local_file_path",
               s3_file_path="s3_file_path")

# Download S3 directory to local
sm.download_dir(s3_dir_path="s3_directory_path",
                local_dir_path="local_directory_path")

# Copy S3 object
sm.copy_object(from_key="from_s3_key",
               to_key="to_s3_key")
