# How to import baram in AWS Glue
- This document demonstrates how to import and use `baram` in AWS Glue job.

## 1. Open AWS Glue console
- In this demonstration, we will create glue job with script editor.
- Click `Script editor`, choose `Python shell` as engine then click `Create script` to create job.

(image)

## 2. Enter job parameters to import `baram`
- Move to `Job details` tab and click `Advanced properties` to open dropdown. 
- In `Job parameters` section, you can add external python package that you would like to import.
- Enter below key and value to import `baram` to glue job.
  - Key: `--additional-python-modules`
  - Value: `baram`

(image)
- If you prefer to use specific version of baram, you can specify version by entering value like `baram==0.4.6`.

## 4. Write job script using `baram`
- Now move to `Script` tab to write job script.
- We will import `AthenaManager` to create external table `sample_table` with below code.

```python
from baram.athena_manager import AthenaManager

am = AthenaManager(query_result_bucket_name='baram-test',
                   output_bucket_name='baram-test',
                   workgroup='test-wg')

am.create_external_table(db_name='sample',
                         table_name='sample_table',
                         column_def={'col1': 'string', 'col2': 'int'},
                         location='s3://baram-test/incoming/sample/third/sample_table/once',
                         s3_output='s3://baram-test/test-wg/once/tables/sample_table2',
                         column_comments={'col1': 'column1', 'col2': 'column2'})
```
- When everything is done, click `Save` button in the upper right corner to save it.

(image)

## 5. Run job and check result
- Now we will run this job by clicking `Run` button and wait until `Run status` turns to `Succeeded`.
- We can check in Athena console that `sample_table` is successfully created.

(image)
