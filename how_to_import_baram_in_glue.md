# How to import baram in AWS Glue
- This document demonstrates how to import and use `baram` in AWS Glue job.

## 1. Create glue job
- In this demonstration, we will create glue job with Glue console's script editor.
- Open Glue console and click `Script editor` in `Create job` section.

![image](https://github.com/lks21c/baram/assets/114719043/94d81c69-f1f1-4950-b63d-50cf4adc873e)

- Choose `Python shell` as engine then click `Create script`.

![image](https://github.com/lks21c/baram/assets/114719043/a1c63354-75bc-49fc-ba87-822484ae4473)

## 2. Enter job parameters to import `baram`
- Navigate to `Job details` tab and click `Advanced properties` to open dropdown. 
- In `Job parameters` section, you can add external python package that you would like to import.
- Enter below key and value to import `baram` to glue job.
  - `Key`: `--additional-python-modules`
  - `Value`: `baram`
- If you prefer to use specific version of baram, you can specify version by entering value like `baram==0.4.5`.

![image](https://github.com/lks21c/baram/assets/114719043/b399fc4e-3bd7-47f8-8efe-5196b29ca658)

## 3. Write job script using `baram`
- Now Navigate to `Script` tab to write job script.
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
                         s3_output='s3://baram-test/test-wg/once/tables/sample_table',
                         column_comments={'col1': 'column1', 'col2': 'column2'})
```
- When finished, click `Save` in the upper right corner to save it.

![image](https://github.com/lks21c/baram/assets/114719043/4223f37a-16b6-40eb-bb34-e4be4d3b9506)

## 4. Run job and check result
- Now we will run this job by clicking `Run` and wait until `Run status` turns to `Succeeded`.

![image](https://github.com/lks21c/baram/assets/114719043/cc43705e-695f-4ee7-905b-222418ebc622)

- We can check in Athena console that `sample_table` has been successfully created.

![image](https://github.com/lks21c/baram/assets/114719043/18a90568-d9f2-4b5c-8b4c-5192a74f9b03)
