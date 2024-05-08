# How to import baram in AWS Sagemaker
- This document demonstrates how to import and use `baram` in AWS Sagemaker studio.

## 1. Create Jupyter notebook
- We will use Jupyter notebook environment for demonstration.
- To begin, create Jupyter notebook by clicking `File > New > Notebook` in your personal Sagemaker studio.

![img](https://github.com/lks21c/baram/assets/114719043/60bbd3a5-3227-488b-91cc-e677ceeb8e57)

## 2. Install `baram` with `pip`
- Run following command in Jupyter notebook code cell to install `baram` with `pip`.
```bash
!pip install baram
```
- Then `baram` will be installed to your notebook environment.
- Be noted that you have to reinstall `baram` when notebook kernel has been re-started or deleted.

![image](https://github.com/lks21c/baram/assets/114719043/11e376f7-901c-4c58-83ad-72defeef0a10)

## 4. Write and run code using `baram`
- After installation is complete, you can now import and use `baram` in Jupyter notebook.
- Now we will import `S3Manager` and use its methods for demonstration.

### 4-1. List objects in specific directory
- We ran below code to list objects in `baram-test` bucket's `dir` directory.

```python
from baram.s3_manager import S3Manager

sm = S3Manager('baram-test')

objects = sm.list_objects(prefix='dir')
print(objects)
```
- Below is the output showing all objects in that directory.
````python
[{'ETag': '"d41d8cd98f00b204e9800998ecf8427e"',
  'Key': 'dir/',
  'LastModified': datetime.datetime(2024, 5, 3, 4, 21, 3, tzinfo=tzlocal()),
  'Size': 0,
  'StorageClass': 'STANDARD'},
 {'ETag': '"30d1073b964a5d815431ec603a0de353"',
  'Key': 'dir/gender_submission.csv',
  'LastModified': datetime.datetime(2024, 5, 3, 4, 21, 7, tzinfo=tzlocal()),
  'Size': 3258,
  'StorageClass': 'STANDARD'},
 {'ETag': '"2309cc5f04782ed9bb6016d9f4e381cf"',
  'Key': 'dir/train.csv',
  'LastModified': datetime.datetime(2024, 5, 7, 14, 0, 20, tzinfo=tzlocal()),
  'Size': 61194,
  'StorageClass': 'STANDARD'}]
````

![img_2](https://github.com/lks21c/baram/assets/114719043/e5945219-ab9e-4733-9150-4e30f84aee85)

### 4-2. Download file from S3 to Sagemaker
- You can download file from S3 bucket to your Sagemaker directory.
- As you can see, there is only one file, `import_baram.ipynb`, in this Sagemaker directory.

![image](https://github.com/lks21c/baram/assets/114719043/8e9b4355-809f-42d6-aa7d-e1437ce7b649)

- We will download a file named `train.csv` from `baram-test` bucket's `dir` directory with following code.
````python
from baram.s3_manager import S3Manager

sm = S3Manager('baram-test')

sm.download_file(s3_file_path='dir/train.csv',
                 local_file_path='/root/train.csv')
````
- Once we run the code, below output is printed.
```python
2024-05-07 14:39:19,163 - S3Manager - INFO - download : dir/train.csv to Target: /root/train.csv Success.
```
- We can see that file named `train.csv` is successfully downloaded to Sagemaker directory.

![image](https://github.com/lks21c/baram/assets/114719043/67860157-059d-486c-97a4-32282d0fa828)
