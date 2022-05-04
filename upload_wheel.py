import os

from baram.s3_manager import S3Manager

if __name__ == '__main__':
    sm = S3Manager()
    os.system('poetry update && poetry install && poetry build')
    for f in os.listdir(os.path.join(os.getcwd(), 'dist')):
        if 'whl' in f:
            sm.upload_file('sli-dst-security', os.path.join(os.getcwd(), 'dist', f), f'wheel/{f}')
