from pprint import pprint
from typing import Optional, Union, Dict, Any, List, Literal

import fire
import boto3
import awswrangler as wr
from awswrangler.athena._utils import _QUERY_WAIT_POLLING_DELAY

from baram.s3_manager import S3Manager
from baram.log_manager import LogManager
from baram.glue_manager import GlueManager


class AthenaManager(object):
    def __init__(self,
                 query_result_bucket_name: str,
                 output_bucket_name: str,
                 workgroup: str = 'primary'):
        self.logger = LogManager.get_logger()
        self.QUERY_RESULT_BUCKET = query_result_bucket_name
        self.OUTPUT_BUCKET = output_bucket_name
        self.ATHENA_WORKGROUP = workgroup

    def create_external_table(self,
                              db_name: str,
                              table_name: str,
                              column_def: dict,
                              location: str,
                              s3_output: str,
                              column_comments: Optional[dict] = None,
                              table_comment: Optional[str] = None,
                              partition_cols: dict = None):
        '''
        Create Glue External Table with s3 file

        :param db_name: target glue database name
        :param table_name: target glue table name
        :param column_def: definition for columns. each key means column name, and its value means column type
        :param location: s3 location of data for table
        :param s3_output: s3 location for query saving
        :param column_comments: comments for columns. each key means column name, and its value means comment
        :param table_comment: comment for table
        :param partition_cols: same with column_def when do partitioning, None when don't.
        :return:
        '''
        columns = ', '.join([f"{k} {column_def[k]} comment '{column_comments[k]}'"
                             if k in column_comments.keys() else f"{k} {column_def[k]}" for k in column_def])
        partitions = 'partitioned by (' + ', '.join([f"{k} {partition_cols[k]}"
                                                    for k in partition_cols]) + ')' if partition_cols else ''

        sql = f"create external table if not exists {db_name}.{table_name}("\
              f"{columns}) "\
              f"comment '{table_comment}' "\
              f"{partitions} " \
              f"row format delimited fields terminated by ',' "\
              f"stored as textfile "\
              f"location '{location}' "\
              f"tblproperties ('classification'='csv', 'skip.header.line.count'='1');"

        self.fetch_query(sql=sql,
                         db_name=db_name,
                         s3_output=s3_output)

    def create_iceberg_table_with_query(self):
        # TODO
        pass

    def create_iceberg_table_with_dataframe(self):
        # TODO
        pass

    def delete_table(self, db_name: str, table_name: str):
        '''
        Delete Glue Table.

        :param db_name: target glue database name
        :param table_name: target glue table name
        :param table_path:
        :return:
        '''
        sm = S3Manager(self.OUTPUT_BUCKET)
        gm = GlueManager(self.OUTPUT_BUCKET)

        try:
            table = gm.get_table(db_name=db_name, table_name=table_name)
            location = table['StorageDescriptor']['Location'].replace(f's3://{gm.s3_bucket_name}/', '')

            wr.catalog.delete_table_if_exists(database=db_name, table=table_name)
            print(f'{db_name}.{table_name} is deleted, on athena')

            sm.delete_dir(s3_dir_path=location)
            print(f'data of {table_name} in its location {location} is deleted, on s3')

        except Exception as e:
            self.logger.info(e)
            raise e

    def fetch_query(self,
                    sql: str,
                    db_name: Optional[str] = None,
                    params: Union[Dict[str, Any], List[str], None] = None,
                    paramstyle: Literal['qmark', 'named'] = 'qmark',
                    s3_output: Optional[str] = None,
                    athena_query_wait_polling_delay: float = _QUERY_WAIT_POLLING_DELAY):
        '''
        Fetch query result.

        :param sql: sql
        :param db_name: database name
        :param params: for parametrized query.
                       This should be dictionary for "named" paramstyle and list for "qmark" paramstyle.
        :param paramstyle: "named" or "qmark"
        :param s3_output: You can choose output bucket for query result. default is workgroup s3 bucket.
        :param athena_query_wait_polling_delay: float, default: 0.25 seconds
        Interval in seconds for how often the function will check if the Athena query has completed.
        :return: Dictionary with the get_query_execution response. You can obtain query result as csv on S3.
        '''
        pprint(sql)
        query_execution_id = wr.athena.start_query_execution(sql=sql,
                                                             workgroup=self.ATHENA_WORKGROUP,
                                                             params=params,
                                                             paramstyle=paramstyle,
                                                             s3_output=s3_output,
                                                             database=db_name)

        res = wr.athena.wait_query(query_execution_id=query_execution_id,
                                   athena_query_wait_polling_delay=athena_query_wait_polling_delay)

        arr = str(res['ResultConfiguration']['OutputLocation']).replace('s3://', '').split('/')

        sm = S3Manager(self.QUERY_RESULT_BUCKET)
        print(f"fetch_result_path={sm.get_s3_web_url(arr[0], '/'.join(arr[1:]))}")
        return res

    def count_rows_from_table(self, db_name: str, table_name: str):
        '''
        Return rows from table.

        :param table_name:
        :return:
        '''
        df = self.from_athena_to_df(sql=f'SELECT count(*) as cnt FROM {table_name}', db_name=db_name)
        return int(df['cnt'])

    def optimize_and_vacumm_iceberg_table(self, db_name: str, table_name: str):
        '''
        Optimize and Vacumm iceberg table in database.

        :param db_name:
        :param table_name:
        :return:
        '''

        print(f"{table_name} optimize start")
        self.optimize_table(table_name, db_name)

        print(f"{table_name} vacumm start")
        self.vacumm_table(table_name, db_name)

    def optimize_iceberg_table(self, db_name: str, table_name: str):
        '''
        Optimize iceberg table.

        :param db_name: database name
        :param table_name: table name
        :return:
        '''

        return self.fetch_query(f"OPTIMIZE {table_name} REWRITE DATA USING BIN_PACK", db_name)

    def vacumm_iceberg_table(self, db_name: str, table_name: str):
        '''
        Vacumm iceberg table.

        :param db_name: database name
        :param table_name: table name
        :return:
        '''

        return self.fetch_query(f"VACUUM {table_name}", db_name)

    def check_table_exists(self, db_name: str, table_name: str):
        '''
        Return table exists or not.

        :param db_name: database name
        :param table_name: table name
        :return:
        '''
        return wr.catalog.does_table_exist(db_name, table_name)

    def read_query_txt(self,
                       bucket_name: str,
                       filepath: str,
                       replacements: Optional[dict] = None):
        '''
        Read txt sql file from s3 and fetch it via Athena.

        :param bucket_name: the name of s3 bucket containing file
        :param filepath: prefix of file
        :param replacements: specified replacements for specific purpose of query
        :return: string, a line of query
        '''
        pass

    def from_athena_to_df(self, sql: str, db_name: str, workgroup: Optional[str] = None):
        '''
        run query and return data frame.

        :param sql: sql
        :param db_name: database name
        :param workgroup: athena workgroup
        :return:
        '''
        workgroup = workgroup if workgroup else self.ATHENA_WORKGROUP

        df = wr.athena.read_sql_query(sql=sql,
                                      ctas_approach=False,
                                      database=db_name,
                                      workgroup=workgroup)
        return df

    # TODO: Add a method that dumps athena query result into s3 directly.


if __name__ == '__main__':
    fire.Fire(AthenaManager)
