from pprint import pprint
from typing import Optional, Union, Dict, Any, List, Literal

import fire
import awswrangler as wr
from awswrangler.athena._utils import _QUERY_WAIT_POLLING_DELAY

from baram.s3_manager import S3Manager
from baram.log_manager import LogManager


class AthenaManager(object):
    def __init__(self,
                 bucket_name: str,
                 workgroup: str = 'primary'):
        self.logger = LogManager.get_logger()
        self.ATHENA_WORKGROUP = workgroup
        self.sm = S3Manager(bucket_name)

    def create_glue_external_table(self,
                                   db_name: str,
                                   table_name: str,
                                   column_def: dict,
                                   location: str,
                                   column_comments: Optional[dict] = None,
                                   table_comment: Optional[str] = None):
        '''
        Create Glue External Table, without row insertion

        :param db_name: target glue database name
        :param table_name: target glue table name
        :param column_def: definition for columns. each key means column name, and its value means column type
        :param location: s3 location for query saving
        :param column_comments: comments for columns. each key means column name, and its value means comment
        :param table_comment: comment for table
        :return:
        '''
        columns = ', '.join([f"{k} {column_def[k]} comment '{column_comments[k]}'"
                             if k in column_comments.keys() else f"{k} {column_def[k]}" for k in column_def])

        sql = f"create external table if not exists {db_name}.{table_name}("\
              f"{columns}) "\
              f"comment '{table_comment}' "\
              f"row format delimited fields terminated by ',' "\
              f"stored as textfile "\
              f"location '{location}' "\
              f"tblproperties ('classification'='csv');"

        self.fetch_query(sql=sql,
                         db_name=db_name,
                         s3_output=location)

    def delete_glue_table(self, db_name: str, table_name: str):
        '''
        Delete Glue Table.

        :param db_name: target glue database name
        :param table_name: target glue table name
        :return:
        '''

        wr.catalog.delete_table_if_exists(database=db_name, table=table_name)
        print(f'delete {db_name} {table_name} completed')

        # TODO: delete file on s3 using glue metadata

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
        :param params: for parametrized query. This should be dictionary for "named" paramstyle and list for "qmark" paramstyle.
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
        print(f"fetch_result_path={self.sm.get_s3_web_url(arr[0], '/'.join(arr[1:]))}")
        return res

    def count_rows_from_table(self, table_name: Optional[str] = None):
        '''
        Return rows from table.

        :param table_name:
        :return:
        '''
        df = self.from_athena_to_df(f'SELECT count(*) as cnt FROM {self.get_table_name(table_name)}')
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
        return table exists or not.

        :param db_name: database name
        :param table_name: table name
        :return:
        '''
        return wr.catalog.does_table_exist(db_name, table_name)

    def read_query_txt(self, filepath: str, replacements: dict):
        '''
        read txt file for query from s3.

        :param filepath: filepath of s3
        :param replacements: specified replacements for specific purpose of query
        :return: string, a line of query
        '''

        response = self.cli.get_object(Bucket=self.from_s3_bucket_name, Key=filepath)
        query_txt = response['Body'].read().decode('utf-8').replace('\n', ' ')
        for k, v in replacements.items():
            query_txt = query_txt.replace(k, v)
        return query_txt

    def from_athena_to_df(self, sql: str, db_name: Optional[str] = None, workgroup: Optional[str] = None):
        '''
        run query and return data frame.

        :param sql: sql
        :param db_name: database name
        :param workgroup: athena workgroup
        :return:
        '''

        df = wr.athena.read_sql_query(sql=sql,
                                      ctas_approach=False,
                                      database=db_name,
                                      workgroup=workgroup)
        return df

    # TODO: Add a method that dumps athena query result into s3 directly.


if __name__ == '__main__':
    fire.Fire(AthenaManager)
