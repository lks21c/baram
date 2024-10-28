from pprint import pprint
from typing import Optional, Union, Dict, Any, List, Literal

import awswrangler as wr
import boto3
import fire
import pandas as pd
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
        self.cli = boto3.client('athena')

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
        columns = ', '.join([f"`{k}` {column_def[k]} comment '{column_comments[k]}'"
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
                       sql_filepath: str,
                       replacements: Optional[dict] = None):
        '''
        Read txt sql file from s3 and fetch it via Athena.

        :param bucket_name: the name of s3 bucket containing file
        :param sql_filepath: prefix of sql text file (s3 key)
        :param replacements: specified replacements for specific purpose of query
        :return: string, a line of query
        '''
        sm = S3Manager(bucket_name=bucket_name)
        query_txt = sm.get_object_body(sql_filepath).decode('utf-8').replace('\n', ' ')
        for k, v in replacements.items():
            query_txt = query_txt.replace(k, v)
        return query_txt

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

    def from_df_to_athena(self, df: pd.DataFrame):
        # TODO
        pass

    def get_iceberg_metadata_df(self,
                                db_name: str,
                                table_name: str,
                                property: str = 'files',
                                workgroup: Optional[str] = None) -> pd.DataFrame:
        '''
        Get metadata DataFrame of athena iceberg table.
        :param db_name:
        :param table_name:
        :param property: among below things
                        files – Shows a table's current data files.
                        manifests – Shows a table's current file manifests.
                        history – Shows a table's history.
                        partitions – Shows a table's current partitions.
                        snapshots – Shows a table's snapshots.
                        refs – Shows a table's references.
        :param workgroup:
        :return:
        '''

        assert property in ['files', 'manifests', 'history', 'partitions', 'snapshots', 'refs']

        sql = f'select * from "{db_name}"."{table_name}${property}"'
        df = self.from_athena_to_df(sql=sql, db_name=db_name, workgroup=workgroup)

        return df

    def get_table_manifest_paths(self,
                                 db_name: str,
                                 table_name: str,
                                 workgroup: Optional[str] = None) -> list:
        '''
        get manifest paths as list
        :param db_name:
        :param table_name:
        :param workgroup:
        :return:
        '''

        df = self.get_table_manifest_df(db_name=db_name,
                                        table_name=table_name,
                                        property='manifests',
                                        workgroup=workgroup)
        return df['path'].to_list()

    def create_iceberg_table_from_table(self,
                                        to_db_name: str,
                                        to_table_name: str,
                                        as_sql: str,
                                        location: str,
                                        params: Union[Dict[str, Any], List[str], None] = None,
                                        paramstyle: Literal['qmark', 'named'] = 'qmark'
                                        ):
        '''
        create iceberg table from existing table (using ctas)
        :param to_db_name: database name of result table
        :param to_table_name: table name of result table
        :param as_sql: query before "create table * with * as", usually starts with "select"
        :param location: s3 location which data and metadata of iceberg table will be saved
        :param params: for parametrized query.
                       This should be dictionary for "named" paramstyle and list for "qmark" paramstyle.
        :param paramstyle: "named" or "qmark"
        :return:
        '''

        ctas = f"""create table {to_db_name}.{to_table_name}
                   with (table_type = 'ICEBERG',
                         location = '{location}',
                         is_external = false)
                   as {as_sql}"""

        self.fetch_query(sql=ctas,
                         db_name=to_db_name,
                         params=params,
                         paramstyle=paramstyle)
    def analyze_query_usage(self, workgroup: str, threshold_size: int):
        '''
        Analyze Athena query usage.

        :param workgroup:
        :param threshold_size:
        :return:
        '''

        def get_query(query_execution_id: str):
            response = athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )

            query_string = response['QueryExecution']['Query']
            return query_string

        query_execution_ids = []
        next_token = None
        kwargs = {'WorkGroup': workgroup, 'MaxResults': 50}
        while True:
            if next_token:
                kwargs['NextToken'] = next_token
            else:
                kwargs.pop('NextToken', None)

            response = athena_client.list_query_executions(**kwargs)
            query_execution_ids.extend(response['QueryExecutionIds'])

            next_token = response.get('NextToken')
            if not next_token:
                break

        # Retrieve information for each query and print the amount of data scanned
        scan_dict = {}
        for query_execution_id in query_execution_ids:
            try:
                query_info = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
                completion_datetime = query_info['QueryExecution']['Status']['CompletionDateTime'].replace(tzinfo=None)
                current_month = datetime(datetime.today().year, datetime.today().month, 1)
                one_month_later = current_month + relativedelta(months=1)

                if current_month <= completion_datetime and completion_datetime < one_month_later:
                    data_scanned = query_info['QueryExecution']['Statistics']['DataScannedInBytes']
                    if data_scanned > threshold_size:
                        scan_dict[query_execution_id] = data_scanned
            except:
                continue

        sorted_scan_dict = {k: v for k, v in sorted(scan_dict.items(), key=lambda item: item[1], reverse=True)}
        total_scanned_byte = 0
        new_dict = {}
        for k, v in sorted_scan_dict.items():
            key = get_query(k)
            if key not in new_dict:
                new_dict[key] = v
            else:
                new_dict[key] += v
            total_scanned_byte += int(v)

        sorted_scan_new_dict = {k: v for k, v in sorted(new_dict.items(), key=lambda item: item[1], reverse=True)}
        for k, v in sorted_scan_new_dict.items():
            percent = v / total_scanned_byte * 100
            print(k, f'{v / threshold_size:.1f}GB ({percent:.1f}%)')

        print(f'total_scanned_byte: {total_scanned_byte / threshold_size:.1f}GB')


if __name__ == '__main__':
    fire.Fire(AthenaManager)
