from pprint import pprint
import datetime
from glob import glob
import subprocess
from subprocess import Popen,PIPE
import os
import sys
import logging
from Jpostgres.sqlCon import  PgCon, JLogger
from Jpostgres.Jcrypto import JKMService
from subprocess import PIPE,Popen
#from sqlCon import  PgCon, JLogger
#from Jcrypto import JKMService

if sys.version_info[0] > 2:
    import configparser
else:
    import ConfigParser as configparser


# todo if not files for the table exit the code
# Fail the job once all tables got loaded ,if any failure is there
# -add table status entries file loads


class MetaDb(JKMService):
    config = configparser.RawConfigParser()
    config.read('/data/juniper/jar/Jpostgres/pgcopy.cfg')
    meta_target_table = config.get('meta_tables', 'target')

    def __init__(self):
        self.meta_sect = dict(self.config.items('pg_meta_db'))
        logger.info(self.meta_sect)
        self.meta_db_obj = PgCon(self.meta_sect)

    def get_meta_details(self, feed_id, run_id):
        logger.info('Getting meta data details ...')

        return self.set_meta_details(feed_id, run_id)

    def get_target_details(self, target_name):
        logger.info("Getting target details")
        pg_target_details = self.set_target_details(target_name)
        self.meta_db_obj.close_conn()
        return pg_target_details

    def set_meta_details(self, feed_id, run_id):
        self.meta_db_obj.open_conn()
        table_master_tbl = self.config.get('meta_tables', 'table_master')
        feed_master_tbl = self.config.get('meta_tables', 'feed_master')
        nifi_status_tbl = self.config.get('meta_tables', 'nifi_status_table')
        query = """select efm.feed_unique_name,etm.table_name,EFM.COUNTRY_CODE,ens.nifi_status_sequence,
            ens.extracted_date from {} etm join  {} efm
            on etm.feed_sequence=efm.feed_sequence  join 
             {} ens on etm.feed_sequence=ens.feed_id
            where etm.feed_sequence={} and ens.run_id='{}' and ens.job_type='R' and lower(ens.status)='success'""".format(table_master_tbl,
                                                                                          feed_master_tbl,
                                                                                          nifi_status_tbl, feed_id,
                                                                                          run_id)
        meta_details = self.meta_db_obj.sel_query(query)
        logger.info("meta details : {}".format(meta_details))

        return meta_details

    def set_target_details(self, target_name):
        target_details = {}
        target_query = """select ora_host_name,ora_port_no,ora_username,ora_database_name,
                            project_sequence,system_sequence,credential_id
                            from {} 
                                where target_unique_name='{}'""".format(self.meta_target_table, target_name)
        result = self.meta_db_obj.sel_query(target_query)
        logger.info(result)
        kmsDecrypted = self.decrypt(result[0][4], result[0][5], result[0][6])
        target_details['host'] = result[0][0]
        target_details['port'] = result[0][1]
        target_details['uname'] = result[0][2]
        target_details['upassword'] = kmsDecrypted
        target_details['dbname'] = result[0][3]
        logger.info("Target details : {}".format(target_details))
        return target_details


class PgLoad(object):

    def __init__(self, feedId, runid, targetName):
        self.feedId = feedId
        self.runid = runid
        self.targetName = targetName
        self.mb = MetaDb()
        self.blob = self.mb.config.get('path', 'blob_path')
        self.counter_object = {}
        self.failed_tables = {}
        self.recon_tuple = list

    def load_process(self):
        source_tables = self.mb.get_meta_details(self.feedId, self.runid)
        logger.info(source_tables)
        if not source_tables:
            logger.info(
                "No tables are present in meta data for feed : {} Run ID : {} ".format(self.feedId, self.runid))
            logger.info("Exiting the Job ...")
            sys.exit(1)
        self.feed_unique_name = source_tables[0][0]
        self.NIFI_sequence = str(source_tables[0][3])
        self.extracted_date = source_tables[0][4]
        target_host_details = self.mb.get_target_details(self.targetName)
        self.mb.meta_db_obj.close_conn()

        self.target_db_obj = PgCon(target_host_details)
        self.target_db_obj.open_conn()
        self.copy_tables(source_tables)
        # self.bulk_copy(source_tables, target_host_details)
        self.target_db_obj.close_conn()

        if (self.counter_object ):
            self.recon()
        if self.failed_tables:
            logger.info('Job Failed : Error in loading tables ----{}')
            self.update_failures()
            logger.info("Job failed : failure in loading the tables ")
            sys.exit(1)

    def bulk_copy(self,tables, target_host_details):
        _host = target_host_details['host']
        _port = target_host_details['port']
        _uname = target_host_details['uname']
        _upassword = target_host_details['upassword']
        _dbname = target_host_details['dbname']
        for each_table in tables:

            table_name = each_table[1]
            country_code = each_table[2]
            logger.info("Loading has started for table : {}\n".format(table_name))
            path = '{}/{}/{}/{}/{}/data/{}_{}_{}_*'.format(self.blob.strip("'"), country_code, self.feed_unique_name,
                                                           self.extracted_date,
                                                           self.runid, self.feed_unique_name, table_name, self.runid)
            logger.info(path)
            files = glob(path)
            file_parse = '\n'.join(files)
            logger.info(files)
            logger.info("Files present in staging for the {} Table : {}".format(table_name, file_parse))
            for each_file in files:
                try:
                    copy_command = """PGPASSWORD={} psql -h {} -U {}  -d cubex -c "\copy {} from {}
                        WITH CSV HEADER DELIMITER AS ',' " """.format(_upassword, _host, _uname, _dbname, table_name, each_file)

                    copy = Popen([copy_command], stdin=PIPE, stdout=PIPE, stderr=PIPE)
                    output,err = copy.communicate()
                    logger.info('copy code : {}'.format(copy.returncode))
                    logger.info('output :{}'.format(output))
                    logger.info('err:{}'.format(err))

                except Exception as e:
                    self.failed_tables[table_name] = "Failed"
                    logger.info('File copy is Failed {}'.format(self.failed_tables.get(table_name)))





    def copy_tables(self, tables):
        for each_table in tables:
            table_name = each_table[1]
            country_code = each_table[2]
            logger.info("Loading has started for table : {}\n".format(table_name))
            path = '{}/{}/{}/{}/{}/data/{}_{}_{}_*'.format(self.blob.strip("'"), country_code, self.feed_unique_name,
                                                           self.extracted_date,
                                                           self.runid, self.feed_unique_name, table_name, self.runid)
            logger.info(path)
            files = glob(path)
            file_parse = '\n'.join(files)
            logger.info(files)
            logger.info("Files present in staging for the {} Table : {}".format(table_name, file_parse))
            for each_file in files:
                file_copy = self.target_db_obj.bulk_insert(each_file, table_name,header=True)
                logger.info("{}".format(file_copy))

                if file_copy is 'Failed':
                    self.failed_tables[table_name] = "Failed"
                    logger.info('File copy is Failed {}'.format(self.failed_tables.get(table_name)))
                    logger.info('Transaction Rolled Back for the table :{}'.format(table_name))
                    self.target_db_obj.pg_conn.rollback()
                    break
                self.counter_object[file_copy.tablename] = self.counter_object.get(file_copy.tablename,
                                                                                   0) + file_copy.count
            if file_copy is not 'Failed':
                self.target_db_obj.pg_conn.commit()
                logger.info("Commit to the database for table : {} completed".format(table_name))

    def recon(self):
        self.mb.meta_db_obj.open_conn()
        self.extracted_date = datetime.datetime.strptime(self.extracted_date, '%Y%m%d')
        self.recon_tuple = []
        for tableName, count in self.counter_object.items():
            self.recon_tuple.append((self.feed_unique_name, self.extracted_date, self.runid + self.NIFI_sequence,
                                     "{}~{}-count".format(self.targetName, tableName),self.counter_object[tableName]))
        pprint(self.recon_tuple)
        recon_table_cols = 'event_feed_id,event_batch_date,event_run_id,event_type,event_value'
        self.mb.meta_db_obj.insert('juniperx.logger_stats_master',recon_table_cols, self.recon_tuple)

    def update_failures(self):
        logger.info('conn :{}'.format(self.mb.meta_db_obj.pg_conn))
        print(self.mb.meta_db_obj.pg_conn.status)
        if  self.mb.meta_db_obj.pg_conn.status != 0:
            print('connection opening')
            self.mb.meta_db_obj.open_conn()
        cols = 'feed_sequence,table_name,status,run_id'
        table = config.get('meta_tables', 'target_status_table')
        values = []
        logger.info('--'*30+' Failed Tables Info '+'--'*30)
        for each_failed_table,status in self.failed_tables.items():
            logger.info('\t -> {}'.format(each_failed_table))
            values.append((self.feedId, each_failed_table,status, self.runid))
        logger.info(values)
        self.mb.meta_db_obj.insert(table, cols, values)
        logger.info('--'*30+' ***** '+'--'*30)
        self.mb.meta_db_obj.close_conn()


if __name__ == '__main__':
    config = configparser.RawConfigParser()
    config.read('/data/juniper/jar/Jpostgres/pgcopy.cfg')
    #config.read('./pgcopy.cfg')
    logPath = config.get('path', 'log_path')
    feedId = sys.argv[1]
    run_id = sys.argv[2]
    targetName = sys.argv[3]
    #feedId = 17
    #run_id = '157109760000001'
    #targetName = 'PG_DEMO'
    log_name = "{}_{}_{}_{}.log".format(feedId, run_id, targetName, datetime.datetime.now().strftime('%Y_%m_%d_%H:%M:%S'))
    logging.basicConfig(format='%(asctime)s-%(levelname)s-%(message)s', datefmt='%d-%b-%y:%H:%M:%S')
    logger = logging.getLogger('sqlCon')
    formatter = logging.Formatter('%(asctime)s-%(levelname)s-%(message)s', datefmt='%d-%b-%y:%H:%M:%S')
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(os.path.join(logPath, log_name))
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.info("Given Arguments FeedId :{} Run id :{} Target Name: {}".format(feedId,run_id,targetName))
    SHandler = logging.StreamHandler(sys.stdout)
    SHandler.setLevel(logging.DEBUG)
    logger.addHandler(SHandler)
    Pg = PgLoad(feedId, run_id, targetName)
    Pg.load_process()

    # feedId = sys.argv[1]
    # run_id = sys.argv[2]
    # targetName = sys.argv[3]
