#  Professional Masters in Big Data Program - Simon Fraser University

#  Assignment 7 (Question 1 - load_logs.py)

#  Submission Date: 26th October 2018
#  Name: Anurag Bejju
#  Student ID: 301369375
#  Professor Name: Gregory Baker


import os, sys, gzip, re, uuid
import datetime
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

def log_split(line):
    updated_line = line_re.split(line)
    if len(updated_line) >= 4:
        if ((len(updated_line[1]) > 0) and (len(updated_line[2]) > 0) and (len(updated_line[3]) > 0) and (len(updated_line[3]) > 0)):
            return (updated_line[1],updated_line[2],updated_line[3],int(updated_line[4]))

def main(input_directory,keyspace,table_name):

    # connecting to keyspace
    cluster = Cluster(['199.60.17.188', '199.60.17.216'])
    session = cluster.connect(keyspace)

    #initialize batch count
    batch_count = 0

    # Creating a table with the inputed table name if it doesnt exixt
    create_statement = 'CREATE TABLE IF NOT EXISTS ' + table_name + ' (id UUID, host TEXT, date_time TIMESTAMP, path_value TEXT, bytes INT, PRIMARY KEY (host,id))'
    session.execute(create_statement);

    # Truncating all old values
    truncate_statemet = 'TRUNCATE '+table_name+';'
    session.execute(truncate_statemet)

    # Opening the inputed file directory and initializing batch statement
    open_directory = os.listdir(input_directory)
    batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)

    # Insert statement used to insert values into the table
    insert_log = session.prepare('INSERT INTO '+ table_name + ' (host, id, date_time, path_value, bytes) VALUES (?,?,?,?,?)')

    for file_value in open_directory:
        if '.gz' in file_value:
            with gzip.open(os.path.join(input_directory,file_value), 'rt', encoding='utf-8') as logfile:
                for line in logfile:

                    # split and get log values
                    log_values = log_split(line)

                    if log_values is not None:

                        # Insert values in batch
                        batch.add(insert_log,(log_values[0],uuid.uuid1(),datetime.datetime.strptime(log_values[1], "%d/%b/%Y:%H:%M:%S"),log_values[2],log_values[3]))
                        batch_count = batch_count + 1

                        #Inserts values in batches of 200
                        if batch_count%200 == 0:
                            session.execute(batch)
                            batch.clear()
    session.execute(batch)
    print ('Query Exectuion Completed')
    cluster.shutdown()


if __name__ == '__main__':
    input_directory = sys.argv[1]
    keyspace = sys.argv[2]
    table_name = sys.argv[3]
    main(input_directory,keyspace,table_name)
