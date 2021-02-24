import paramiko
import pandas as pd
import time
from subprocess import PIPE, Popen
from ssl import create_default_context
from elasticsearch import helpers, Elasticsearch
import csv
import os
import subprocess



host = '10.123.212.107'
user = 'hadoop'
secret = 'Claro@123'
port = 22

ssh = paramiko.SSHClient()
# Set policy to use when connecting to servers without a known host key
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(hostname=host, username=user, password=secret, port=port)
stdin, stdout, stderr = ssh.exec_command('/opt/ericsson/bin/alnip -l 3 | grep BSC')
allnip_bsc = stdout.readlines()
bsc_df = pd.DataFrame(allnip_bsc)
bsc_lista=bsc_df[0].str.extract(r'ManagedElement=(.*)')
bsc_lista.columns = ['bsc']
bscdelimitado = pd.DataFrame(columns=['CELL','BCCH','CBCH','SDCCH','QUEUED','ECBCCH','lixo1','lixo2'])
for index, row in bsc_lista.iterrows():
    print(row['bsc'])
    comando='/opt/ericsson/bin/cfi -N '+row['bsc']+' -1 "RLCRP:CELL=ALL;"'
    stdin, stdout, stderr = ssh.exec_command(comando)
    saida_comando_tmp = stdout.readlines()
    bsc = pd.DataFrame(saida_comando_tmp)
    bsc.drop(bsc.head(5).index, inplace=True)
    bsc.drop(bsc.tail(5).index, inplace=True)
    bsc.columns = ['bsc']
    bscdelimitado_tmp=bsc['bsc'].str.split(" +",expand = True)
    bscdelimitado_tmp['bsc'] = row['bsc']
    bscdelimitado = pd.concat([bscdelimitado, bscdelimitado_tmp], axis=0)
bscdelimitado.drop(bsc.head(1).index, inplace=True)
bscdelimitado=bscdelimitado.drop(columns=['BCCH','CBCH','CELL','ECBCCH','QUEUED','SDCCH','lixo1','lixo2'])
bscdelimitado.columns=['CELL','BSC','BCCH','ECBCCH','QUEUED','SDCCH','lixo1','lixo2','lixo3']
cols = bscdelimitado.columns.tolist()
cols=['BSC','CELL', 'BCCH', 'ECBCCH']
bscdelimitado = bscdelimitado[cols]
bscdelimitado.to_csv('/home/rosey/automacoes/ericsson/legado/2g/noi_bsc_cells_query.csv',mode = 'w', index=False)


hdfsputvhh = ['/usr/bin/hdfs dfs -put -f /home/rosey/automacoes/ericsson/legado/2g/noi_bsc_cells_query.csv hdfs://nameservice1/data/ericsson/legado/2g/noi_bsc_cells_query/noi_bsc_cells_query.csv']
hdfsputcommandvhh = ''.join(map(str, hdfsputvhh))
print(hdfsputcommandvhh)
subprocess.call([hdfsputcommandvhh], shell=True)