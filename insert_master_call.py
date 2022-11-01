#!/bin/python3
from datetime import date
import datetime
import time
import traceback
import logging
import logging.handlers
import pymysql
import sys
import os

konek = pymysql.connect(host = '127.0.0.1', port = 3310, user = 'root', password = 'rahasia', database = 'lif')
cursor = konek.cursor()

sqltrun1 = "TRUNCATE telakses;"
cursor.execute(sqltrun1)
konek.commit()

sqltrun2 = "TRUNCATE voxnet;"
cursor.execute(sqltrun2)
konek.commit()

sqltrun3 = "TRUNCATE telakses2;"
cursor.execute(sqltrun3)
konek.commit()

logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

#logger = app.logger
filename = "/home/boruto/alip/new_laporan_cdr/log/cdrmaster.log"
ch = logging.handlers.TimedRotatingFileHandler(filename,'midnight',1)
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger 
logger.addHandler(ch)

print("Masukan Nama Folder Dengan Format : Tahun-Bulan-Tanggal")
print("Ex : 2022-05-27\n")

def format_check(date, fmt):
    datetime.datetime.strptime(dir_date, '%Y-%m-%d')

while(True):
	try:
		dir_date = input("Masukan Nama Folder : ")
		if not format_check(dir_date, '%Y-%m-%d'):
			break
	except:
		print("Format Salah")
		continue

data = {}

os.system(f"mkdir /home/boruto/alip/new_laporan_cdr/backup_harian/{dir_date}")

os.system('/home/boruto/alip/new_laporan_cdr/tarik_master_call.bash')

f_names_telakses = ['/home/boruto/alip/new_laporan_cdr/hasil_tarikan/Masternoela.csv','/home/boruto/alip/new_laporan_cdr/hasil_tarikan/Masterwista.csv','/home/boruto/alip/new_laporan_cdr/hasil_tarikan/Mastervioleta.csv','/home/boruto/alip/new_laporan_cdr/hasil_tarikan/Masterfreya.csv','/home/boruto/alip/new_laporan_cdr/hasil_tarikan/Mastermedik.csv','/home/boruto/alip/new_laporan_cdr/hasil_tarikan/Masterkomcad.csv','/home/boruto/alip/new_laporan_cdr/hasil_tarikan/Masternaomi.csv','/home/boruto/alip/new_laporan_cdr/hasil_tarikan/Masterkopnus.csv']

for f_name_telakses in f_names_telakses:
	split_server_name = f_name_telakses.strip().split("/Master")
	split_server_name = split_server_name[1].strip().split(".")
	data['server_name'] = split_server_name[0]

	with open(f_name_telakses) as f:
		lines = f.readlines()
		os.system(f"touch backup_harian/{dir_date}/{data['server_name']}_{dir_date}.sql")
		with open(f"backup_harian/{dir_date}/{data['server_name']}_{dir_date}.sql", "w") as editfile:
			for line in lines:
				if('outtovoxnet' not in line):
					continue
				if('/opt/ivr_ai' in line):
					continue
				cols = line.strip().split(",")
				data['no_destination'] = cols[2].replace('"','').strip()
				data['context'] = cols[3].replace('"','').strip()
				data['call_start'] = cols[9].replace('"','').strip()
				data['call_start_until_end'] = cols[12].replace('"','').strip()
				data['call_answer'] = cols[10].replace('"','').strip()
				data['status'] = cols[14].replace('"','').strip()

				if(data['call_answer'] == ''):
					continue
				totanggal = datetime.datetime.strptime(data['call_answer'], '%Y-%m-%d %H:%M:%S').timestamp()
				data['call_answer'] = str(datetime.datetime.fromtimestamp(totanggal + 25200))
				
				totanggal = datetime.datetime.strptime(data['call_start'], '%Y-%m-%d %H:%M:%S').timestamp()
				data['call_start'] = str(datetime.datetime.fromtimestamp(totanggal + 25200))

				if(data['context'] != 'keluar'):
					continue
				if(data['call_start'][:10] != str(dir_date)):
					continue
				data['call_start'] = data['call_start'].strip()
				data['call_answer'] = data['call_answer'].strip()

				sql_dump = f"""INSERT INTO telakses (nomortujuan, callstart, durasi, callanswer, status, server) VALUES ("{data['no_destination']}", "{data['call_start']}", {data['call_start_until_end']}, "{data['call_answer']}", "{data['status']}", "{data['server_name']}");"""
				editfile.write(f"{sql_dump}\n")

				sql_insert = "INSERT INTO telakses (nomortujuan, callstart, durasi, callanswer, status, server) VALUES (%s, %s, %s, %s, %s, %s)"
				cursor.execute(sql_insert,(data['no_destination'], data['call_start'], data['call_start_until_end'], data['call_answer'], data['status'], data['server_name']))
				konek.commit()

data_v = {}

f_names_voxnet = "/home/boruto/alip/new_laporan_cdr/hasil_tarikan/Mastervoxnet.csv"
split_server_name = f_names_voxnet.strip().split("/Master")
split_server_name = split_server_name[1].strip().split(".")
data_v['server_name'] = split_server_name[0]

with open(f_names_voxnet) as f:
	lines = f.readlines()
	os.system(f"touch backup_harian/{dir_date}/{data_v['server_name']}_{dir_date}.sql")
	with open(f"backup_harian/{dir_date}/{data_v['server_name']}_{dir_date}.sql", "w") as editfile:
		for line in lines:
			if('Caller' in line):
				continue
			cols = line.strip().split("|")

			data_v['no_destination'] = cols[1].strip()
			data_v['no_destination'] = '0' + data_v['no_destination'][0:]

			data_v['call_start'] = cols[3].strip()

			data_v['call_start_until_end'] = cols[5].strip()
			split_time = data_v['call_start_until_end'].split(":")
			jam = int(split_time[0])
			menit = int(split_time[1])
			detik = int(split_time[2])
			data_v['call_start_until_end'] = (jam * 3600) + (menit * 60) + detik

			if(data_v['call_start'][:10] != str(dir_date)):
					continue

			sql_dump = f"""INSERT INTO voxnet (nomortujuan, callstart, durasi) VALUES ("{data_v['no_destination']}", "{data_v['call_start']}", {data_v['call_start_until_end']});"""
			editfile.write(f"{sql_dump}\n")

			sql_insert = "INSERT INTO voxnet (nomortujuan, callstart, durasi) VALUES (%s, %s, %s)"
			cursor.execute(sql_insert,(data_v['no_destination'], data_v['call_start'], data_v['call_start_until_end']))
			konek.commit()

f_names = f_names_telakses
f_names.append(f_names_voxnet)

for f_name in f_names:
	split_server_name = f_name.strip().split("/Master")
	split_server_name = split_server_name[1].strip().split(".")
	data['server_name'] = split_server_name[0]

	file = f"backup_harian/{dir_date}/{data['server_name']}_{dir_date}.sql"
	filesize = os.path.getsize(file)

	if filesize == 0:
		os.remove(file)
	else:
		pass

sql_insert = "INSERT INTO telakses_backup (nomortujuan, callstart, durasi, callanswer, status, server) SELECT nomortujuan, callstart, durasi, callanswer, status, server FROM telakses;"
cursor.execute(sql_insert)
konek.commit()

sql_insert = "INSERT INTO voxnet_backup (nomortujuan, callstart, durasi) SELECT nomortujuan, callstart, durasi FROM voxnet;"
cursor.execute(sql_insert)
konek.commit()

sql_insert = "INSERT INTO telakses2 (nomortujuan ,callstart, durasi, server) SELECT nomortujuan, callstart, durasi, server FROM telakses WHERE status = 'ANSWERED' ORDER BY callstart;"
cursor.execute(sql_insert)
konek.commit()

print("")
os.system(f"mysql -u root -p -h127.0.0.1 -P3310 lif < summarize.sql")
print("")

while(True):
	tombol = input("Ada Perubahan (Y/N) : ").lower()
	if(tombol == "n"):
		os.system(f"mysql -u root -p -h127.0.0.1 -P3310 lif < summarize.sql > backup_harian/{dir_date}/summarize_{dir_date}.csv")
		break
	else:
		break