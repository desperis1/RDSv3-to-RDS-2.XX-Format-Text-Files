#import pandas
import os
import sqlite3
import subprocess
import time
import csv
from datetime import datetime
from bs4 import BeautifulSoup
import requests
import zipfile
import urllib3
import traceback

urllib3.disable_warnings()


pwd = str(os.getcwd())
logfile = open("logfile_lastrun.txt", "w")
logfile.write(str(datetime.now()) + "   Program executed.\n")
logfile.close()
rdsversionfile = os.path.expanduser('~') + "/.rdsversion"
pwd = pwd + "/"


def extract_zip():
    log("starting extraction of downloaded zip")
    current_version = get_rds_version()
    with zipfile.ZipFile(pwd + "RDS_" + current_version + "_modern_minimal.zip", 'r') as zip_ref:
        zip_ref.extractall(pwd + current_version)
    log("Extracted successfully")

def download_current_rds():
    current_version = get_rds_version()
    link = "https://s3.amazonaws.com/rds.nsrl.nist.gov/RDS/rds_"+ current_version +"/RDS_" + current_version + "_modern_minimal.zip"
    response = requests.get(link, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    bytes_downloaded = 0
    previous_progress = 0
    with open(pwd + "RDS_" + current_version + "_modern_minimal.zip", 'wb') as file:
        for data in response.iter_content(chunk_size=4096):
            file.write(data)
            bytes_downloaded += len(data)
            progress = (bytes_downloaded / total_size) * 100
            if progress - previous_progress >= 1:
                log(f"Download progress: {progress:.2f}%")
                previous_progress = progress
    log("Download Completed")


def check_for_updates_download_extract():
    current_version = get_rds_version()
    if os.path.exists(rdsversionfile):# ak existuje
        log("config found, loading....")
        installedversion = open(rdsversionfile, "a+")
        installedversion = installedversion.read()
        log("installed version :" + installedversion)
        log("current version: " + current_version)
        if current_version in installedversion:# ak je databaza aktualna
            log("database is up to date.")
        else:# ak neni aktualna
            log("database is not actual downloading new database. current version is: "+current_version)
            download_current_rds()
            extract_zip()
            configfile = open(rdsversionfile, "w")
            configfile.write(current_version+"\n")
            configfile.close()
    else:# ak config subor neexistuje
        log("config not found downloading new database.. current version is: "+current_version)
        download_current_rds()
        extract_zip()
        configfile = open(rdsversionfile, "w")
        configfile.write(current_version+"\n")
        configfile.close()


def get_rds_version():
    url = 'https://www.nist.gov/itl/ssd/software-quality-group/national-software-reference-library-nsrl/nsrl-download/current-rds'
    response = requests.get(url, verify=False)
    soup = BeautifulSoup(response.text, 'html.parser')
    element = soup.find('p', class_='critical')
    element_text = str(element.get_text(strip=True))
    rdsversion = element_text.split()[0]
    log("Function get_rds_version() Founded version : " +rdsversion)
    return rdsversion


def log(log_message):
    print(log_message)
    logfile = open("logfile_lastrun.txt", "a")
    logfile.write(str(datetime.now()) + ":  "  +str(log_message) + "\n")
    logfile.close()

def NSRLFile():
    log("Starting... NSRLFile Creation")
    log("PWD: " + pwd )
    current_version = get_rds_version()
    log("starting database edit")
    conn = sqlite3.connect( pwd + "RDS_" + current_version + "_modern_minimal/RDS_" + current_version + "_modern_minimal.db")
    cursor = conn.cursor()
    log("Database Mounter")

    log("EXECUTING DROP TABLE...")
    cursor.execute('DROP TABLE IF EXISTS EXPORT;')
    log("DROP TABLE executed. SUCCESSFULY" )


    log("EXECUTING CREATE TABLE..." )
    cursor.execute('CREATE TABLE EXPORT AS SELECT sha1, md5, crc32, file_name, file_size, package_id FROM FILE;')
    log("CREATE TABLE executed. SUCCESSFULY" )


    log("Executing UPDATE...")
    cursor.execute("""UPDATE EXPORT SET file_name = REPLACE(file_name, \'"\', \'\');""")
    log("UPDATE EXECUTED SUCCESSFULY")

    log("EXECUTING SELECT COMMAND")
    cursor.execute('SELECT sha1, md5, crc32, file_name, file_size, package_id, 0, "" FROM EXPORT ORDER BY sha1;')
    log("SELECT COMMAND EXECUTED  SUCCESSFULY")

    row = cursor.fetchone()

    log("Creating output.txt file")
    output_file = open('output.txt', 'w', newline='')
    log("output file created.")

    csv_writer = csv.writer(output_file, delimiter=',')

    log("Starting writing loop")
    while row is not None:
        csv_writer.writerow(['"' + str(column) + '"' for column in row])
        row = cursor.fetchone()
    log("done all writed.")

    output_file.close()
    log("output file closed")
    conn.close()
    log("Database connection closed.")

    log("executing first sed command")
    sed_command = ['sed', "-i", "s/\"\"\"/\"/g", "output.txt"]
    sed_process_output = subprocess.Popen(sed_command)
    log("Waiting for process to finish")
    sed_process_output.wait()
    log("sed1 command finished successfuly")

    log("creating headerfile..")
    os.system("""echo '"SHA-1","MD5","CRC32","FileName","FileSize","ProductCode","OpSystemCode","SpecialCode"' > NSRLFile-header.txt""")
    log("Header file created.")
    time.sleep(5)
    log("starting output to header file")
    cat_command = "cat output.txt >> NSRLFile-header.txt"
    cat_process_output = subprocess.Popen(cat_command, shell=True)
    log("waiting for cat1 process to finish")
    cat_process_output.wait()
    log("cat1 finished successfuly")
    log("starting sed2 output")
    sedtwo= """sed -e "s/\r//g" NSRLFile-header.txt > NSRLFile.txt"""
    sedtwo_process_output = subprocess.Popen(sedtwo, shell=True)
    log("sed2 executed waiting to finish")
    sedtwo_process_output.wait()
    os.remove("output.txt")
    os.remove("NSRLFile-header.txt")
    log("done, sed2 executed successfully")
    log("remove leftovers triggered")


try:
    check_for_updates_download_extract()
    NSRLFile()

except Exception as e:
    error_message = traceback.format_exc()
    log(error_message)