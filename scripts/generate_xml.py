import random, time, threading, os, glob
from random import randint
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
import shutil, subprocess

# Thread function to generate XML files
def generate_xml_files():
    xml_count = 0    
    xml_directory = '/tmp/transactions'
    xml_directory_tmp = '/tmp/transactions_tmp'
    generate_xml_interval = 10 #seconds
    checkpoint = '/data/checkpoint'

    if os.path.exists(xml_directory):
        shutil.rmtree(xml_directory)
    if os.path.exists(xml_directory_tmp):
        shutil.rmtree(xml_directory_tmp)    
    if os.path.exists(checkpoint):
        shutil.rmtree(checkpoint)
    os.mkdir(xml_directory)
    os.mkdir(xml_directory_tmp)
    os.mkdir(checkpoint)


    subprocess.run('/opt/hadoop-3.3.6/bin/hdfs dfs -rm /transactions_xml/*', shell=True)
    while True:
        time.sleep(generate_xml_interval)
        root = ET.Element("Data")         
        for i in range(0, random.randint(3, 5)):
            transaction = ET.SubElement(root, "Transaction")
            transaction_id = ET.SubElement(transaction, "TransactionId")
            transaction_id.text = str(random.randint(1, 100))

            amount = ET.SubElement(transaction, "Amount")
            amount.text = str(round(random.uniform(0, 1000), 2))

            customer_id = ET.SubElement(transaction, "CustomerId")
            customer_id.text = str(random.randint(1, 100))

            datetime_element = ET.SubElement(transaction, "DateTime")
            datetime_element.text = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

            location = ET.SubElement(transaction, "Location")
            location.text = random.choice(["New York", "London", "Tokyo", "Sydney", "Paris"])

            result = ET.SubElement(transaction, "Result")
            result.text = random.choice(["Success", "Failure"])

        xml_data = minidom.parseString(ET.tostring(root, encoding='utf8', method='xml')).toprettyxml(indent="   ")
        file_name = f"transaction_{xml_count}.xml"
        tmp_file_path = f"{xml_directory_tmp}/{file_name}"    
        file_path = f"{xml_directory}/{file_name}"
        if not os.path.exists(xml_directory):
            # Create the folder
            os.mkdir(xml_directory)
        with open(tmp_file_path, "w") as file:
            file.write(xml_data)
        subprocess.run(f'/opt/hadoop-3.3.6/bin/hdfs dfs -put {tmp_file_path} /transactions_xml', shell=True)
        xml_count += 1

generate_xml_files()
