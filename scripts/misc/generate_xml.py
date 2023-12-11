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
    local_xml_directory = '/tmp/transactions'
    hdfs_xml_directory = '/in/transactions'
    generate_xml_interval = 1 #seconds

    if os.path.exists(local_xml_directory):
        shutil.rmtree(local_xml_directory) 
    os.mkdir(local_xml_directory)

    subprocess.run(f'/opt/hadoop-3.3.6/bin/hdfs dfs -rm {hdfs_xml_directory}/*', shell=True)

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
        file_name = f"transaction_{int(time.time())}.xml"
        local_file_path = f"{local_xml_directory}/{file_name}"
        if not os.path.exists(local_xml_directory):
            # Create the folder
            os.mkdir(local_xml_directory)
        with open(local_file_path, "w") as file:
            file.write(xml_data)
        subprocess.run(f'/opt/hadoop-3.3.6/bin/hdfs dfs -put {local_file_path} {hdfs_xml_directory}/', shell=True)
        xml_count += 1

generate_xml_files()
