# -*- coding: utf-8 -*-
##IMPORTING LIBRARIES

import requests
from gspread.models import Cell
import json
import csv
import gspread
import boto3
from oauth2client.service_account import ServiceAccountCredentials
## LAMBDA HANDLER FUNCTION FOR AWS MOUNTING
def lambda_handler(event, context):
    headers = {'Accept':'application/json'}  ##AUTHENTICATION
    url = "https://7f7a4efac527336d106feba28c6aca597971c95f:x@api.bamboohr.com/api/gateway.php/cloudtaskhr/v1/reports/185?fd=no"
    response = requests.get(url,headers=headers)  ## GETS LIST OF ALL EMPLOYEES
    ReportJson = json.loads(response.text)
    EmployeeList = ReportJson["employees"]  
    
    #properly call your s3 bucket
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('scriptsautomation')
    key = 'data_file.csv'
    #you would need to grab the file from somewhere. Use this incomplete line below to get started:
    with requests.Session() as s:
        getfile = s.get('https://scriptsautomation.s3.us-east-2.amazonaws.com/data_file.csv')
    with requests.Session() as s:
        getfile = s.get('https://scriptsautomation.s3.us-east-2.amazonaws.com/data_file_out.csv')

    file = open('/tmp/data_file.csv', 'w')  ## ACCESS THE DATA FILE INSIDE TMP FOLDER IN S3 BUCKET (AWS)
    csv_writer = csv.writer(file)
    
    count = False
    for emp in EmployeeList:   ##FOR EACH EMPLOYEE
        if count == False: 
            header = ('ID','Employee #','First Name','Middle Name','Last Name','Status','Gender','Work Email','LindkedIn URL','Hire Date','Termination Date','Employment Status','Termination Type','Elegible For Re-hire','Job Information Date','Location','Division','Department','Job Title','Reporting to') 
            csv_writer.writerow(header) 
            count = True
        csv_writer.writerow(emp.values())  ## PARTSIN DATA STAGE
    file.close() 
    with open('/tmp/data_file.csv') as data_file:
        with open('/tmp/data_file_out.csv', 'w',newline='') as data_file_out: ## GET RID OF BLANK ROWS 
            writer = csv.writer(data_file_out)
            #i=0
            for row in csv.reader(data_file):  ## DATA WAS NOT PARSED CORRECTLY
                #if (i%2)!=0:
                    #i=i+1
                writer.writerow(row)    ## BUT NOW IS CORRECTED
                    #continue
               #if (i%2)==0:
                    #writer.writerow(row)
                    #i=i+1
    cells = []        
    with open('/tmp/data_file_out.csv',newline='') as f:   ## AND SAVED TO DATAFILE OUT
        reader=csv.reader(f)
        cells = list(reader)
        lines=len(list(cells))
        
        print("BambooHR Report fetched (",lines," rows )")  ## THEN WE WANT TO ACCESSS GSPREADSHEETS API TO UPLOAD THE REPORT
        
        scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json",scope)
        client = gspread.authorize(creds)     ##AUTHENTICATION 
        cellsList = []
        for i in range(lines):
            for j in range(20):
                cellsList.append(Cell(row=i+1, col=j+1, value=cells[i][j]))   ##CREATE CELL LIST OBJECT AND POPULATE IT
        sheet = client.open("Bamboo HR HC").sheet1
        sheet.update_cells(cellsList)     #FINALLY MAKE THE UPDATE API CALL ON THE SHEET, APPLYING 
        print("Spreadsheet updated!")
        print("Program Terminated.")
        
    #upload the data into s3
    bucket.upload_file('/tmp/data_file.csv', key)
    bucket.upload_file('/tmp/data_file_out.csv', key)
