from __future__ import division
import csv
import os
import requests
from sodapy import Socrata
import yaml
import base64
import itertools
import datetime
import json
import time 
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEBase import MIMEBase
from email import encoders
import csv
import logging
from os import listdir
from os.path import isfile, join
import re

class ConfigItems:

    def __init__(self, inputdir, fieldConfigFile):
        self.inputdir = inputdir
        self.fieldConfigFile = fieldConfigFile

    def getConfigs(self):
        configItems = 0
        with open(self.inputdir + self.fieldConfigFile ,  'r') as stream:
            try:
                configItems = yaml.load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        return configItems

class SocrataClient:
    def __init__(self, inputdir, configItems):
        self.inputdir = inputdir
        self.configItems = configItems
        
    def connectToSocrata(self):
        clientConfigFile = self.inputdir + self.configItems['socrata_client_config_fname']
        with open(clientConfigFile,  'r') as stream:
            try:
                client_items = yaml.load(stream)
                client = Socrata(client_items['url'],  client_items['app_token'], username=client_items['username'], password=base64.b64decode(client_items['password']))
                return client
            except yaml.YAMLError as exc:
                print(exc)
        return 0

class emailer():
    '''
    util class to email stuff to people.
    '''
    def __init__(self, inputdir, configItems):
        self.inputdir = inputdir
        self.configItems = configItems
        self.emailConfigs = self.getEmailerConfigs()
        
        
    def getEmailerConfigs(self):
        emailConfigFile = self.inputdir + self.configItems['email_config_fname']
        with open(emailConfigFile,  'r') as stream:
            try:
                email_items = yaml.load(stream)
                return email_items
            except yaml.YAMLError as exc:
                print(exc)
        return 0
    
    def setConfigs(self, subject_line, msgBody, fname_attachment=None, fname_attachment_fullpath=None):
        self.server = self.emailConfigs['server_addr']
        self.server_port = self.emailConfigs['server_port']
        self.address =  self.emailConfigs['email_addr']
        self.password = base64.b64decode(self.emailConfigs['email_pass'])
        self.msgBody = msgBody
        self.subjectLine = subject_line
        self.fname_attachment = fname_attachment
        self.fname_attachment_fullpath = fname_attachment_fullpath
        self.recipients = self.emailConfigs['receipients']
    
    def getEmailConfigs(self):
        return self.emailConfigs
    
    def sendEmails(self, subject_line, msgBody, fname_attachment=None, fname_attachment_fullpath=None):
        self.setConfigs(subject_line, msgBody, fname_attachment, fname_attachment_fullpath)
        fromaddr = self.address
        toaddr = self.recipients
        msg = MIMEMultipart()
        msg['From'] = fromaddr
        msg['To'] = toaddr
        msg['Subject'] = self.subjectLine
        body = self.msgBody 
        msg.attach(MIMEText(body, 'plain'))
          
        #Optional Email Attachment:
        if(not(self.fname_attachment is None and self.fname_attachment_fullpath is None)):
            filename = self.fname_attachment
            attachment = open(self.fname_attachment_fullpath, "rb")
            part = MIMEBase('application', 'octet-stream')
            part.set_payload((attachment).read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
            msg.attach(part)
        
        #normal emails, no attachment
        server = smtplib.SMTP(self.server, self.server_port)
        server.starttls()
        server.login(fromaddr, self.password)
        text = msg.as_string()
        server.sendmail(fromaddr, toaddr, text)
        server.quit()




class PostGeoJsonToSocrata:
    def __init__(self, inputdir, configItems, client):
        self.configItems = configItems
        self.client = client
        self.dataset = {}
        self.dataset['fourXFour'] = configItems['fourXFour']
        self.json_file = configItems['json_file']
        self.dataset['rowsInserted'] = 0
        self.dataset['totalRecords'] = 0
        self.dataset['dataset_name'] = 'CPC 0001: Planning Cases'
        self.retries = 0
        self.chunkSize = 1000
        self.rejected_chunks_file = configItems['rejected_chunks_file']
        self.inputdir = inputdir 
        
    def setChunkSize(self, chunkSize):
        self.chunkSize = chunkSize

    def setRetries(self, retries):
        self.retries = self.retries + retries

    def renameKey(obj):
        for key in obj.keys():
            new_key = key.replace("geometry","the_geom")
            if new_key != key:
                obj[new_key] = obj[key]
                del obj[key]
        return obj

    @staticmethod
    def parseGeom(obj):
        attributes = obj['properties']
        geom = obj['geometry']
        attributes['the_geom'] = geom
        return attributes

    def replaceDataSet(self, chunk):
        try: 
            result = self.client.replace(self.dataset['fourXFour'], chunk) 
            self.dataset['rowsInserted'] = self.dataset['rowsInserted'] + int(result['Rows Created'])
            print "Rows inserted: " + str(self.dataset['rowsInserted'])
            time.sleep(0.25)
        except Exception as e:
            logging.exception(e)
            print 'Error: did not insert first dataset chunk'
            return chunk

    def insertData(self, chunk):
        try:
            result = self.client.upsert(self.dataset['fourXFour'], chunk) 
            self.dataset['rowsInserted'] = self.dataset['rowsInserted'] + int(result['Rows Created'])
            print "Rows inserted: " + str(self.dataset['rowsInserted'])
            time.sleep(0.25)
        except Exception as e:
            logging.exception(e)
            return chunk
            print 'Error: did not insert dataset chunk'

    def makeChunks(self, insertDataSet):
        return [insertDataSet[x:x+ self.chunkSize] for x in xrange(0, len(insertDataSet), self.chunkSize)]
    
    def insertGeodataSet(self, insertDataSet):
        print self.dataset['fourXFour']
        rejectedChunks = []
        #need to chunk up dataset so we dont get Read timed out errors
        insertChunks =self.makeChunks(insertDataSet)

        #overwrite the dataset on the first insert chunk[0]
        if self.dataset['rowsInserted'] == 0:
            rejectedChunk = self.replaceDataSet(insertChunks[0])
            if rejectedChunk:
                print "*****Replace Chunk Rejected!************"
                #need to break and return if we didn't insert the first chunk
                return [self.dataset], insertDataSet
            #insert the rest of the chunks[1:]
            if len(insertChunks) > 1:
                for chunk in insertChunks[1:]:
                    rejectedChunk = self.insertData(chunk)
                    if rejectedChunk:
                        rejectedChunks = rejectedChunks + rejectedChunk
        else:
            for chunk in insertChunks[0:]:
                rejectedChunk = self.insertData(chunk)
                if rejectedChunk:
                    rejectedChunks = rejectedChunks + rejectedChunk
        return [self.dataset], rejectedChunks
    
    def retryChunk(self, rejectedChunks):
        newlyRejectedChunks = []
        chunksize  =  int(round((self.chunkSize/2)))
        self.setChunkSize( chunksize) 
        rejectedChunks =self.makeChunks(rejectedChunks)
        while len(rejectedChunks) > 0:
            chunk = rejectedChunks.pop()
            rejectedChunk = self.insertData(chunk)
            if rejectedChunk:
                newlyRejectedChunks = newlyRejectedChunks + rejectedChunk
        self.setRetries(1)
        return newlyRejectedChunks

    def retryRejectedChunks(self, rejectedChunks):
        #area to retry rejected chunks 
        while (self.retries < 11) and (len(rejectedChunks) > 0):               
            rejectedChunks = self.retryChunk(rejectedChunks)
        #finally, save the rejected chunks to review
        with open(self.rejected_chunks_file, 'w') as f:
            json.dump(rejectedChunks, f)
        return [self.dataset]
        

    def loadJson(self, f):
        with open(self.inputdir + f) as f:
            data = json.load(f)
            data =  [self.parseGeom(dataItem) for dataItem in data['features']]
            self.dataset['totalRecords'] = len(data) + self.dataset['totalRecords']
            return data
    

class logETLLoad:
    '''
    util class to get job status- aka check to make sure that records were inserted; also emails results to receipients
    '''
    def __init__(self, inputdir, configItems):
        self.log_dir = configItems['log_dir']
        self.dataset_base_url = configItems['dataset_base_url']
        self.failure =  False
        self.job_name = configItems['job_name']
        self.logfile_fname = self.job_name + ".csv"
        self.logfile_fullpath = self.log_dir + self.job_name + ".csv"
        self.configItems =  configItems
        self.inputdir = inputdir

    def sucessStatus(self, dataset):
        if dataset['rowsInserted'] == dataset['totalRecords']:
            dataset['jobStatus'] = "SUCCESS"
        else: 
            dataset['jobStatus'] = "FAILURE"
            self.failure =  True
        return dataset
    
    def makeJobStatusAttachment(self,  finishedDataSets ):
        with open(self.logfile_fullpath, 'w') as csvfile:
            fieldnames = finishedDataSets[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for dataset in finishedDataSets:
                writer.writerow(dataset)

    def getJobStatus(self):
        if self.failure: 
            return  "FAILED: " + self.job_name
        else: 
            return  "SUCCESS: " + self.job_name

    def makeJobStatusMsg( self,  dataset  ):
        msg = dataset['jobStatus'] + ": " + dataset['dataset_name'] + "-> Total Rows:" + str(dataset['totalRecords']) + ", Rows Inserted: " + str(dataset['rowsInserted'])  + ", Link: "  + self.dataset_base_url + dataset['fourXFour'] + " \n\n " 
        return msg
    
    def sendJobStatusEmail(self, finishedDataSets):
        msgBody  = "" 
        for i in range(len(finishedDataSets)):
            #remove the column definitions, check if records where inserted
            dataset = self.sucessStatus( finishedDataSets[i])
            msg = self.makeJobStatusMsg( dataset  )
            msgBody  = msgBody  + msg
        subject_line = self.getJobStatus()
        email_attachment = self.makeJobStatusAttachment(finishedDataSets)
        e = emailer(self.inputdir, self.configItems)
        emailconfigs = e.getEmailConfigs()
        if os.path.isfile(self.logfile_fullpath):
            e.sendEmails( subject_line, msgBody, self.logfile_fname, self.logfile_fullpath)
        else:
            e.sendEmails( subject_line, msgBody)
        print "****************JOB STATUS******************"
        print subject_line
        print "Email Sent!"

class pyLogger:
    def __init__(self, configItems):
        self.logfn = configItems['exception_logfile']
        self.log_dir = configItems['log_dir']
        self.logfile_fullpath = self.log_dir+self.logfn

    def setConfig(self):
        #open a file to clear log
        fo = open(self.logfn, "w")
        fo.close
        logging.basicConfig(level=logging.DEBUG, filename=self.logfn, format='%(asctime)s %(levelname)s %(name)s %(message)s')
        logger=logging.getLogger(__name__)
            #self.logfile_fullpath )
    


#inputdir = "C:\SfGis\Program\Data\FmeServer\Data\CPC\\"
inputdir = 'C:\Users\Janine.Heiser\Desktop\planning\\'
fieldConfigFile = 'fieldConfig.yaml'
cI =  ConfigItems(inputdir,fieldConfigFile  )
configItems = cI.getConfigs()

#set the log level
lg = pyLogger(configItems)
lg.setConfig()

#make the socrata client
sc = SocrataClient(inputdir, configItems)
client = sc.connectToSocrata()

#create class objects
pgJS = PostGeoJsonToSocrata(inputdir, configItems, client)
lte = logETLLoad(inputdir, configItems)

onlyfiles = [f for f in listdir(inputdir) if isfile(join(inputdir, f))]
firstpass = True
filesRegex = re.compile(configItems['files_regex'])
for f in onlyfiles:
    #if re.match(r'planning_cases+[0-9]+', f):
    if re.match(filesRegex, f):
        print "***Loading File: " + str(f) + "*******"
        #load the json file:
        data = pgJS.loadJson(f)
        if firstpass:
            rejectedChunks = data
            counter = 0
            while (len(rejectedChunks) == len(data)) and (counter < 10):
                loadedData, rejectedChunks = pgJS.insertGeodataSet(data)
                counter = counter + 1
                firstpass = False
        else:
            loadedData, rejectedChunks = pgJS.insertGeodataSet(data)
        
#now try to re-insert chunks to make sure it wasn't a connection error
if len(rejectedChunks) > 0:
    print "retry!"
    loadedData = pgJS.retryRejectedChunks(rejectedChunks) 
print loadedData

#send out the email notice about success or failure
msg = lte.sendJobStatusEmail(loadedData)

#shut everything down
client.close()
logging.shutdown()

