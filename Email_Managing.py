# -*- coding: utf-8 -*-
"""
Created on Tue Sep 17 07:38:05 2019

@author: Alvaro
"""

import imaplib
import base64
import os
import email
from datetime import date
from getpass import getpass
import tkinter as tk
from tkinter import filedialog
    
#from getpass import getpass library for using password without appearing in console
championship=""
while (championship.upper() != 'F2') or (championship.upper() != 'F3'):
    championship = input('Choose Championship (F2 or F3): ').upper()
    if championship.upper() == 'F2' or championship.upper() == 'F3':
        break 
email_user = input ('Email: ')
email_pass = getpass('Password: ')

mail = imaplib.IMAP4_SSL("outlook.office365.com",'993')
mail.login(email_user, email_pass)
mail.select('Inbox')

root = tk.Tk()
root.withdraw()

main_path = filedialog.askopenfilename()

# "/Users/Alvaro/Desktop/Git Repos/Timing PDF/OfficialTimingF1_PDF/last_search.txt"
# #Folders for My Surface
# main_path_f2="/Users/aform/Desktop/Git Projects/Timing Campos Racing/OfficialTimingF1_PDF/F2/"
# main_path_f3="/Users/aform/Desktop/Git Projects/Timing Campos Racing/OfficialTimingF1_PDF/F3/"
#
# if championship.upper() == 'F2':
#     main_path=main_path_f2
#
# else:
#     main_path=main_path_f3
dir_list=os.listdir(main_path)
if "last_search.txt" in dir_list:
    #last_search txt file already exists. No txt creation needed
    pass
else:
    #Create the empty txt file in the folder
    with open('last_search.txt', 'w') as fp:
        pass

text_file_path=main_path+"last_search.txt"

with open(text_file_path,"r") as myfile:
    try:
        last_search=myfile.readlines()[0]
        last_search_date=last_search.split("_")[0]
    except:
        last_search_date='01-Jan-2019'

try:
    last_search_event_nr=last_search.split("_")[1]
    event_Nr=last_search_event_nr
except:
    event_Nr=1
try:
    last_search_event=last_search.split("_")[2]
    event_name=last_search_event
except:
    if championship == 'F2':
        event_name='BRN'
    else:
        event_name='ESP'

### Gathering TimeSectorAnalysis and RaceHistoryChart Files ###


type,data = mail.search(None, '(FROM "officialtiming@F1.com" SUBJECT '+ championship +' SINCE "' + last_search_date + '")')
mail_ids=data[0]
id_list = mail_ids.split()

for nums in id_list:
    typ, data = mail.fetch(nums, '(RFC822)' )
    raw_email = data[0][1]
    raw_email_string = raw_email.decode('utf-8')
    email_message = email.message_from_string(raw_email_string)

    for part in email_message.walk():
        # this part comes from the snipped I don't understand yet... 
        if part.get_content_maintype() == 'multipart':
            continue
        if part.get('Content-Disposition') is None:
            continue
        fileName = str(part.get_filename())
        if bool(fileName) and (('SectorAnalysis' in str(fileName)) or ('HistoryChart' in str(fileName))):
            if str(fileName).split('_')[2] != str(event_name):
                event_Nr=int(event_Nr)+1
                event_name=str(fileName).split('_')[2]
            if not os.path.isdir(main_path+str(event_Nr).zfill(2)+"_"+str(event_name)):
                os.mkdir(main_path+str(event_Nr).zfill(2)+"_"+event_name)
                filePath = os.path.join(main_path+str(event_Nr).zfill(2)+"_"+str(event_name), fileName)                
            else:
                filePath = os.path.join(main_path+str(event_Nr).zfill(2)+"_"+str(event_name), fileName)
            if not os.path.isfile(filePath):
                fp = open(filePath, 'wb')
                fp.write(part.get_payload(decode=True))
                fp.close()

#event_Nr=event_Nr
#event_name=event_name
                
### Gathering FIA Entry Lists ###
type,data = mail.search(None, '(FROM "cslenzak@fia.com" SINCE "' + last_search_date + '")')
mail_ids_ = data[0]
mail_ids=data[0]
id_list = mail_ids.split()


### Entry List ###

for nums in id_list:
    typ, data = mail.fetch(nums, '(RFC822)' )
    raw_email = data[0][1]
    raw_email_string = raw_email.decode('utf-8')
    email_message = email.message_from_string(raw_email_string)

    for part in email_message.walk():
        # this part comes from the snipped I don't understand yet... 
        if part.get_content_maintype() == 'multipart':
            continue
        if part.get('Content-Disposition') is None:
            continue
        fileName = str(part.get_filename())
#        print(fileName)
    
        
        if bool(fileName) and (('signed' in str(fileName)) and ('Doc' in str(fileName))): # or ('Admitted' in str(fileName)) or ('Entry' in str(fileName))):
            main_path=main_path# + "/" + str(event_Nr) + "_" + str(event_name)
            filePath = os.path.join(main_path, fileName)
            
            if not os.path.isfile(filePath):
                fp = open(filePath, 'wb')
                fp.write(part.get_payload(decode=True))
                fp.close()

today = date.today()
text_update=today.strftime('%d-%b-%Y')+'_'+str(event_Nr).zfill(2)+'_'+str(event_name)
print("Today's date:", today)
with open(text_file_path,"w") as myfile:
    myfile.write(text_update)
    myfile.close()
print('Finished')
print(event_Nr)
print(event_name)
input('Press enter to exit:')
    