'''
Sends email alerts for errors/notifications
Specify recepient emails in emails.txt
'''

import smtplib, ssl
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__)))

#send email to handle error
def send_email(message):
	server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
	server.login("xiao@virgilqr.com", "921211Rx")
	senders = []
	filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "emails.txt")
	with open (filepath, "r") as f:
		lines = f.readlines()
		lines.pop(0)
		for line in lines:
			senders.append(line)
	server.sendmail(msg=message, from_addr="xiao@virgilqr.com", to_addrs=senders)
	server.quit()
    
