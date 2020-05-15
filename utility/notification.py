import datetime
import smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders

import sys 
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)


# send eamil 
def send_error_message(subject,error_message):
    current_time = datetime.datetime.utcnow()
    body = 'script breaks at time {}\n'.format(current_time) + 'Error message is ' + error_message 
    message = 'Subject: {}\n\n{}'.format(subject,body)
    server = smtplib.SMTP_SSL('smtp.gmail.com',465)
    server.login("xiao@virgilqr.com","921211Rx")
    server.sendmail(msg = message, from_addr="vpfa.reports@gmail.com", to_addrs=["xiao@virgilqr.com"])
    server.quit()
    
    

# send email with attachment
def send_email_attachment(file_name):
    msg = MIMEMultipart()
    msg['From'] = "monitor"
    msg['To'] = "xiao@virgilqr.com"
    msg['Date'] = formatdate(localtime = True)
    msg['Subject'] = "InfluxDB Monitor"
    text = "The attached below is the most recent data entry"
    msg.attach(MIMEText(text))

    part = MIMEBase('application', "octet-stream")
    part.set_payload(open(file_name, "rb").read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment; filename="{}"'.format(file_name))
    msg.attach(part)

    smtp = smtplib.SMTP('smtp.gmail.com',587)
    smtp.starttls()
    smtp.login("xiao@virgilqr.com","921211Rx")
    smtp.sendmail("monitor",["xiao@virgilqr.com","nasir@virgilqr.com"], msg.as_string())
    smtp.quit()


def send_email_dataframe_content(df_test,subject):
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = 'xiao@virgilqr.com'

    html = '{0}'.format(df_test.render())

    part1 = MIMEText(html, 'html')
    msg.attach(part1)

    smtp = smtplib.SMTP('smtp.gmail.com',587)
    smtp.starttls()
    smtp.login("vpfa.reports@gmail.com","921211@Rx")
    smtp.sendmail("monitor",["vpfa.reports@gmail.com","nasir@virgilqr.com"], msg.as_string())
    smtp.quit()
    
    
    
