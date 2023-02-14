import smtplib
import ssl
from email.message import EmailMessage
from datetime import datetime
import json
import base64
import logging

def pubsub_to_email(event, context):
    #Decode the Pub/Sub message and define the readable variable.
    logging.getLogger().setLevel(logging.INFO)
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    message = json.loads(pubsub_message)

    #Define variables to be used in the email conditions
    current_time = datetime.now()
    email_sender = 'sender_address' 
    email_password = 'sender_pwd'
    email_receiver = "receiver_address"
    
    #Stablish conditions depending of the Pub/Sub message entry as different emails are intended for different messages.
    if "red" in message["status"]:
    
        # Write the subject and body of the email to be sent depending of the input received from the Pub/Sub message.
        subject = 'Critical failure detected! Please repare immediately'
        body = f"""
        
        Status: {message["status"]}
        
        Mean: {message["mean"]}
        
        Registry time: {current_time}"""
        
        em = EmailMessage()
        em['From'] = email_sender
        em['To'] = email_receiver
        em['Subject'] = subject
        em.set_content(body)
        
    elif "yellow" in message["status"]:

        subject = 'Warning! Error detected!'
        body = f"""
        
        Status: {message["status"]}
        
        Processing time: {message["processingTime"]}
        
        Registry time: {current_time}"""
  
    else:
        #No emails are required when status is green, so in this case it simply prints an "OK message" in logs.
        print("No failures detected")
        
    # Add the security layer.
    context = ssl.create_default_context()
        
    # Logging into gmail using smtp library, adding the previously stablished security layer and send the email
    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
        smtp.login(email_sender, email_password)
        smtp.sendmail(email_sender, email_receiver, em.as_string())
