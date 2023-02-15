# Improvement attempts
 
 Even though the 3 PCollections for extracting and transforming the measured data of temperature, pressure and motor power will be successfully sent to the same Pub/Sub Topic, there is still room for improvement. 

 The main objective is to **reduce the number of messages** sent to the Pub/Sub Topic and then the number of emails being triggered by Cloud Functions. 

 At the moment, a message (& email) will be published for each data type (temperature, pressure and motor power). In this folder you'll find some *Python Scripts* where it was intended to combine the 3 Pollections together with a new one for the time when the data was being generated/ collected. 

We managed to join the 4 PCollections but we cannot figure out how to convert it correctly into a dictionary for later encoding and publishing it to Pub/Sub. 


**Feel free to give us feedback and/ or create a Pull Request.**

 Lots of love from Valencia <3

 ![Help](https://media.tenor.com/pwhTYF9q7IIAAAAM/lord-pleas-help-oh-no.gif)