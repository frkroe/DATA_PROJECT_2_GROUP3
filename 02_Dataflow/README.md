# Dataflow
The dataflow being created in the Python Script *dataflow.py* indicates you how good the machine is working. 

For this purpose, the measured data of temperature, pressure and motor power are read in, analysed and assigned to one of the following Status Ranges depending on the value.

**Status green:**
- pressure: 60-70 mbar
- temperature: 45-47 ºC 
- motor power: 11-13

**Status yellow:**
- pressure: 58 >= x <60 and/or 70 > x <= 72
- temperature: 44 >= x < 45 and/or 47 > x <= 48
- motor power: 9 >= x < 11 and/or 13> x <= 15

**Status red:**
- pressure: x < 58 and/or x > 72
- temperature: x < 44 and/or x > 48
- motor power: x < 9 and/or x > 15