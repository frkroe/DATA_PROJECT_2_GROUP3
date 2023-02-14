import docker
import sys, getopt
import time
import os
import uuid
import random

print("#############################")
print("Starting Generator execution")
print("#############################")

#Predeterminar las variables a utilizar.
topcontainers = 0
elapsedtime = 0
containername=""

#Lista de contenedores que se van a levantar.
containers=[]

def getcontainers():
    #lista comandos en ejecución y "grep" busca las líneas donde está el contenedor
    cmd=f"docker ps | grep -c {containername}"
    #para ejecutar el comando en el sistema operativo y leer su salida. Convierte la salida en un entero.
    stream = os.popen(cmd)
    output = stream.read()
    return int(output)

#Función para generar un ID de cada contenedor que simula un sensor diferente.
def genuserid():
    return uuid.uuid4().hex
#Función para eliminar el contenedor mediante identificador.
def deletecontainer(container_id):
    cmd=f"docker container rm {container_id} -f "
    stream = os.popen(cmd)
    output = stream.read()
    containers.remove(container_id)
    print(f"Container Removed with id: {container_id}")

#Crea el contenedor:
def createcontainer():
    #Definición de variables globales
    global containername
    global elapsedtime
    global topcontainers
    global containers
    #Llama a la función anterior que crea el ID unico.
    userid=genuserid()
    #Comando para levantar el docker:
    cmd=f"docker run -e TIME_ID={elapsedtime} -e USER_ID={userid} -d {containername}:latest"
    #para ejecutar el comando en el sistema operativo y leer su salida. Convierte la salida en un entero.
    stream = os.popen(cmd)
    output = stream.read().replace("\n","")
    containers.append(output)
    print(f"Container Created with id: {output} for user: {userid}")
    return output
#Función para tomar la lista de argumentos 
def main(argv):
   global containername
   global elapsedtime
   global topcontainers
   #Procesa la lista mediante "getop.getop"
   try:
      opts, args = getopt.getopt(argv,"t:e:i:",["topcontainers=","elapsedtime=","image="])
    #Si no contempla la opción anterior, sale del contenedor con el status "2"
   except getopt.GetoptError:
      print('main.py -t <topcontainers> -e <elapsedtime> -i <image>')
      sys.exit(2)
    #Itera en el bulce dentro de la tupla "opts" y comprueba si conincde con alguna de las opciones esperadas:
   for opt, arg in opts:
      if opt in ("-h", "--help"):
         print('main.py -t <topcontainers> -e <elapsedtime> -n <image>')
         print(" elapsedtime: int (seconds)")
         print(" topcotainers: int (top number of concurrent clients)")
         print(" image: string (image name)")
         sys.exit()
      elif opt in ("-t", "--topcontainers"):
         topcontainers = int(arg)
      elif opt in ("-e", "--elapsedtime"):
         elapsedtime = int(arg)
      elif opt in ("-i", "--image"):
         containername = arg
   print(f"Top Containers: {topcontainers}")
   print(f"Elapsed Time: {elapsedtime}")
   print(f"Container name: {containername}")
#Llama al main:
if __name__ == "__main__":
   main(sys.argv[1:])



while True:
#Guarda el resultado de la funcion "getcontainers" en la variale "numcon". Saca por consola el número de contenedores
   numcon=getcontainers()
   print(f"Currently running containers: {len(containers)}")
#Si se cumple la siguiente condicion, se genera un número aleatorio entre 0 y "topcontainers" para determinar el número de contenedores a crear
   if numcon<topcontainers:
    create=random.randint(0,topcontainers-numcon)
    print(f"Containers to be created: {create}")
    #Llamamos a la función "containers" tantas veces como conetenedoress se va a crear (definido en la parte superior)
    for i in range(0,create):
        createcontainer()
     #Si "numcon>=topcontainers" no se crearan más contenedores. 
   else:
    print("No more containers can be created") 
   time.sleep(2)
   #En el bucle for, se asigna un número aleatorio de 0 al 10 a cada contenedor. 
   for item in containers:
    prob=random.randint(0, 10)
    #Si el número asignado es 0, el contenedor se borrará y ya se podrá generar un nuevo contenedor.
    #Cada contenedor tiene un 10% de probabilidad de ser borrado.
    if prob == 0:
        # 10% probabilidad de eliminar container
        deletecontainer(item)
    
   time.sleep(1)