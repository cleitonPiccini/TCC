import subprocess
import threading
import time
import sys
import os
sys.path.insert(0, "..")


def start_client (numero):
    os.system("python3 Client.py " + str(numero))    

arquivo_config = []

# Le arquivo de configuracao.
with open("ConfigOPC.txt", "r") as arquivo:
    for linha in arquivo:
        arquivo_config.append(linha.replace("\n",""))

numero_clientes = int(arquivo_config[1])

i = 1
t = []

while (i <= numero_clientes):
    
    nome = "thread N = " + (str (i))
    t.append(threading.Thread(target=start_client,args=(i,))) 
    t[i-1].start()
    i = i + 1