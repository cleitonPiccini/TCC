import paho.mqtt.client as mqtt
import time
import sys
sys.path.insert(0, "..")
import threading, time, random
import os
import psutil
import xlsxwriter
import datetime
import math

semaforo = threading.Semaphore(2)
workbook = None
worksheet = None
start_mensagens = 0
client = mqtt.Client()

def carga_cpu ():    
    #Obtem a carga da CPU
    carga_processador = psutil.cpu_percent()
    return carga_processador

def carga_ram ():
    #Obtem a carga da memória
    total_memory, used_memory, free_memory = map( int, os.popen('free -t -m').readlines()[-1].split()[1:]) 
    carga_memoria = (round((used_memory/total_memory) * 100, 2))
    return carga_memoria

def Write_Excell (indice, dado_A, dado_B, dado_C, dado_D, dado_E):
    global worksheet
                
    #Salva os dados no arquivo.
    coluna = 'A'+str(indice)
    worksheet.write(coluna, dado_A)
    coluna = 'B'+str(indice)
    worksheet.write(coluna, dado_B)
    coluna = 'C'+str(indice)
    worksheet.write(coluna, dado_C)
    coluna = 'D'+str(indice)
    worksheet.write(coluna, dado_D)
    coluna = 'E'+str(indice)
    worksheet.write(coluna, dado_E)
    
# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    global start_mensagens, semaforo
           
    # Confirmacao de Publicação.
    if start_mensagens == 1 and Teste_Ativo == 2:
        dado_broker = msg.payload
        semaforo.release()
        semaforo.release()
    elif start_mensagens == 1:
        semaforo.release()
        semaforo.release()

def start_sub_thread ():
    global client
    client.loop_forever()

def Start(numero_cliente, end, porta, numero_mensagens, tamanho_inicio, tamanho_fim, tipo_teste):
    global worksheet, workbook, Teste_Ativo
    global start_mensagens, semaforo, client

    client.connect( end, porta, 60)
    client.on_connect = on_connect
    client.on_message = on_message

    Teste_Ativo = tipo_teste

    
    t = threading.Thread(target=start_sub_thread,args=())
    t.start()

    try:
        print("Inicio do teste - Cliente Número = ", numero_cliente)
        
        topico_data = "Teste/Data_" + str(numero_cliente)
        topico_echo = "Teste/Echo_" + str(numero_cliente)
        topico_ack = "Teste/Ack_" + str(numero_cliente)
        topico_var_global = "Teste/Variavel"
        
        time.sleep(2)
        
        # Seleciona o tipo de teste. E cria a assinatura das variaveis.
        if tipo_teste == 1:
            nome_xlsx = "Dados Testes/MQTT Teste Ack Cliente - " + str(numero_cliente) + ".xlsx"
            workbook = xlsxwriter.Workbook(nome_xlsx) 
            worksheet = workbook.add_worksheet() 
            Write_Excell(1,'Tempo de ACK', 'Desvio Padrão', 'Carga Processador', 'Carga Memoria RAM', 'Tamanho do dado')
            client.subscribe(topico_data)
            
        elif tipo_teste == 2:
            nome_xlsx = "Dados Testes/MQTT Teste Echo Cliente - " + str(numero_cliente) + ".xlsx"
            workbook = xlsxwriter.Workbook(nome_xlsx) 
            worksheet = workbook.add_worksheet() 
            Write_Excell(1,'Tempo de Echo', 'Desvio Padrão', 'Carga Processador', 'Carga Memoria RAM', 'Tamanho do dado')
            client.subscribe(topico_echo)
    
        else:
            print("Erro no tipo de teste")
        
        # Atribui o valor inicial do dado para o servidor.
        dado = "a"
        contador_tamanho = 1
        while (contador_tamanho < int(tamanho_inicio)):
            dado = dado + dado
            contador_tamanho = contador_tamanho + 1

        # Variaveis de resultado do teste.
        contador_mensagens = 0
        aux_contador_mensagens = 0
        contador_arquivo = 2
        desvio_padrao = 0
        amostra = []
        cpu_old = 0
        ram_old = 0

        # Inicia a troca de mensagens com o Servidor.
        while (contador_tamanho <= int(tamanho_fim)):
            
            # Teste Ack             
            if tipo_teste == 1:               
                if contador_mensagens < numero_mensagens :
                    
                    start_mensagens = 1
                    mensagem = dado + str(contador_mensagens)
                    Inicio_Timer = datetime.datetime.now()
                    
                    semaforo.acquire()
                    client.publish(topico_data, mensagem)
                    semaforo.acquire()

                    #Obtem o tempo da troca de dados.
                    atraso_mensagem = datetime.datetime.now() - Inicio_Timer
                    amostra.append(atraso_mensagem.total_seconds() * 1000)

                    cpu_old = carga_cpu() + cpu_old
                    media_cpu = cpu_old / (aux_contador_mensagens + 1)
                    ram_old = carga_ram() + ram_old
                    media_ram = ram_old / (aux_contador_mensagens + 1)
                    
                    aux_contador_mensagens = aux_contador_mensagens + 1
                    contador_mensagens = contador_mensagens + 1
                else:
                    print("Trocou o tamanho = ", contador_tamanho, "Cliente = ", numero_cliente)
                    # Cálculo da média.
                    indice = 0
                    somatorio = 0
                    while (indice < len(amostra)):
                        somatorio = somatorio + amostra[indice]
                        indice = indice +1
                    media_tempo = somatorio / len(amostra)
                    
                    # Cálcula diferança
                    indice = 0
                    while (indice < len(amostra)):
                        amostra[indice] = (amostra[indice] - media_tempo)**2
                        indice = indice +1
                    # Somatório para desvio padrão.
                    indice = 0
                    somatorio = 0
                    while (indice < len(amostra)):
                        somatorio = somatorio + amostra[indice]
                        indice = indice +1
                    # Calcula desvio padrão.
                    desvio_padrao = math.sqrt((somatorio / len(amostra)))
                    #Salva os dados no arquivo.
                    Write_Excell(contador_arquivo, media_tempo, desvio_padrao, media_cpu, media_ram, contador_tamanho)
                    dado = dado * 2
                    contador_mensagens = 0
                    aux_contador_mensagens = 0
                    cpu_old = 0
                    ram_old = 0
                    contador_tamanho = contador_tamanho * 2
                    contador_arquivo = contador_arquivo + 1
                    amostra.clear()
                    amostra = []
                    
                time.sleep(0.5)
            
            # Teste Echo
            elif tipo_teste == 2:

                if contador_mensagens < numero_mensagens :
                    
                    start_mensagens = 1

                    # Envia mensagem
                    mensagem = dado + str(contador_mensagens)
                    Inicio_Timer = datetime.datetime.now()
                    semaforo.acquire()
                    client.publish(topico_echo, mensagem)
                    semaforo.acquire()

                    #Obtem o tempo da troca de dados.
                    atraso_mensagem = datetime.datetime.now() - Inicio_Timer
                    amostra.append(atraso_mensagem.total_seconds() * 1000)
                    
                    cpu_old = carga_cpu() + cpu_old
                    media_cpu = cpu_old / (aux_contador_mensagens + 1)
                    ram_old = carga_ram() + ram_old
                    media_ram = ram_old / (aux_contador_mensagens + 1)
                    
                    aux_contador_mensagens = aux_contador_mensagens + 1
                    contador_mensagens = contador_mensagens + 1
                else:
                    print("Trocou o tamanho = ", contador_tamanho, "Cliente = ", numero_cliente)
                    # Cálculo da média.
                    indice = 0
                    somatorio = 0
                    while (indice < len(amostra)):
                        somatorio = somatorio + amostra[indice]
                        indice = indice +1
                    media_tempo = somatorio / len(amostra)
                    
                    # Cálcula diferança
                    indice = 0
                    while (indice < len(amostra)):
                        amostra[indice] = (amostra[indice] - media_tempo)**2
                        indice = indice +1
                    # Somatório para desvio padrão.
                    indice = 0
                    somatorio = 0
                    while (indice < len(amostra)):
                        somatorio = somatorio + amostra[indice]
                        indice = indice +1
                    # Calcula desvio padrão.
                    desvio_padrao = math.sqrt((somatorio / len(amostra)))
                    #Salva os dados no arquivo.
                    Write_Excell(contador_arquivo, media_tempo, desvio_padrao, media_cpu, media_ram, contador_tamanho)
                    # Dobra a carga de envio
                    dado = dado * 2
                    # Reseta os dados.
                    amostra.clear()
                    amostra = []
                    contador_mensagens = 0
                    aux_contador_mensagens = 0
                    cpu_old = 0
                    ram_old = 0
                    contador_tamanho = contador_tamanho * 2
                    contador_arquivo = contador_arquivo + 1
                    
                time.sleep(0.5)

            else:
                break

        time.sleep(2)
        workbook.close()
        time.sleep(2)
        
    finally:
        client.disconnect()
        print("Fim do teste Cliente Número = ", numero_cliente)

def main(args):
    return args[1]

if __name__ == "__main__":

    arquivo_config = []

    # Le arquivo de configuracao.
    with open("ConfigMQTT.txt", "r") as arquivo:
        for linha in arquivo:
            arquivo_config.append(linha.replace("\n",""))

    numero_clientes = int(arquivo_config[1])
    end = arquivo_config[3]
    porta = int(arquivo_config[5])
    numero_mensagens = int(arquivo_config[7])
    tamanho_inicio = int(arquivo_config[9])
    tamanho_fim = int(arquivo_config[11])
    tipo_teste = int(arquivo_config[13])            

    # Obtem o numero do processo que iniciou o Client.
    numero = main(sys.argv)
    Start(numero, end, porta, numero_mensagens, tamanho_inicio, tamanho_fim, tipo_teste)
