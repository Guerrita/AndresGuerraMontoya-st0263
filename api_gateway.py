import grpc
import archivo_pb2  
import archivo_pb2_grpc
from flask import Flask, jsonify
import pika 
import threading
from dotenv import load_dotenv
import os
import atexit
import functools


# Cargar las variables de entorno desde el archivo .env
load_dotenv()

MSERV1_URL = os.getenv("MSERV1_URL")
MSERV2_URL = os.getenv("MSERV2_URL")

class ApiGateway:
    def __init__(self):
        self.channel_mserv1 = grpc.insecure_channel(MSERV1_URL)
        self.stub_mserv1 = archivo_pb2_grpc.ArchivoStub(self.channel_mserv1)
        
        self.channel_mserv2 = grpc.insecure_channel(MSERV2_URL)
        self.stub_mserv2 = archivo_pb2_grpc.ArchivoStub(self.channel_mserv2)
        credentials = pika.PlainCredentials('guest', 'guest')
        self.rabbitmq_connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost', credentials=credentials)
        )
        self.rabbitmq_channel = self.rabbitmq_connection.channel()
        self.rabbitmq_channel.queue_delete(queue='request_queue')
        self.rabbitmq_channel.queue_declare(queue='request_queue')

        self.rabbitmq_lock = threading.Lock()

    def __del__(self):
        # Cerrar conexiones y canales al destruir la instancia
        self.rabbitmq_channel.close()
        self.rabbitmq_connection.close()

    def _enqueue_request(self, method_name):
        try:
            with self.rabbitmq_lock:
                if self.rabbitmq_channel.is_open:
                    # Realiza la encolación solo si el canal está abierto
                    with self.rabbitmq_connection:
                        self.rabbitmq_channel.basic_publish(exchange='', routing_key='request_queue', body=method_name)
                else:
                    print("El canal está cerrado, no se pudo encolar la petición.")
        except Exception as e:
            print("Error al encolar petición:", str(e))

    def _process_queued_requests(self):
        for method_frame, properties, body in self.rabbitmq_channel.consume('request_queue'):
            if body:
                try:
                    method_name = body.decode('utf-8')  # Decodifica el cuerpo del mensaje
                    # Procesar la petición encolada
                    if method_name == "listar_archivos":
                        self.listar_archivos()
                        print("Petición 'listar_archivos' procesada")
                    elif method_name.startswith("buscar_archivos:"):
                        nombre_archivo = method_name.split(":")[1]  # Extraer el nombre del archivo
                        self.buscar_archivos_con_nombre(nombre_archivo)
                        print(f"Petición 'buscar_archivos' para '{nombre_archivo}' procesada")
                except Exception as e:
                    print("Error al procesar petición:", str(e))
                self.rabbitmq_channel.basic_ack(method_frame.delivery_tag)  # Confirma la recepción del mensaje

    def listar_archivos(self):
        try:
            response = self.stub_mserv1.ListarArchivos(archivo_pb2.ArchivoVacio())
            archivos = list(response.archivos)  # Convertir a lista de Python
            return archivos
        except grpc.RpcError as e:
            print("Error en comunicación gRPC, encolando petición...")
            self._enqueue_request("listar_archivos")
            return ["Error en comunicación gRPC, encolando petición..."]

    def buscar_archivos_con_nombre(self, nombre_archivo):
        try:
            request = archivo_pb2.ArchivoRequest(nombre_archivo=nombre_archivo)
            response = self.stub_mserv2.BuscarArchivos(request)
            archivos = list(response.archivos)  # Convertir a lista de Python
            return archivos
        except grpc.RpcError as e:
            print("Error en comunicación gRPC, encolando petición...")
            self._enqueue_request(f"buscar_archivos:{nombre_archivo}")
            return ["Error en comunicación gRPC, encolando petición..."]

app = Flask(__name__)
api_gateway = ApiGateway()

@app.route('/listar_archivos', methods=['GET'])
def listar_archivos_handler():
    archivos = api_gateway.listar_archivos()
    return jsonify(archivos), 200

@app.route('/buscar_archivos/<nombre_archivo>', methods=['GET'])
def buscar_archivos_handler(nombre_archivo):
    archivos = api_gateway.buscar_archivos_con_nombre(nombre_archivo)
    return jsonify(archivos), 200

if __name__ == '__main__':
    queued_requests_thread = threading.Thread(target=api_gateway._process_queued_requests)
    queued_requests_thread.start()

    app.run(host='0.0.0.0', port=5000)
