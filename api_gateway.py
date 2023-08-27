import grpc
import archivo_pb2  
import archivo_pb2_grpc
from flask import Flask, jsonify
import pika 
import threading
from dotenv import load_dotenv
import os
import atexit  


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

        self.rabbitmq_connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.rabbitmq_channel = self.rabbitmq_connection.channel()
        self.rabbitmq_channel.queue_declare(queue='request_queue')

    def _purge_queued_requests(self):
        with self.rabbitmq_connection:
            self.rabbitmq_channel.queue_purge(queue='request_queue')
    
    def _enqueue_request(self, method_name):
        with self.rabbitmq_connection:
            self.rabbitmq_channel.basic_publish(exchange='', routing_key='request_queue', body=method_name)

    def _process_queued_requests(self):
        for method_frame, properties, body in self.rabbitmq_channel.consume('request_queue'):
            if body:
                try:
                    method_name = body.decode('utf-8')  # Decodifica el cuerpo del mensaje
                    # Procesar la petición encolada
                    if method_name == "listar_archivos":
                        self.listar_archivos()
                        
                    # elif method_name == "buscar_archivos":
                    #     nombre_archivo = "buenas.txt"  # Define el nombre de archivo que deseas buscar
                    #     archivos = self.buscar_archivos_con_nombre(nombre_archivo)
                    #     print("Resultado de búsqueda:", archivos)
                except Exception as e:
                    print("Error al procesar petición:", str(e))
                try:
                    self.rabbitmq_channel.basic_ack(method_frame.delivery_tag)
                except Exception as e:
                    print("Error al tratar de remover la petición:", str(e))
    
        # Cerrar el canal después de que se hayan procesado todas las peticiones encoladas
        self.rabbitmq_channel.close()


    def listar_archivos(self):
        try:
            response = self.stub_mserv1.ListarArchivos(archivo_pb2.ArchivoVacio())
            return response.archivos
        except grpc.RpcError as e:
            self._enqueue_request("listar_archivos")
            return ["Error en comunicación gRPC, encolando petición..."]

    # def buscar_archivos(self):
    #     try:
    #         response = self.stub_mserv2.BuscarArchivos(archivo_pb2.ArchivoVacio())
    #         return response.archivos
    #     except grpc.RpcError as e:
    #         self._enqueue_request("buscar_archivos")
    #         return ["Error en comunicación gRPC, encolando petición..."]

    # def buscar_archivos_con_nombre(self, nombre_archivo):
    #     try:
    #         request = archivo_pb2.ArchivoRequest(nombre_archivo=nombre_archivo)
    #         response = self.stub_mserv2.BuscarArchivos(request)
    #         return response.archivos
    #     except grpc.RpcError as e:
    #         self._enqueue_request("buscar_archivos")
    #         return ["Error en comunicación gRPC, encolando petición..."]

app = Flask(__name__)
api_gateway = ApiGateway()

@app.route('/listar_archivos', methods=['GET'])
def listar_archivos():
    archivos = api_gateway.listar_archivos()
    archivos_serializable = list(archivos)  # Convertir a lista de Python
    return jsonify(archivos_serializable), 200


# @app.route('/buscar_archivos/<nombre_archivo>', methods=['GET'])
# def buscar_archivos_con_parametro(nombre_archivo):
#     archivos = api_gateway.buscar_archivos_con_nombre(nombre_archivo)
#     archivos_serializable = list(archivos)  # Convertir a lista de Python
#     return jsonify(archivos_serializable), 200


if __name__ == '__main__':
    queued_requests_thread = threading.Thread(target=api_gateway._process_queued_requests)
    queued_requests_thread.start()

    app.run(host='0.0.0.0', port=5000)
