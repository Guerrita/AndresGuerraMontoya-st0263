import grpc
import archivo_pb2  # Importa el mensaje personalizado ArchivoVacio
import archivo_pb2_grpc
from flask import Flask, jsonify

MSERV1_URL = "localhost:5001"
MSERV2_URL = "localhost:5002"

class ApiGateway:
    def __init__(self):
        self.channel_mserv1 = grpc.insecure_channel(MSERV1_URL)
        self.stub_mserv1 = archivo_pb2_grpc.ArchivoStub(self.channel_mserv1)
        
        self.channel_mserv2 = grpc.insecure_channel(MSERV2_URL)
        self.stub_mserv2 = archivo_pb2_grpc.ArchivoStub(self.channel_mserv2)
    
    def listar_archivos(self):
        # Utiliza tu mensaje vacío personalizado ArchivoVacio()
        response = self.stub_mserv1.ListarArchivos(archivo_pb2.ArchivoVacio())
        print(response)
        return response.archivos
    
    def buscar_archivos(self):
        # Utiliza tu mensaje vacío personalizado ArchivoVacio()
        response = self.stub_mserv2.BuscarArchivos(archivo_pb2.ArchivoVacio())
        return response.archivos

app = Flask(__name__)
api_gateway = ApiGateway()

@app.route('/listar_archivos', methods=['GET'])
def listar_archivos():
    archivos = api_gateway.listar_archivos()
    archivos_serializable = list(archivos)  # Convertir a lista de Python
    return jsonify(archivos_serializable), 200


@app.route('/buscar_archivos', methods=['GET'])
def buscar_archivos():
    archivos = api_gateway.buscar_archivos()
    archivos_serializable = list(archivos)  # Convertir a lista de Python
    return jsonify(archivos_serializable), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
