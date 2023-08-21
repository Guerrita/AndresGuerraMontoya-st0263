import grpc
from concurrent import futures
import archivo_pb2
import archivo_pb2_grpc

class ArchivoServicer(archivo_pb2_grpc.ArchivoServicer):
    def BuscarArchivos(self, request, context):
        # Lógica para buscar archivos en mserv2
        return archivo_pb2.ArchivoLista(archivos=['archivo1.txt'])

server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
archivo_pb2_grpc.add_ArchivoServicer_to_server(ArchivoServicer(), server)
server.add_insecure_port('[::]:5002')
server.start()
