import grpc
from concurrent import futures
import os
import archivo_pb2
import archivo_pb2_grpc

class ArchivoServicer(archivo_pb2_grpc.ArchivoServicer):
    def ListarArchivos(self, request, context):
        directorio = "D:"
        
        try:
            archivos = os.listdir(directorio)
        except Exception as e:
            return archivo_pb2.ArchivoLista(archivos=[])

        return archivo_pb2.ArchivoLista(archivos=archivos)

def main():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    archivo_pb2_grpc.add_ArchivoServicer_to_server(ArchivoServicer(), server)
    server.add_insecure_port('[::]:5001')
    server.start()
    print("Microservicio mserv1 escuchando en el puerto 5001...")
    server.wait_for_termination()

if __name__ == '__main__':
    main()