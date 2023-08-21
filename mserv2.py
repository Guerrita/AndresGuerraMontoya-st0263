import grpc
from concurrent import futures
import archivo_pb2
import archivo_pb2_grpc

class ArchivoServicer(archivo_pb2_grpc.ArchivoServicer):
    def BuscarArchivos(self, request, context):
        return archivo_pb2.ArchivoLista(archivos=['archivo1.txt'])

def main():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    archivo_pb2_grpc.add_ArchivoServicer_to_server(ArchivoServicer(), server)
    server.add_insecure_port('[::]:5002')
    server.start()
    print("Microservicio mserv2 escuchando en el puerto 5002...")
    server.wait_for_termination()

if __name__ == '__main__':
    main()