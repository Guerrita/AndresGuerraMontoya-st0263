import grpc
from concurrent import futures
import os
import archivo_pb2
import archivo_pb2_grpc
from dotenv import load_dotenv
import os

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

MSERV1_PORT = os.getenv("MSERV1_PORT")


class ArchivoServicer(archivo_pb2_grpc.ArchivoServicer):
    def ListarArchivos(self, request, context):
        directorio = "./files"
        
        try:
            archivos = os.listdir(directorio)
        except Exception as e:
            return archivo_pb2.ArchivoLista(archivos=[])

        print("Petición 'listar_archivos' procesada")
        return archivo_pb2.ArchivoLista(archivos=archivos)

def main():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    archivo_pb2_grpc.add_ArchivoServicer_to_server(ArchivoServicer(), server)
    server.add_insecure_port(f"[::]:{MSERV1_PORT}")
    server.start()
    print(f"Microservicio mserv1 escuchando en el puerto {MSERV1_PORT}...")
    server.wait_for_termination()

if __name__ == '__main__':
    main()