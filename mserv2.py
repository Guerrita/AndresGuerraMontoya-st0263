import grpc
from concurrent import futures
import archivo_pb2
import archivo_pb2_grpc
from dotenv import load_dotenv
import os

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

MSERV2_PORT = os.getenv("MSERV2_PORT")

class ArchivoServicer(archivo_pb2_grpc.ArchivoServicer):
    # def BuscarArchivos(self, request, context):
    #     return archivo_pb2.ArchivoLista(archivos=['archivo1.txt'])
    def BuscarArchivos(self, request, context):
        directorio = "./files"
        archivo = os.listdir(directorio + nombre_archivo)
        print(archivo)
        nombre_archivo = request.nombre_archivo
        archivo_path = os.path.join('./files', nombre_archivo)
        print(os.path.exists(archivo_path))

        if os.path.exists(archivo_path):
            return archivo_pb2.ArchivoLista(archivos=[nombre_archivo])
        else:
            return archivo_pb2.ArchivoLista(archivos=[])

def main():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    archivo_pb2_grpc.add_ArchivoServicer_to_server(ArchivoServicer(), server)
    server.add_insecure_port(f'[::]:{MSERV2_PORT}')
    server.start()
    print(f"Microservicio mserv2 escuchando en el puerto {MSERV2_PORT}...")
    server.wait_for_termination()

if __name__ == '__main__':
    main()