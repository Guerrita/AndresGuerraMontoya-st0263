python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. archivo.proto  Genera el archivo: archivo_pb2_grpc.py

protoc --python_out=. archivo.proto  Genera el archivo: archivo_pb2.py