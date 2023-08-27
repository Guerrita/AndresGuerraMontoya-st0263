python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. archivo.proto  Genera el archivo: archivo_pb2_grpc.py

docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.9-management