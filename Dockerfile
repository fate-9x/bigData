# Usa una imagen base de Python
FROM python:3.9-slim

# Establece el directorio de trabajo en el contenedor
WORKDIR /app

# Copia los archivos de requerimientos al contenedor
COPY requirements.txt requirements.txt

# Instala las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copia el script al contenedor
COPY script.py script.py

# Copia el directorio de datos al contenedor
COPY data /app/data

# Ejecuta el script cuando se inicie el contenedor
CMD ["python", "script.py"]
