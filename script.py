import requests
from bs4 import BeautifulSoup
import os
import zipfile
import pandas as pd
import shutil
from google.cloud import storage
from google.cloud import pubsub_v1

# Establece la ruta de las credenciales y la variable de entorno
credential_path = "../credential/analog-period-417715-3e7d75d7dcd0.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

# Configura tu cliente de Google Cloud Storage
storage_client = storage.Client()
bucket_name = 'bucket_bigdataa'
bucket = storage_client.bucket(bucket_name)

# Configura el cliente de Pub/Sub
topic_path = "projects/analog-period-417715/topics/bigdata"
publisher = pubsub_v1.PublisherClient()

def publish_message(message):
    data = message.encode("utf-8")
    future = publisher.publish(topic_path, data)
    print(f"Published message ID: {future.result()}")

# URL de la página que vamos a scrape
url = 'https://www.dtpm.cl/index.php/noticias/gtfs-vigente'

# Directorios de destino
txt_folder = 'data/txt_files'
csv_folder = 'data/csv_files'

# Crear carpetas para .txt y .csv
os.makedirs(txt_folder, exist_ok=True)
os.makedirs(csv_folder, exist_ok=True)

def main():
    # Realiza la solicitud HTTP a la página
    response = requests.get(url)

    # Comprueba que la solicitud fue exitosa
    if response.status_code == 200:
        # Parsear el contenido HTML de la página
        soup = BeautifulSoup(response.content, 'html.parser')

        # Busca el enlace que contiene 'GTFS' en el texto
        link = soup.find('a', href=True, text='GTFS')

        if link:
            # Obtén el valor del atributo href
            href = link['href']
            # Construye la URL completa
            file_url = f"https://www.dtpm.cl{href}"

            # Descarga el archivo
            file_response = requests.get(file_url)
            
            # Comprueba que la solicitud para descargar el archivo fue exitosa
            if file_response.status_code == 200:
                # Nombre del archivo a guardar
                file_name = os.path.basename(href)
                local_zip_path = os.path.join('/tmp', file_name)
                
                # Guarda el archivo en la máquina local
                with open(local_zip_path, 'wb') as file:
                    file.write(file_response.content)
                    
                print(f"Archivo descargado y guardado como {local_zip_path}")

                # Descomprime el archivo
                with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
                    zip_ref.extractall('/tmp/extracted_files')
                
                print("Archivo descomprimido")

                # Convertir archivos .txt a .csv y mover archivos
                extracted_path = '/tmp/extracted_files'
                for root, dirs, files in os.walk(extracted_path):
                    for file in files:
                        if file.endswith('.txt'):
                            txt_file_path = os.path.join(root, file)
                            txt_destination_path = os.path.join(txt_folder, file)
                            csv_file_path = txt_file_path.replace('.txt', '.csv')
                            csv_destination_path = os.path.join(csv_folder, os.path.basename(csv_file_path))

                            # Mover el archivo .txt a la carpeta correspondiente y reemplazar si existe
                            shutil.move(txt_file_path, txt_destination_path)
                            
                            # Leer el archivo .txt y guardar como .csv, reemplazando si existe
                            df = pd.read_csv(txt_destination_path, delimiter=',')  # Ajusta el delimitador si es necesario
                            df.to_csv(csv_destination_path, index=False)
                            
                            print(f"Convertido {txt_destination_path} a {csv_destination_path}")
                            
                            # Subir el archivo .csv al bucket de Cloud Storage
                            blob = bucket.blob(f'csv_files/{os.path.basename(csv_destination_path)}')
                            blob.upload_from_filename(csv_destination_path)
                            print(f"Archivo subido a Cloud Storage: {csv_destination_path}")

                # Enviar un mensaje a Pub/Sub al finalizar la carga de datos
                publish_message("Se cargaron los datos correctamente.")
            else:
                print(f"Error al descargar el archivo: {file_response.status_code}")
                publish_message(f"Error al cargar los datos: {file_response.status_code}")
        else:
            print("Enlace no encontrado")
    else:
        print(f"Error al acceder a la página: {response.status_code}")

if __name__ == "__main__":
    main()
