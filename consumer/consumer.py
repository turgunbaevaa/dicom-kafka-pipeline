import os
from confluent_kafka import Consumer
from pymongo import MongoClient
import json
import pydicom
import matplotlib.pyplot as plt
import numpy as np

# --- Configuration using environment variables ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://mongo:27017/') 
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'file-data')

# Define the base path inside the container where images will be saved
IMAGE_BASE_OUTPUT_DIR = "/app/images"

# Ensure the base output directory exists in the container
os.makedirs(IMAGE_BASE_OUTPUT_DIR, exist_ok=True)

kafka_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'dicom_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(kafka_config)
consumer.subscribe([KAFKA_TOPIC])

client = MongoClient(MONGO_URI)
db = client["dicom_database"]
collection = db["scans"]

print("📡 Consumer запущен...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == -195: # RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN
                print("❌ Ошибка: Все брокеры Kafka недоступны. Повторная попытка...")
            else:
                print("❌ Ошибка:", msg.error())
            continue
        
        try:
            data = json.loads(msg.value().decode('utf-8'))
            
            original_dicom_path_in_container = data.get('original_dicom_file')
            
            if original_dicom_path_in_container:
                try:
                    ds = pydicom.dcmread(original_dicom_path_in_container)
                    pixel_array = ds.pixel_array
                    
                    if pixel_array.dtype != np.uint8:
                         pixel_array = (np.maximum(pixel_array, 0) / pixel_array.max()) * 255.0
                         pixel_array = np.uint8(pixel_array)

                    # --- MODIFICATION START ---
                    # Extract the relative path from /app/dicom_scans/
                    # e.g., 'case1/case3c_002.dcm'
                    relative_dicom_path = os.path.relpath(original_dicom_path_in_container, '/app/dicom_scans')
                    
                    # Get the directory part (e.g., 'case1')
                    # This handles nested subfolders if you ever have them
                    case_folder = os.path.dirname(relative_dicom_path) # e.g., 'case1' or 'case2'
                    
                    # Construct the output directory for this case (e.g., /app/images/case1)
                    png_case_output_dir = os.path.join(IMAGE_BASE_OUTPUT_DIR, case_folder)
                    
                    # Ensure this specific case directory exists
                    os.makedirs(png_case_output_dir, exist_ok=True)
                    
                    # Get the base filename (e.g., 'case3c_002')
                    base_filename = os.path.basename(original_dicom_path_in_container).replace('.dcm', '')
                    
                    # Construct the full path for the PNG (e.g., /app/images/case1/mri_case3c_002.png)
                    png_filename = f"mri_{base_filename}.png"
                    png_output_full_path = os.path.join(png_case_output_dir, png_filename)
                    # --- MODIFICATION END ---

                    plt.imshow(pixel_array, cmap=plt.cm.bone)
                    plt.axis('off')
                    plt.savefig(png_output_full_path, bbox_inches='tight', pad_inches=0)
                    plt.close()

                    print(f"✅ Сохранено PNG: {png_output_full_path}")
                    
                    data['generated_png_path'] = png_output_full_path
                    
                except Exception as img_e:
                    print(f"❌ Ошибка при обработке или сохранении изображения для {original_dicom_path_in_container}: {img_e}")
            else:
                print("⚠️ Предупреждение: 'original_dicom_file' не найден в сообщении Kafka.")

            collection.insert_one(data)
            print(f"✅ Сохранено метаданные в Mongo для: {data.get('original_dicom_file')}")

        except json.JSONDecodeError as e:
            print(f"❌ Ошибка декодирования JSON: {e} - Сообщение: {msg.value().decode('utf-8', errors='ignore')}")
        except Exception as e:
            print(f"❌ Ошибка при сохранении в MongoDB или общей обработке: {e} - Сообщение: {data}")

except KeyboardInterrupt:
    print("🛑 Остановлено вручную.")
finally:
    consumer.close()
    print("Consumer завершил работу.")