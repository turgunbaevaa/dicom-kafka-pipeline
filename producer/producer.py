import os
import pydicom
from PIL import Image
import numpy as np
from confluent_kafka import Producer
import json
import uuid
import datetime

# --- Configuration using environment variables ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'file-data') 

# To retrieve DICOM folder path from environment variable:
DICOM_FOLDER = os.getenv('DICOM_FOLDER', './dicom_scans')

print(f"Connecting to Kafka at: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Processing DICOM files from: {DICOM_FOLDER}")
print(f"Sending messages to Kafka topic: {KAFKA_TOPIC}")

kafka_config = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
producer = Producer(kafka_config)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to topic '{msg.topic()}' [{msg.partition()}] at offset {msg.offset()}")

def send_to_kafka(topic, data):
    try:
        producer.produce(topic, json.dumps(data).encode('utf-8'), callback=delivery_report)
        producer.poll(0)
    except Exception as e:
        print(f"Failed to produce message: {e}")

# Create output folder for images (relative to where the script is run *inside* the container)
output_base_folder = "images/" 
os.makedirs(output_base_folder, exist_ok=True)

try:
    if not os.path.isdir(DICOM_FOLDER):
        print(f"Error: DICOM folder '{DICOM_FOLDER}' not found. Please check volume mount and path.")
        exit(1) 

    files_processed = 0
    
    for root, _, filenames in os.walk(DICOM_FOLDER):
        for filename in filenames:
            if not filename.endswith(".dcm"):
                continue
            
            filepath = os.path.join(root, filename)
            
            if not os.path.isfile(filepath):
                print(f"Warning: File '{filepath}' not found or not accessible. Skipping.")
                continue

            try:
                ds = pydicom.dcmread(filepath)
            except Exception as e:
                print(f"Error reading DICOM file {filepath}: {e}. Skipping.")
                continue

            metadata = {
                "PatientID": ds.get("PatientID", "N/A"),
                "StudyDate": ds.get("StudyDate", "N/A"),
                "Modality": ds.get("Modality", "N/A"),
                "Manufacturer": ds.get("Manufacturer", "N/A"),
                "BodyPartExamined": ds.get("BodyPartExamined", "N/A"),
                "StudyDescription": ds.get("StudyDescription", "N/A"),
                "SOPInstanceUID": ds.get("SOPInstanceUID", str(uuid.uuid4())), 
                "timestamp": datetime.datetime.utcnow().isoformat()
            }

            if not hasattr(ds, 'pixel_array') or ds.pixel_array is None:
                print(f"No pixel data found for {filepath}. Sending metadata only.")
                doc = {
                    # Store the full path in the database
                    "original_dicom_file": filepath, 
                    "image_path": None, 
                    "metadata": metadata
                }
                send_to_kafka(KAFKA_TOPIC, doc)
                files_processed += 1
                continue

            array = ds.pixel_array
            
            if 'RescaleSlope' in ds and 'RescaleIntercept' in ds:
                array = array * ds.RescaleSlope + ds.RescaleIntercept

            # Determine the relative path to create mirrored subdirectories in output_base_folder
            relative_path_to_dicom_root = os.path.relpath(root, DICOM_FOLDER)
            output_image_folder = os.path.join(output_base_folder, relative_path_to_dicom_root)
            os.makedirs(output_image_folder, exist_ok=True) 

            # Get filename without extension for cleaner image names
            image_base_name = os.path.splitext(filename)[0]

            if len(array.shape) == 3: # Multi-frame DICOM (e.g., 3D or sequences)
                for i, slice_ in enumerate(array):
                    slice_norm = ((slice_ - slice_.min()) / (slice_.ptp() + 1e-6) * 255).astype(np.uint8)
                    img = Image.fromarray(slice_norm)
                    
                    img_name = f"{image_base_name}_slice_{i}_{uuid.uuid4().hex[:6]}.png"
                    img_path_in_container = os.path.join(output_image_folder, img_name)
                    img.save(img_path_in_container)
                    
                    doc = {
                        "original_dicom_file": filepath, 
                        "image_path": img_path_in_container, 
                        "slice_number": i,
                        "metadata": metadata
                    }
                    send_to_kafka(KAFKA_TOPIC, doc)
                    files_processed += 1
            # Single-frame DICOM:
            else: 
                image_norm = ((array - array.min()) / (np.ptp(array) + 1e-6) * 255).astype(np.uint8)
                img = Image.fromarray(image_norm)
                
                img_name = f"{image_base_name}_{uuid.uuid4().hex[:6]}.png"
                img_path_in_container = os.path.join(output_image_folder, img_name)
                img.save(img_path_in_container)
                
                doc = {
                    "original_dicom_file": filepath, 
                    "image_path": img_path_in_container, 
                    "metadata": metadata
                }
                send_to_kafka(KAFKA_TOPIC, doc)
                files_processed += 1

    print(f"✅ Producer: Processed {files_processed} files. All messages sent (or attempted).")

except Exception as e:
    print(f"An unexpected error occurred during processing: {e}")
finally:
    remaining_msgs = producer.flush(timeout=10) 
    if remaining_msgs > 0:
        print(f"Warning: {remaining_msgs} messages were not delivered.")
    else:
        print("All messages successfully delivered.")

print("Producer script finished.")