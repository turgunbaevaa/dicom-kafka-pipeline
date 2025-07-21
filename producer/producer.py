import os
import sys  
from confluent_kafka import Producer
import json
import pydicom
import uuid
import datetime
from PIL import Image 
import numpy as np    

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092') 
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'file-data')
DICOM_FOLDER = os.getenv('DICOM_FOLDER', '/app/dicom_scans')
OUTPUT_BASE_FOLDER = os.getenv('OUTPUT_BASE_FOLDER', '/app/images') 

os.makedirs(OUTPUT_BASE_FOLDER, exist_ok=True)

kafka_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'dicom_producer'
}
producer = Producer(kafka_config)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to topic {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def send_to_kafka(topic, data):
    producer.produce(topic, key=str(uuid.uuid4()), value=json.dumps(data).encode('utf-8'), callback=delivery_report)

def process_dicom_file(filepath_in_container):
    """Processes a single DICOM file, extracts metadata, converts to PNG, and sends to Kafka."""
    files_processed_count = 0
    try:
        if not os.path.isfile(filepath_in_container):
            print(f"Warning: File '{filepath_in_container}' not found or not accessible. Skipping.")
            return files_processed_count

        try:
            ds = pydicom.dcmread(filepath_in_container)
        except Exception as e:
            print(f"Error reading DICOM file {filepath_in_container}: {e}. Skipping.")
            return files_processed_count

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
            print(f"No pixel data found for {filepath_in_container}. Sending metadata only.")
            doc = {
                "original_dicom_file": filepath_in_container,
                "image_path": None,
                "metadata": metadata
            }
            send_to_kafka(KAFKA_TOPIC, doc)
            files_processed_count += 1
            return files_processed_count

        array = ds.pixel_array

        if 'RescaleSlope' in ds and 'RescaleIntercept' in ds:
            array = array * ds.RescaleSlope + ds.RescaleIntercept

        # Example: Input path is /app/dicom_scans/case1/case3c_002.dcm
        path_parts = filepath_in_container.split(os.path.sep)
        case_folder = "manual_uploads" 
        if len(path_parts) >= 3 and path_parts[-3] == "dicom_scans": 
            case_folder = path_parts[-2] 
        elif len(path_parts) >= 2 and path_parts[-2] == "dicom_scans": 
            case_folder = "root_scans" 
        
        output_image_folder = os.path.join(OUTPUT_BASE_FOLDER, case_folder)
        os.makedirs(output_image_folder, exist_ok=True) 

        image_base_name = os.path.splitext(os.path.basename(filepath_in_container))[0]

        if len(array.shape) == 3: 
            for i, slice_ in enumerate(array):
                slice_norm = ((slice_ - slice_.min()) / (slice_.ptp() + 1e-6) * 255).astype(np.uint8)
                img = Image.fromarray(slice_norm)
                
                img_name = f"{image_base_name}_slice_{i}_{uuid.uuid4().hex[:6]}.png"
                img_path_in_container = os.path.join(output_image_folder, img_name)
                img.save(img_path_in_container)
                
                doc = {
                    "original_dicom_file": filepath_in_container,
                    "image_path": img_path_in_container,
                    "slice_number": i,
                    "metadata": metadata
                }
                send_to_kafka(KAFKA_TOPIC, doc)
                files_processed_count += 1
        # Single-frame DICOM:
        else:
            image_norm = ((array - array.min()) / (np.ptp(array) + 1e-6) * 255).astype(np.uint8)
            img = Image.fromarray(image_norm) 
            
            img_name = f"{image_base_name}_{uuid.uuid4().hex[:6]}.png"
            img_path_in_container = os.path.join(output_image_folder, img_name)
            img.save(img_path_in_container)
            
            doc = {
                "original_dicom_file": filepath_in_container,
                "image_path": img_path_in_container,
                "metadata": metadata
            }
            send_to_kafka(KAFKA_TOPIC, doc)
            files_processed_count += 1
    except Exception as e:
        print(f"An unexpected error occurred during processing of {filepath_in_container}: {e}")
    return files_processed_count


if __name__ == "__main__":
    target_files = []

    if len(sys.argv) > 1:
        print("Producer: Processing files from command-line arguments.")
        for arg_path in sys.argv[1:]:
            if arg_path.startswith('./dicom_scans/'):
                container_path = os.path.join('/app', arg_path.lstrip('./'))
            elif arg_path.startswith('/dicom_scans/'): 
                container_path = os.path.join('/app', arg_path.lstrip('/'))
            else:
                container_path = os.path.join(DICOM_FOLDER, os.path.basename(arg_path))
            target_files.append(container_path)

        files_processed_total = 0
        for file_to_process in target_files:
            files_processed_total += process_dicom_file(file_to_process)
        
        print(f"Producer: Processed {files_processed_total} files from command line. All messages sent (or attempted).")

    else:
        # No command-line arguments, default to scanning the DICOM_FOLDER
        print(f"Producer: Scanning DICOM folder '{DICOM_FOLDER}' for new files...")
        if not os.path.isdir(DICOM_FOLDER):
            print(f"Error: DICOM folder '{DICOM_FOLDER}' not found. Please check volume mount and path.")
            sys.exit(1) 

        files_processed_total = 0
        for root, _, filenames in os.walk(DICOM_FOLDER):
            for filename in filenames:
                if not filename.endswith(".dcm"):
                    continue
                
                filepath_in_container = os.path.join(root, filename)
                files_processed_total += process_dicom_file(filepath_in_container)

        print(f"Producer: Processed {files_processed_total} files from folder scan. All messages sent (or attempted).")

    try:
        remaining_msgs = producer.flush(timeout=10)
        if remaining_msgs > 0:
            print(f"Warning: {remaining_msgs} messages were not delivered.")
        else:
            print("All messages successfully delivered.")
    except Exception as e:
        print(f"Error flushing producer: {e}")

    print("Producer script finished.")