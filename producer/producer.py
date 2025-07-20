import os
import pydicom
from PIL import Image
import numpy as np
# For confluent-kafka-python (based on your logs 'rdkafka')
from confluent_kafka import Producer
import json
import uuid
import datetime

# --- Configuration using environment variables ---
# Retrieve Kafka broker from environment variable, default to 'kafka:9092' for safety
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'file-data') # Or 'dicom_topic' if that's what you use in consumer

# Retrieve DICOM folder path from environment variable
DICOM_FOLDER = os.getenv('DICOM_FOLDER', './dicom_scans') # This is your root folder, which now contains subfolders

print(f"Connecting to Kafka at: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Processing DICOM files from: {DICOM_FOLDER}")
print(f"Sending messages to Kafka topic: {KAFKA_TOPIC}")

# Kafka Producer setup (using confluent_kafka.Producer)
kafka_config = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
producer = Producer(kafka_config)

# Optional: Delivery report callback for debugging
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to topic '{msg.topic()}' [{msg.partition()}] at offset {msg.offset()}")

def send_to_kafka(topic, data):
    try:
        producer.produce(topic, json.dumps(data).encode('utf-8'), callback=delivery_report)
        # It's good to poll occasionally to trigger callbacks and clear the internal queue
        producer.poll(0)
    except Exception as e:
        print(f"Failed to produce message: {e}")

# Create output folder for images (relative to where the script is run *inside* the container)
output_base_folder = "images/" # This will be created inside /app/images/ by default
os.makedirs(output_base_folder, exist_ok=True)


# Main processing loop
try:
    if not os.path.isdir(DICOM_FOLDER):
        print(f"Error: DICOM folder '{DICOM_FOLDER}' not found. Please check volume mount and path.")
        exit(1) # Exit if the source folder is not found

    files_processed = 0
    
    # --- MODIFICATION STARTS HERE: Use os.walk to traverse subdirectories ---
    for root, _, filenames in os.walk(DICOM_FOLDER):
        for filename in filenames:
            if not filename.endswith(".dcm"):
                continue
            
            # Construct the full path to the DICOM file
            filepath = os.path.join(root, filename)
            
            # Add a check for file existence and readability
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
                # Use SOPInstanceUID from DICOM if available, otherwise generate a UUID
                # Note: For multi-frame DICOMs, each frame typically has its own SOPInstanceUID.
                # If your original DICOMs have a single SOPInstanceUID for the entire file,
                # you might want to consider how you ensure uniqueness for each slice in the database
                # (e.g., combining with slice_number, or making SOPInstanceUID optional/generated for slices).
                "SOPInstanceUID": ds.get("SOPInstanceUID", str(uuid.uuid4())), 
                "timestamp": datetime.datetime.utcnow().isoformat()
            }

            # Handle DICOMs without pixel data
            if not hasattr(ds, 'pixel_array') or ds.pixel_array is None:
                print(f"No pixel data found for {filepath}. Sending metadata only.")
                doc = {
                    "original_dicom_file": filepath, # Store the full path in the database
                    "image_path": None, # Indicate no image was generated
                    "metadata": metadata
                }
                send_to_kafka(KAFKA_TOPIC, doc)
                files_processed += 1
                continue

            array = ds.pixel_array
            
            # Apply Rescale Slope and Intercept if present
            if 'RescaleSlope' in ds and 'RescaleIntercept' in ds:
                array = array * ds.RescaleSlope + ds.RescaleIntercept

            # Determine the relative path to create mirrored subdirectories in output_base_folder
            relative_path_to_dicom_root = os.path.relpath(root, DICOM_FOLDER)
            output_image_folder = os.path.join(output_base_folder, relative_path_to_dicom_root)
            os.makedirs(output_image_folder, exist_ok=True) # Ensure the subdirectory exists

            # Get filename without extension for cleaner image names
            image_base_name = os.path.splitext(filename)[0]

            if len(array.shape) == 3: # Multi-frame DICOM (e.g., 3D or sequences)
                for i, slice_ in enumerate(array):
                    # Normalize and convert to 8-bit for image saving
                    # Adding a small epsilon to ptp() to prevent division by zero for flat images
                    slice_norm = ((slice_ - slice_.min()) / (slice_.ptp() + 1e-6) * 255).astype(np.uint8)
                    img = Image.fromarray(slice_norm)
                    
                    # Image path now includes the subdirectory based on original DICOM's location
                    img_name = f"{image_base_name}_slice_{i}_{uuid.uuid4().hex[:6]}.png"
                    img_path_in_container = os.path.join(output_image_folder, img_name)
                    img.save(img_path_in_container)
                    
                    doc = {
                        "original_dicom_file": filepath, # Store full path to original DICOM
                        "image_path": img_path_in_container, # Store path to generated PNG
                        "slice_number": i,
                        "metadata": metadata
                    }
                    send_to_kafka(KAFKA_TOPIC, doc)
                    files_processed += 1
            else: # Single-frame DICOM
                image_norm = ((array - array.min()) / (np.ptp(array) + 1e-6) * 255).astype(np.uint8)
                img = Image.fromarray(image_norm)
                
                # Image path now includes the subdirectory based on original DICOM's location
                img_name = f"{image_base_name}_{uuid.uuid4().hex[:6]}.png"
                img_path_in_container = os.path.join(output_image_folder, img_name)
                img.save(img_path_in_container)
                
                doc = {
                    "original_dicom_file": filepath, # Store full path to original DICOM
                    "image_path": img_path_in_container, # Store path to generated PNG
                    "metadata": metadata
                }
                send_to_kafka(KAFKA_TOPIC, doc)
                files_processed += 1
    # --- MODIFICATION ENDS HERE ---

    print(f"✅ Producer: Processed {files_processed} files. All messages sent (or attempted).")

except Exception as e:
    print(f"An unexpected error occurred during processing: {e}")
finally:
    # Ensure all messages are delivered before exiting
    remaining_msgs = producer.flush(timeout=10) # 10 seconds timeout
    if remaining_msgs > 0:
        print(f"⚠️ Warning: {remaining_msgs} messages were not delivered.")
    else:
        print("All messages successfully delivered.")

print("Producer script finished.")