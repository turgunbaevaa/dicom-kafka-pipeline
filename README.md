## This README.md provides a quick reference for command-line operations to set up, run, and manage the DICOM processing pipeline.

### 1. Clone the repository:
```
git clone [dicom-kafka-pipeline link]
cd [your-project-folder-name]
```

### 2. Create input/output directories:
```
mkdir dicom_scans
mkdir images
```

### 3. I already uploaded .dcm files, so, you do not need. Or, if you want, you can do that.
Put your .dcm files into the dicom_scans directory (e.g., dicom_scans/case1/mri_scan.dcm). If you already have your DICOM files in this folder, you can skip this step.

### 4. Build and run all services in detached mode:
```
docker compose up --build -d
```

### 5. Verify services are running:
```
docker compose ps
```

### 6. Manual File Upload (Selective Processing)
To process specific DICOM files, run the producer as a one-off command.
Ensure core services (Kafka, Mongo, Consumer) are already running via docker compose up -d zookeeper kafka mongo mongo-express consumer.
```
docker compose run producer python producer.py ./dicom_scans/case1/your_file.dcm
```

### 7. View real-time processing logs from the consumer:
```
docker logs -f mri_data-consumer-1
```

### 8. Viewing Generated Images
Check your local images folder (at the project root) for generated PNGs, organized by case folder.

Access the Mongo Express web UI in your browser:
```
http://localhost:8081 
admin for both user and password
```


## Troubleshooting
Stop and remove all services, images, and volumes (clean slate):
```
docker compose down --volumes
docker system prune -a --volumes
```

(Caution: This will delete all Docker-related data, including MongoDB data and named volumes.)

Then, retry running the pipeline from scratch:

docker compose up --build -d
