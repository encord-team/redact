import os
from pathlib import Path
import requests 
import pydicom
from encord import EncordUserClient, Project
import boto3
from tqdm import tqdm
import numpy as np




keyfile = Path.home() / ".ssh" / "id_ed25519"
with Path(keyfile).open() as f:
    private_key = f.read()
user_client = EncordUserClient.create_with_ssh_private_key(private_key)
client = boto3.client("s3")

project_hashes = [INSERT_YOUR_PROJECT_HASH]
bucket_name = 'BUCKET_NAME'
bucket_folder = 'Pixel-redaction-complete'

output_path = './dicom/'

isExist = os.path.exists(output_path)
if not isExist:
   os.makedirs(output_path)


annotations_storage = {}
# Store annotation in dict keyed by the data_hash


"""
    First, cache all the annotations in a project
"""
for p_hash in project_hashes:
    project = user_client.get_project(p_hash)    
    for label_row in project.list_label_rows():
            lr = project.get_label_row(label_row.label_hash, get_signed_url=True)
            with tqdm(total=len(list(lr['data_units'].values())[0]['labels'].values())) as pbar:
                for dicom_slice in list(lr['data_units'].values())[0]['labels'].values():
                    # Check if annotations exist
                    if len(dicom_slice['objects'])>0:
                        if lr.data_hash in annotations_storage.keys():     
                            annos = annotations_storage[lr.data_hash]
                            annos.append(dicom_slice)
                            annotations_storage[lr.data_hash]=annos
                        else:
                            annotations_storage.update({lr.data_hash:[dicom_slice]})                      
                    pbar.update(1)

"""
    Now in the second pass we propagate all found bounding boxes to all slices 
"""

for p_hash in project_hashes:
    project = user_client.get_project(p_hash)
    for label_row in project.list_label_rows():
            lr = project.get_label_row(label_row.label_hash, get_signed_url=True)
            with tqdm(total=len(list(lr['data_units'].values())[0]['labels'].values())) as pbar:
                for dicom_slice in list(lr['data_units'].values())[0]['labels'].values():
                    if len(dicom_slice['objects'])>0:
                        w = dicom_slice['metadata']['width']
                        h = dicom_slice['metadata']['height']
                         # First save slice
                        r = requests.get(dicom_slice['metadata']['file_uri'])
                        output_dirname = os.path.join(os.path.join(output_path,lr.data_hash))
                        output_filename = dicom_slice['metadata']['dicom_instance_uid'] + '.dcm'
                        if not os.path.exists(output_dirname):
                            os.makedirs(output_dirname)
                        with open(os.path.join(output_dirname,output_filename), 'wb') as f:
                            _ = f.write(r.content)

                        # Check if annotations exist
                        if lr.data_hash in annotations_storage.keys():
                            # We have annotations for the slice ...
                            dcm = pydicom.read_file(os.path.join(output_dirname,output_filename))
                            for dicom_slice in annotations_storage[lr.data_hash]:
                                # Annotations found on the slice
                                for bb in dicom_slice['objects']:
                                    if 'boundingBox' in bb.keys():
                                        x1 = round(bb['boundingBox']['x'] * w)
                                        y1 = round(bb['boundingBox']['y'] * h)
                                        x2 = x1 + round(bb['boundingBox']['w'] * w)
                                        y2 = y1 + round(bb['boundingBox']['h'] * h)
                                    # Zero out pixels inside bounding box
                                    dcm.pixel_array[y1:y2,x1:x2] = 0
                                    redacted_pixeldata = dcm.pixel_array.tobytes()
                                    dcm.PixelData = redacted_pixeldata
                                    # Store redacted file
                            f = os.path.join(output_dirname,output_filename)
                            # Store locally 
                            # dcm.save_as(f) 
                            # Store in bucket
                            client.upload_file(f, bucket_name, os.path.join(bucket_folder,os.path.basename(f)))
                    pbar.update(1)






