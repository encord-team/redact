import os
from pathlib import Path
import requests 
import pydicom
from encord import EncordUserClient, Project
from tqdm import tqdm
import numpy as np

keyfile = Path.home() / ".ssh" / "id_ed25519"
with Path(keyfile).open() as f:
    private_key = f.read()
user_client = EncordUserClient.create_with_ssh_private_key(private_key)

project_hashes = [ADD_YOUR_PROJECT_HASH_HERE]

output_path = './dicom/'

isExist = os.path.exists(output_path)
if not isExist:
   os.makedirs(output_path)

res = []
for p_hash in project_hashes:
    project = user_client.get_project(p_hash)
    
    for label_row in project.list_label_rows():
            lr = project.get_label_row(label_row.label_hash, get_signed_url=True)
            with tqdm(total=len(list(lr['data_units'].values())[0]['labels'].values())) as pbar:
                for dicom_slice in list(lr['data_units'].values())[0]['labels'].values():
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
                    if len(dicom_slice['objects'])>0:
                        # Annotations found on the slice
                        for bb in dicom_slice['objects']:
                            if 'boundingBox' in bb.keys():
                                x1 = round(bb['boundingBox']['x'] * w)
                                y1 = round(bb['boundingBox']['y'] * h)
                                x2 = x1 + round(bb['boundingBox']['w'] * w)
                                y2 = y1 + round(bb['boundingBox']['h'] * h)
                            dcm = pydicom.read_file(os.path.join(output_dirname,output_filename))
                            # Zero out pixels inside bounding box
                            dcm.pixel_array[y1:y2,x1:x2] = 0
                            redacted_pixeldata = dcm.pixel_array.tobytes()
                            dcm.PixelData = redacted_pixeldata
                            # Store redacted file
                            dcm.save_as(os.path.join(output_dirname,output_filename))
                    pbar.update(1)
