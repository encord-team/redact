import os
import shutil
from typing import List, Dict, Tuple

import boto3
import requests
from encord import EncordUserClient
from encord.orm.label_row import AnnotationTaskStatus
from imagecodecs import jpeg2k_encode
from pydicom import read_file, FileDataset
from pydicom.encaps import encapsulate
from pydicom.uid import JPEG2000Lossless
from tqdm import tqdm


def get_redaction_bboxes_and_metadata(
    labels: List[Dict],
) -> Tuple[List[Dict], List[Dict]]:
    # Extract all annotations from the series
    redaction_bboxes = []
    metadata = []

    for dicom_slice in labels:
        metadata.append(
            {
                'signed_url': dicom_slice['metadata']['file_uri'],
                'filename': dicom_slice['metadata']['dicom_instance_uid'] + '.dcm',
            }
        )
        # Check if annotations exist
        if len(dicom_slice['objects']) > 0:
            for bb in dicom_slice['objects']:
                if 'boundingBox' in bb.keys():
                    w = dicom_slice['metadata']['width']
                    h = dicom_slice['metadata']['height']
                    x1 = round(bb['boundingBox']['x'] * w)
                    y1 = round(bb['boundingBox']['y'] * h)
                    redaction_bboxes.append(
                        {
                            'x1': x1,
                            'y1': y1,
                            'x2': x1 + round(bb['boundingBox']['w'] * w),
                            'y2': y1 + round(bb['boundingBox']['h'] * h),
                        }
                    )
    return redaction_bboxes, metadata


def redact_slice(
    redaction_bboxes: List[Dict], meta: Dict, output_dirname: str, output_filename: str
) -> FileDataset:
    r = requests.get(meta['signed_url'])
    with open(os.path.join(output_dirname, output_filename), 'wb') as f:
        f.write(r.content)
    dcm = read_file(os.path.join(output_dirname, output_filename))
    if dcm.file_meta.TransferSyntaxUID == JPEG2000Lossless and dcm.BitsStored == 12:
        dcm.BitsStored = 16

    for bbox in redaction_bboxes:
        # Zero out pixels inside bounding box
        dcm.pixel_array[bbox['y1']:bbox['y2'], bbox['x1']:bbox['x2']] = 0
    if dcm.file_meta.TransferSyntaxUID == JPEG2000Lossless:
        encoded = jpeg2k_encode(dcm.pixel_array, level=0)
        dcm.PixelData = encapsulate([encoded])
    else:
        redacted_pixeldata = dcm.pixel_array.tobytes()
        dcm.PixelData = redacted_pixeldata
    return dcm


def main(
    keyfile: str, project_hashes: List[str], bucket_name: str, bucket_folder: str
) -> None:
    user_client = EncordUserClient.create_with_ssh_private_key(
        ssh_private_key_path=keyfile
    )
    s3_client = boto3.client("s3")
    output_path = './dicom/'
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    for p_hash in project_hashes:
        project = user_client.get_project(p_hash)
        completed_lrms = project.list_label_rows(label_statuses=[AnnotationTaskStatus.COMPLETED])
        print(f'Project {project.title} has {len(completed_lrms)} completed label rows.')
        for lrm in (lr_pbar := tqdm(completed_lrms)):
            lr_pbar.set_description(f'Redacting {lrm.data_title}')
            lr = project.get_label_row(lrm.label_hash, get_signed_url=True)
            labels = list(lr['data_units'].values())[0]['labels'].values()
            redaction_bboxes, metadata = get_redaction_bboxes_and_metadata(labels)

            output_dirname = os.path.join(os.path.join(output_path, lr.data_hash))
            if not os.path.exists(output_dirname):
                os.makedirs(output_dirname)

            for meta in metadata:
                output_filename = meta['filename']
                dcm = redact_slice(redaction_bboxes, meta, output_dirname, output_filename)
                f = os.path.join(output_dirname, output_filename)
                dcm.save_as(f)
                s3_client.upload_file(
                    f,
                    bucket_name,
                    os.path.join(bucket_folder, lr.data_title, output_filename),
                )
    shutil.rmtree(output_path)


if __name__ == '__main__':
    main(
        keyfile='PATH_TO_KEYFILE',
        project_hashes=['PROJECT_HASH'],
        bucket_name='BUCKET_NAME',
        bucket_folder='BUCKET_FOLDER'
    )
