# -*- coding: utf-8 -*-
"""
Created on Mon Nov 28 21:23:08 2022

@author: Robert
"""
import zipfile
import h5py
import numpy as np


def zip_to_hdf(path_zip, path_hdf, head_len=24, body_len=1024,name_hdf='waves'):
    with zipfile.ZipFile(path_zip) as file:
        name_list=file.namelist()
        streams = len(name_list)
        subfile_size = file.filelist[0].file_size
        f_32=np.dtype('float32').itemsize
        starts_size = body_len * f_32
        starts = int(subfile_size / (starts_size + head_len))
        with h5py.File(path_hdf, 'w') as file_hdf:
            dataset = file_hdf.create_dataset(name_hdf, (starts, streams, body_len))
            for ind, filename in enumerate(name_list):
                with file.open(filename) as subfile:
                    for i in range(starts):
                        subfile.seek(head_len, 1)
                        entry = np.frombuffer(subfile.read(starts_size))
                        dataset[i, ind, :] = entry