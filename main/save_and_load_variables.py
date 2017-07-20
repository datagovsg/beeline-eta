from file_system import download_file, upload_file
import pickle

def write_to_pickle(filename, variable, to_bucketeer=False):
    with open(filename, 'wb') as f:
        pickle.dump(variable, f)

    if to_bucketeer:
        upload_file(filename)

def read_from_pickle(filename, from_bucketeer=False):
    if from_bucketeer:
        download_file(filename)

    with open(filename, 'rb') as f:
        return pickle.load(f)
