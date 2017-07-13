import pickle

def write_to_pickle(filename, variable):
    with open(filename, 'wb') as f:
        pickle.dump(variable, f)
        # print("Saved in {}".format(filename))

def read_from_pickle(filename):
    with open(filename, 'rb') as f:
        # print("Load {}".format(filename))
        return pickle.load(f)