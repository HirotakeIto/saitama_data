import unicodedata
import os

def safe_path(path):
    for type_normalized in ['NFC', 'NFKC', 'NFD', 'NFKD']:
        normalized_path = unicodedata.normalize(type_normalized, path)
        if os.path.exists(normalized_path):
            return normalized_path
    else:
        raise FileExistsError
