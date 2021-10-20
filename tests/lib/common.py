import hashlib


def hashfile(file):
    buf_size = 65536
    sha256 = hashlib.sha256()

    with open(file, "rb") as f:
        while True:
            data = f.read(buf_size)
            if not data:
                break

            sha256.update(data)
    return sha256.hexdigest()
