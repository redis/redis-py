import os


def get_ssl_filename(name):
    root = os.path.join(os.path.dirname(__file__), "..")
    cert_dir = os.path.abspath(os.path.join(root, "dockers", "stunnel", "keys"))
    if not os.path.isdir(cert_dir):  # github actions package validation case
        cert_dir = os.path.abspath(
            os.path.join(root, "..", "dockers", "stunnel", "keys")
        )
        if not os.path.isdir(cert_dir):
            raise OSError(f"No SSL certificates found. They should be in {cert_dir}")

    return os.path.join(cert_dir, name)
