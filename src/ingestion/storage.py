"""Bronze ingestion and publication of completed pipeline runs."""
import hashlib
import os
from urllib.request import urlopen

SOURCE = "https://data.nasa.gov/docs/legacy/meteorite_landings/Meteorite_Landings.csv"
SHA256 = "5b4c6a3596f177b3fe8b722e8a7e5f98f05628fe2167ec3bd007acdeb77036b5"
BYTES = 3_952_161
PREFIX = "meteorite-hunter"
RAW_KEY = f"{PREFIX}/bronze/meteorite_landings.csv"


def fingerprint(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def validate_source(path):
    if path.stat().st_size != BYTES or fingerprint(path) != SHA256:
        raise ValueError(f"Le CSV ne correspond pas à l'instantané attendu : {path}")


def fetch_source(path):
    with urlopen(SOURCE, timeout=60) as response:
        path.write_bytes(response.read(BYTES + 1))
    validate_source(path)


class ObjectStore:
    def __init__(self, bucket):
        import boto3
        from botocore.config import Config

        endpoint = os.getenv("AWS_S3_ENDPOINT") or os.getenv("AWS_ENDPOINT_URL_S3") or os.getenv("S3_ENDPOINT")
        if not endpoint:
            raise ValueError("Renseigner AWS_S3_ENDPOINT avec le point d'accès Onyxia")
        if "://" not in endpoint:
            endpoint = "https://" + endpoint
        self.client = boto3.client(
            "s3", endpoint_url=endpoint, config=Config(s3={"addressing_style": "path"})
        )
        self.bucket = bucket

    def uri(self, key):
        return f"s3://{self.bucket}/{key}"

    def stage_bronze(self, source, staged):
        self.client.upload_file(str(source), self.bucket, RAW_KEY)
        self.client.download_file(self.bucket, RAW_KEY, str(staged))
        validate_source(staged)
        print(f"BRONZE {self.uri(RAW_KEY)} | {BYTES} octets | SHA-256 confirmé", flush=True)

    def publish(self, run_dir):
        for layer in ("silver", "gold"):
            for path in sorted((run_dir / layer).iterdir()):
                key = f"{PREFIX}/{layer}/{run_dir.name}/{path.name}"
                self.client.upload_file(str(path), self.bucket, key)
                info = self.client.head_object(Bucket=self.bucket, Key=key)
                if info["ContentLength"] != path.stat().st_size:
                    raise IOError(f"Taille inattendue sur S3 : {self.uri(key)}")
                print(f"S3 {self.uri(key)} | {info['ContentLength']} octets", flush=True)

        # Only advertise a run once all its objects and verifications are present.
        manifest = f"{PREFIX}/gold/{run_dir.name}/manifest.json"
        self.client.upload_file(str(run_dir / "manifest.json"), self.bucket, manifest)
        print(f"SUCCÈS {self.uri(manifest)}", flush=True)
