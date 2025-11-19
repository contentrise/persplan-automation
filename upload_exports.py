from pathlib import Path

import boto3

BUCKET_NAME = "YOUR_BUCKET_HERE"
PREFIX = "exports/"


def main():
    s3 = boto3.client("s3")
    exports = Path("exports")
    exports.mkdir(exist_ok=True)

    for csv_file in exports.glob("*.csv"):
        key = f"{PREFIX}{csv_file.name}"
        s3.upload_file(str(csv_file), BUCKET_NAME, key)
        print(f"[OK] {csv_file} → s3://{BUCKET_NAME}/{key}")


if __name__ == "__main__":
    main()
