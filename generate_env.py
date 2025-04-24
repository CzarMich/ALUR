# generate_env.py

import os
from pathlib import Path

env_path = Path("conf/environment/.env")

DEFAULT_ENV_CONTENT = """\
# Auto-generated .env file for Alur

# Database Settings
DB_TYPE=postgres
DB_HOST=localhost
DB_PORT=5432
DB_NAME=touch
DB_USER=touch
DB_PASSWORD=your_secure_password

# EHR Server
EHR_SERVER_URL=http://localhost/ehr/rest/v1
EHR_SERVER_USER=admin
EHR_SERVER_PASSWORD=password
EHR_AUTH_METHOD=basic

# FHIR Server
FHIR_SERVER_URL=http://localhost:8080/fhir
FHIR_SERVER_USER=
FHIR_SERVER_PASSWORD=
FHIR_AUTH_METHOD=basic

# Key path (inside container)
KEY_PATH=./conf/environment/key/key.bin

# GPAS (optional)
GPAS_BASE_URL=
GPAS_ROOT_DOMAIN=
GPAS_CLIENT_CERT=
GPAS_CLIENT_KEY=
GPAS_CA_CERT=
"""

def generate_env_file():
    if not env_path.exists():
        os.makedirs(env_path.parent, exist_ok=True)
        with open(env_path, "w") as f:
            f.write(DEFAULT_ENV_CONTENT)
        print(f"✅ .env created at {env_path}")
    else:
        print("ℹ️ .env already exists. Skipping generation.")

if __name__ == "__main__":
    generate_env_file()
