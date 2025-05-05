# ALUR Project – openEHR AQL–FHIR Mapper  
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)

> 🔓 **Open Source Release**: This project is available under the MIT License.  
> 🧠 **Project Lead**: Maintained by **Michael Anywar** — Healthcare Technology Researcher at **Tallinn University of Technology**  
> 📫 Contact: **michael.anywar@alpamax.eu**

---

## What does ALUR stand for?

**ALUR** stands for:  
> **AQL-based**  
> **Logical**  
> **Unified**  
> **Routing**

---

## Overview

**ALUR** is a modular, query-driven engine for transforming openEHR data into FHIR-compliant resources. It enables secure, selective, and reusable extraction of clinical data from openEHR CDRs using AQL, mapping the results to FHIR using YAML. Supports both real-time and scheduled processing.

Highlights:
- Selective pseudonymization (GPAS or AES)
- YAML-based custom FHIR mappings
- Consent provision nesting support
- Secure, reproducible, and scalable

---

## Features

- **AQL-based integration** with openEHR CDRs  
- **Jinja2 + YAML** driven FHIR resource mapping  
- **GPAS / AES support** for pseudonymization  
- **Provision-aware Consent transformation**  
- **Rotating logs and interval-based scheduling**  
- **Compatible with both Docker and Python dev environments**

---

## 🚀 Deployment Options

### Option 1: Run prebuilt image from Docker Hub

```bash
docker pull alpamaxeu/alur:latest
```

Create required folders on your host:

```bash
sudo mkdir -p /opt/alur/{data,logs,conf/environment/cert,conf/environment/key,resources}
```

Then launch using Docker Compose (see `docker-compose.yml`):

```bash
docker-compose up -d
```

### Option 2: Build from source

```bash
git clone https://github.com/alpamax/ALUR.git
cd ALUR
./run.sh
```

---

## 🧑‍💻 Development (optional for contributors)

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
export $(cat application/conf/environment/.env | xargs)

python application/main.py
```

---

## 🔧 Configuration

Edit the following files:

- `application/conf/settings.yml` — fetch intervals, logic  
- `application/conf/environment/.env` — DB + endpoint config  
- `application/resources/*.yml` — FHIR mapping templates  

Example `.env`:

```ini
DB_HOST=db
DB_PORT=5432
DB_NAME=alur
DB_USER=aluruser
DB_PASSWORD=alurpassword
FHIR_BASE_URL=http://your.fhir.server/fhir
```

---

## 🧪 Consent-Specific Logic

- Groups records by `composition_id` or configurable key  
- Uses `identifier` to detect existing Consent  
- Generates full `provision` nesting  
- Uses `committed_dateTime` for `Consent.dateTime`

---

## 📋 Logging

- Logs stored in `/opt/alur/logs/`  
- Rotated daily, kept for 30 days  
- Controlled via `settings.yml`

---

## 🔐 Security

- **GPAS integration** via certificate-based SOAP  
- **AES encryption** with auto key at `conf/environment/key/key.bin`  
- **No PHI stored locally**

---

## 🧩 Extension Mapping Examples

### 1. Under Root Resource

```yaml
extension:
  - url: "http://example.org/fhir/StructureDefinition/custom-flag"
    valueBoolean: "{{ has_custom_flag }}"
```

### 2. Under Coding

```yaml
coding:
  - system: "{{ system }}"
    code: "{{ code }}"
    display: "{{ display }}"
    extensions:
      - url: "http://fhir.com/StructureDefinition/icd-10-gm-manifestations"
        valueCoding:
          system: "http://fhir.com/codeSystem/icd-10-gm-manifestationscoding"
          code: "{{ display }}"
          display: "{{ code }}"
```

---

## ⚠️ Troubleshooting

- 🔌 Check `.env` if DB or endpoint fails  
- 🔐 Ensure `key.bin` exists for AES  
- 📤 Validate YAML mapping structure  
- 🧩 Use correct `value[x]` in extensions  
- 🧪 Confirm `provision: "{{ provision }}"` is in mappings

---

## 📦 Dependency Management

Update dependencies with:

```bash
pip freeze > requirements.txt
```

---

## 🛠 Project Scripts

- `run.sh` – prepare folders, run, wait for DB, tail logs  
- `stop.sh` – gracefully shut down  
- `Makefile` – shortcut: `make run`, `make stop`, `make logs`, etc.  

---

## 🐙 CI/CD

- CI builds and pushes hardened image (stripped `.py`, compiled `.pyc`)  
- See `.github/workflows/alur.yml` for details

---

## 🤝 Contributing

We welcome contributions!  
Open issues, suggest improvements, or submit a pull request.

---

## 🧾 License

**MIT License** — See `LICENSE` file.

---

## 📬 Contact

**Michael Anywar**  
michael.anywar@alpamax.eu  
https://www.alpamax.eu
