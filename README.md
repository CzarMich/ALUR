# ALUR Project – openEHR AQL–FHIR Mapper  
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)

> 🔓 **Open Source Release**: This project is available under the MIT License. You are free to use, adapt, and contribute under the conditions defined in the `LICENSE` file.  
> 🧠 **Project Lead**: Maintained by **Michael Anywar** — Healthcare Technology Researcher at **Tallinn University of Technology**. For contributions or queries, contact: **michael.anywar@alpamax.eu**

---
## What does ALUR stand for?

**ALUR** stands for:  
> **AQL-based**  
> **Logical**  
> **Unified**  
> **Routing**

It reflects the tool’s purpose: to logically route clinical data retrieved via openEHR AQL into structured FHIR resources, supporting seamless and selective healthcare data exchange.

---

## Overview

**ALUR** is a modular, query-driven engine for transforming openEHR data into FHIR-compliant resources. It enables secure, selective, and reusable extraction of clinical data from openEHR CDRs using AQL, mapping the results to FHIR using YAML. The engine is designed for both in realtime and scheduled data processing.

Supports:
- Selective pseudonymization via GPAS or AES encryption  
- Custom mappings 
---

## Features

- **openEHR AQL-based Integration** – Pull data using parameterized AQLs  
- **FHIR Mapping Engine** – YAML + templates to output FHIR-compliant JSON  
- **Pseudonymization Support** – AES encryption or external GPAS integration  
- **Consent Provision Nesting** – Fully supports complex `provision` trees (`permit`, `deny`, etc.)  
- **Extension Mapping** – Add custom `extension` fields with full value[x] support  
- **Realtime and Scheduled processing**
---

---

## 🔧 Installation

### Prerequisites

- Docker & Docker Compose  
- Python 3.9+ for local execution
- Postgres DB for temporary data persistance

### Clone the Repository

```bash
git clone https://github.com/CzarMich/ALUR.git
cd alur
```

### Configuration
Edit the following files as needed:
- `application/conf/settings.yml` – resource configuration and fetch interval  
- `application/conf/environment/.env` – database credentials, AQL/FHIR endpoints  
- `application/resources/*.yml` – AQL and FHIR mapping templates

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

## ▶️ Usage

### Docker Execution

```bash
docker-compose build
docker-compose up
```

The service will:
- Run AQL queries on schedule  
- Apply optional pseudonymization  
- Map results to FHIR JSON  
- Push to FHIR server

### Local Execution

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
export $(cat application/conf/environment/.env | xargs)

python application/main.py
```

To run tests or override the entry point:

```bash
python application/test.py
```

---

## ✨ Consent-Specific Logic

Consent resource processing includes:
- Grouping by `composition_id` (or custom `group_by`)
- Searching existing FHIR Consent by `identifier`
- Conditional POST or PUT based on presence
- Full support for `provision` nesting

---

## 🧪 Performance Testing

```bash
grep "✅ Final FHIR Consent" logs/alur.log
```

---

## 📋 Logging

- Logs stored in `/logs/` folder  
- Rotated daily, retained for 30 days (configurable)  
- Verbosity controlled via `settings.yml`

---

## 🔐 Security

- **GPAS integration** with certificate-based SOAP for pseudonymization  
- **AES encryption** for ID masking (auto-generated key stored under `conf/environment/key/key.bin`)  
- **No PHI stored locally**

---

## 🔄 Extension Mapping Examples

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

- 🔌 **Connection errors**: Check `.env` values  
- 🔐 **Key errors**: Ensure container has access to `conf/environment/key/`  
- 📤 **FHIR errors**: Validate YAML structure and required fields  
- 🧩 **Consent issues**: Confirm `provision: "{{ provision }}"` is used properly  
- 📎 **Extension problems**: Check `value[x]` consistency with FHIR spec

---

## 📦 Requirements Management

To update dependencies:

```bash
pip freeze > requirements.txt
```

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
