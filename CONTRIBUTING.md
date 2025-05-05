# Contributing to Alur – openEHR–FHIR Mapper

Thank you for your interest in contributing to Alur!  
Alpamax welcome bug reports, feature requests, and pull requests to improve the tool.

---

## 💡 Before You Start

- Please read the [README](./README.md)
- Check open issues: https://github.com/alpamax/openehr-aql-fhir-mapper/issues
- For major changes, start with a GitHub issue or discussion

---

## 🛠️ Development Setup

1. Fork this repository
2. Clone your fork locally:

```bash
git clone https://github.com/alpamax/ALUR.git
cd openehr-aql-fhir-mapper
```

3. Create and activate a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

4. (Optional) Use the Docker setup for realistic testing:

```bash
make run
```

---

## 🧪 Running Tests

To run the full processing pipeline:

```bash
python application/main.py
```

To test core mapping logic:

```bash
python application/test.py
```

To inspect logs:

```bash
tail -f /opt/alur/logs/alur.log
```

---

## ✅ Submitting a Pull Request

1. Create a new branch:

```bash
git checkout -b feature/your-feature-name
```

2. Commit your changes and push:

```bash
git commit -m "Add feature X"
git push origin feature/your-feature-name
```

3. Open a pull request on GitHub and describe your changes clearly.

---

## 🔒 Licensing

By submitting a pull request, you agree that your contribution will be licensed under the MIT License, the same as this repository.

---

## 🙌 Thank You!

Your help makes this project better for the entire digital health community.
