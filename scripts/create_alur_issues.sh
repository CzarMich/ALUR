#!/bin/bash

PROJECT_ID=PVT_kwDOB-C9_c4A4l2M

echo "Creating prioritized ALUR issues and linking to GitHub Project..."

# Phase 1: Core
gh issue create --title "Implement Mapping YAML Upload Functionality" --body "Enable users to upload YAML files for FHIR→openEHR and openEHR→FHIR mappings." --label "mapping,yaml,uploader" --project "$PROJECT_ID"
gh issue create --title "Validate Uploaded Mapping Files (YAML + Jinja2)" --body "Validate uploaded YAML and Jinja2 template combinations for syntax and completeness." --label "validation,jinja2,mapping" --project "$PROJECT_ID"
gh issue create --title "Add FHIR → openEHR Mapping Support" --body "Enable ingestion of FHIR resources and convert them into openEHR compositions using .opt templates." --label "mapping,enhancement" --project "$PROJECT_ID"
gh issue create --title "Export Mapped Compositions (FHIR → openEHR)" --body "Export or POST openEHR compositions generated from mapped FHIR resources." --label "openEHR,export,feature-request" --project "$PROJECT_ID"

# Phase 2: Façade Engine
gh issue create --title "Add FHIR Façade (Search → AQL → FHIR in Real-Time)" --body "Accept FHIR search queries, translate to AQL, fetch openEHR, and return FHIR JSON on the fly." --label "facade,real-time,FHIR,AQL" --project "$PROJECT_ID"

# Phase 3: UI Layer
gh issue create --title "Add Mapping Designer GUI" --body "Web interface to visually create, validate, and preview mapping YAMLs with drag-and-drop or form input." --label "GUI,UX,mapping" --project "$PROJECT_ID"
gh issue create --title "Add Dashboard for Job Monitoring & Metrics" --body "Show job execution stats, failures, retries, and mapping success over time." --label "dashboard,monitoring,UX" --project "$PROJECT_ID"

# Phase 4: Governance
gh issue create --title "Add Consent Management & Audit Logging" --body "Implement patient-level consent rules and secure audit logs for all mapping events." --label "consent,audit,security" --project "$PROJECT_ID"
gh issue create --title "Add FHIR Resource Validation Against Profiles" --body "Validate outgoing FHIR JSONs against standard or custom StructureDefinitions." --label "validation,FHIR,quality" --project "$PROJECT_ID"

# Phase 5: Logic & AI
gh issue create --title "Integrate Clinical Quality Language (CQL) Execution" --body "Evaluate .cql logic on FHIR-mapped data to power decision support or quality reporting." --label "CQL,FHIR,quality-measure" --project "$PROJECT_ID"
gh issue create --title "Enable AI/ML Pipeline Export" --body "Export structured, pseudonymized data as JSON/CSV for machine learning and research." --label "AI,export,research" --project "$PROJECT_ID"
gh issue create --title "Integrate Terminology Service for Code Validation" --body "Use SNOMED CT, LOINC, ICD APIs to validate and auto-correct clinical codes." --label "terminology,validation" --project "$PROJECT_ID"

# Phase 6: Export Layer
gh issue create --title "Add Export of openEHR & FHIR Data to CSV / Excel" --body "Support export of raw AQL results and mapped FHIR resources into CSV and Excel formats for research, audits, and non-developer analysis.\n\n**Subtasks:**\n- [ ] Export AQL JSON results to \`aql_output.csv\`\n- [ ] Export mapped FHIR resources to \`.xlsx\` and \`.jsonl\`\n- [ ] Include timestamps and query metadata\n- [ ] Provide CLI and optional UI toggle for export" --label "export,csv,excel,research,future" --project "$PROJECT_ID"

echo "🎯 All issues created and added to project ID $PROJECT_ID."
