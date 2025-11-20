# OpenPurview Development Roadmap

## Status Atual: 65% Completo

### ✅ Fase 1: Infraestrutura Core (100%)
- [x] Estrutura do projeto
- [x] KùzuDB Graph Database
- [x] Azure AD Authentication
- [x] Streamlit Frontend Base
- [x] Git Repository Setup

### ✅ Fase 2: Azure & M365 Collection (100%)
- [x] Azure Resource Graph API (5000+ recursos)
- [x] Microsoft 365 Users & Groups
- [x] Azure Subscriptions tracking
- [x] Resource relationships (Sub → Resource)

### ✅ Fase 3: Analytics Platform Integration (100%)
- [x] Power BI Scanner API (workspaces, reports, datasets)
- [x] Power BI lineage (Dataset → Report)
- [x] Azure Synapse Analytics (pipelines, notebooks, datasets, linked services)
- [x] Azure Data Factory (pipelines, datasets, linked services)

### ✅ Fase 4: Database Schema Scanning (100%)
- [x] SQL Server / Azure SQL schema extraction
- [x] Table and column metadata
- [x] Foreign key relationships
- [x] Azure AD token authentication

### 🔄 Fase 5: Data Lake & Storage (80% - EM DESENVOLVIMENTO)
- [x] Azure Blob Storage collector structure
- [x] Container and blob metadata extraction
- [x] File type detection (CSV, Parquet, JSON, etc.)
- [x] Database schema with Storage nodes/edges
- [ ] UI integration for storage scanning
- [ ] Metadata caching for large storage accounts

### 🔄 Fase 6: Data Classification & Sensitivity (70% - EM DESENVOLVIMENTO)
- [x] PII detection engine (CPF, CNPJ, Email, Phone, Credit Card, SSN)
- [x] Pattern matching with regex
- [x] Column name-based classification
- [x] Sample value analysis
- [x] Confidence scoring
- [x] Database schema with Classification nodes/edges
- [ ] UI integration for classification reports
- [ ] Automated classification on scan
- [ ] Custom classification rules

### ⏳ Fase 7: Advanced UI & Dashboard (50%)
- [x] 5 tabs (Overview, Asset Catalog, Data Lineage, Compliance, Graph Explorer)
- [x] Advanced search with filters
- [x] Resource distribution charts
- [x] Interactive PyVis graph visualization
- [ ] Real-time scanning progress
- [ ] Classification heatmaps
- [ ] Sensitivity dashboards
- [ ] Export reports (PDF/Excel)

### ⏳ Fase 8: Data Lineage Mapping (40%)
- [x] Power BI Dataset → Report
- [x] ADF Pipeline tracking
- [x] SQL Foreign Keys
- [ ] End-to-end lineage (SQL → ADF → Blob → Power BI)
- [ ] Impact analysis
- [ ] Column-level lineage

### ⏳ Fase 9: Additional Data Sources (20%)
- [ ] Azure Data Lake Gen2
- [ ] PostgreSQL
- [ ] MySQL
- [ ] Databricks
- [ ] Snowflake
- [ ] Oracle
- [ ] Teradata
- [ ] AWS S3 (multi-cloud)
- [ ] Google BigQuery (multi-cloud)

### ⏳ Fase 10: Compliance & Governance (30%)
- [x] Sensitivity levels (Critical, High, Medium, Low)
- [x] PII categories (National ID, Financial, Contact Info)
- [ ] LGPD compliance reporting
- [ ] GDPR compliance reporting
- [ ] Data retention policies
- [ ] Access audit logs
- [ ] Policy enforcement

### ⏳ Fase 11: Advanced Features (10%)
- [ ] Scheduled scans (cron/background)
- [ ] Change detection
- [ ] Anomaly detection
- [ ] Data quality metrics
- [ ] Schema drift detection
- [ ] API for external integration
- [ ] Multi-tenant support

### ⏳ Fase 12: Performance & Scale (30%)
- [x] Database lock handling
- [x] Batch processing (100 workspaces chunks)
- [ ] Incremental scanning
- [ ] Parallel collection
- [ ] Redis caching layer
- [ ] Performance benchmarks

---

## Próximos Passos Imediatos

### Sprint Atual (Novembro 2025)
1. **Integrar Storage Collector na UI** (2h)
   - Adicionar botão "Scan Blob Storage"
   - Query storage accounts from Resource table
   - Display container and blob counts

2. **Integrar Classification Engine** (3h)
   - Add "Run Classification" button
   - Scan SQL columns and blob samples
   - Display sensitivity report in Compliance tab

3. **Enhanced Lineage Visualization** (2h)
   - Add end-to-end lineage view
   - Connect SQL → ADF → Blob → Power BI
   - Impact analysis for table changes

4. **Classification Reports** (1h)
   - Sensitivity distribution charts
   - Top sensitive tables/files
   - LGPD/GDPR compliance status

### Sprint Seguinte (Dezembro 2025)
1. Data Lake Gen2 support
2. PostgreSQL collector
3. Scheduled background scans
4. Export functionality (PDF reports)

---

## Paridade com Microsoft Purview

| Feature | Microsoft Purview | OpenPurview | Status |
|---------|------------------|-------------|---------|
| Azure Resources | ✅ | ✅ | 100% |
| M365 Integration | ✅ | ✅ | 100% |
| Power BI Scanning | ✅ | ✅ | 100% |
| SQL Schema | ✅ | ✅ | 100% |
| Synapse Analytics | ✅ | ✅ | 100% |
| Data Factory | ✅ | ✅ | 100% |
| Blob Storage | ✅ | 🔄 | 80% |
| PII Classification | ✅ | 🔄 | 70% |
| Data Lineage | ✅ | 🔄 | 40% |
| Data Lake Gen2 | ✅ | ⏳ | 0% |
| Multi-DB Support | ✅ | ⏳ | 20% |
| Compliance Reports | ✅ | ⏳ | 30% |
| Multi-Cloud | ✅ | ⏳ | 0% |
| API Access | ✅ | ⏳ | 0% |

**Overall Parity: 65%**

---

## Tecnologias Utilizadas

### Backend
- **Python 3.11+**
- **KùzuDB 0.11.3** - Graph database embedded
- **Azure SDK** - azure-identity, azure-mgmt-*, azure-synapse-*, azure-storage-*
- **Microsoft Graph SDK** - msgraph-sdk
- **pyodbc** - SQL Server connectivity

### Frontend
- **Streamlit 1.28+** - Web interface
- **PyVis 0.3.2** - Network visualization
- **Pandas** - Data manipulation

### Authentication
- **DefaultAzureCredential** - Azure AD (az login, Service Principal, Managed Identity)

### Infrastructure
- **Git/GitHub** - Version control
- **Local execution** - No cloud dependencies
- **Zero licensing costs** - 100% open source

---

## Contribuindo

### Arquitetura
```
src/
├── collectors/          # Data source collectors
│   ├── azure_collector.py
│   ├── m365_collector.py
│   ├── powerbi_collector.py
│   ├── synapse_collector.py
│   ├── adf_collector.py
│   ├── sql_collector.py
│   ├── storage_collector.py        # NEW
│   └── classification_engine.py    # NEW
├── database/
│   └── db_manager.py   # Graph database operations
└── ui/
    └── app.py          # Streamlit interface
```

### Como Adicionar Novo Collector
1. Criar arquivo em `src/collectors/`
2. Implementar método `scan_*()` que retorna dict
3. Adicionar nodes/edges no `db_manager.py` → `_init_schema()`
4. Criar método `upsert_*()` no `db_manager.py`
5. Adicionar botão e lógica na UI (`app.py`)
6. Atualizar `requirements.txt` se necessário

---

## Métricas de Sucesso

- ✅ Escaneamento de 5000+ recursos Azure
- ✅ Coleta de 100+ workspaces Power BI
- ✅ Schema de 50+ tabelas SQL
- 🔄 1000+ blobs com classificação
- ⏳ Lineage end-to-end completo
- ⏳ Performance < 5min para scan completo

**Meta Final: 95% de paridade com Microsoft Purview até Q1 2026**
