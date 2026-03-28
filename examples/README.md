# Canopy Examples

## employees_to_postgres

Normalizes a messy CSV of employee data into a clean PostgreSQL table.

### Source Data (`sample_employees.csv`)

The CSV contains intentionally messy data:
- Mixed date formats: `01/15/1990`, `1985-03-22`, `March 1 2021`, `Sept 15 1987`
- Mixed currency formats: `$75,000`, `62000`, `$88,500.00`
- Mixed boolean representations: `Yes`, `true`, `1`, `YES`, `True`, `0`
- Mixed phone formats: `(555) 123-4567`, `555.987.6543`, `5551234567`
- Empty/null values scattered throughout

### Running

```bash
# 1. Start Ollama with a model
ollama pull llama3

# 2. Set up PostgreSQL (or use Docker)
export DB_USER=canopy
export DB_PASSWORD=canopy_dev_pass

# 3. Run the pipeline
canopy run examples/employees_to_postgres.yaml

# 4. Inspect the generated script
cat scripts/*_convert.py

# 5. Re-run without LLM (if needed)
canopy rerun scripts/*_convert.py examples/employees_to_postgres.yaml
```
