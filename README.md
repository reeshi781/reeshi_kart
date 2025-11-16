## Reeshi Kart Order File Validator

### Overview
Reeshi Kart is an online retailer operating in Mumbai and Bangalore. Each day, both cities generate transaction files that must be validated and organized. This repository contains an automated pipeline to:
- Read daily incoming order files
- Validate using a defined set of business rules
- Organize outputs into success and rejected folders
- Log detailed rejection reasons per order
- Notify the business team via email with a daily summary

### Core Validation Rules
1. Product ID must exist in the product master file.
2. Total sales amount must equal price × quantity.
3. Order date cannot be in the future.
4. No field should be empty (all fields must have values).
5. City must be either 'Mumbai' or 'Bangalore'.

### Folder Structure
The pipeline organizes files by day (YYYYMMDD) inside S3 (recommended) or a similar structure if used locally.

```
ReeshiKart/
  incoming_files/
    YYYYMMDD/            <- daily order files (input)
  success_files/
    YYYYMMDD/            <- valid files (output)
  rejected_files/
    YYYYMMDD/            <- rejected files + error details (output)
```

Rejected outputs also include per-file error extracts:
- error_{original_file_name}.csv containing only invalid rows plus a column `rejection_reason` (if multiple, separated by semicolons).

### Tech Stack
- Python (pandas, boto3, loguru)
- AWS S3 (storage)
- AWS SES (email notifications)
- Config-driven via INI file

### Repository Contents
- `incoming_files.py`: Lists and reads all incoming CSVs for today’s date from S3, consolidating them into a single DataFrame.
- `send.py`: Validates, splits into clean/error datasets, uploads to S3, and sends daily email notifications.
- `product_master.csv`: Product reference (id, name, price, category).
- `config.sample.ini`: Sample configuration (never commit real secrets).
- `.gitignore`: Prevents committing sensitive files (e.g., `config.ini`).

### What You Built
- Automated daily ingestion from S3 prefix `incoming_files/YYYYMMDD/`.
- Validation engine enforcing business rules (product existence, price×quantity, date checks, non-empty fields, allowed cities).
- Data enrichment by joining with `product_master.csv` and computing `sale_actual_price`.
- Split pipeline producing clean dataset and detailed error dataset with `rejection_reason`.
- Organized outputs to date-partitioned S3 prefixes `success_files/` and `rejected_files/`.
- Email notifications via AWS SES summarizing processed, passed, and failed counts (or no-files message).
- Secure configuration practice: `config.ini` ignored; `config.sample.ini` provided for safe sharing.
- Modular Python code using `pandas`, `boto3`, and `loguru` for logging.

### Configuration
Copy the sample config and fill in your values. Do NOT commit real credentials.

```
cp config.sample.ini config.ini
```

`config.ini` (example fields):
```
[aws]
access_key = YOUR_AWS_ACCESS_KEY
secret_access_key = YOUR_AWS_SECRET_ACCESS_KEY
bucket_name = your-bucket-name
incoming_prefix = incoming_files/
success_prefix = success_files/
rejected_prefix = rejected_files/

[email]
sender = you@example.com
receiver = someone@example.com
region = ap-south-1
```

Keep `config.ini` private. Git is configured to ignore it.

### Input Data Formats
- Incoming orders CSV fields (per file):
  - order_id
  - product_id
  - order_date
  - city
  - quantity
  - sales (aka total_sales_amount)

- Product master CSV fields:
  - product_id
  - product_name
  - price
  - category

### How It Works
1. Read: `incoming_files.py` lists today’s S3 prefix `incoming_files/YYYYMMDD/` and reads all CSVs into a DataFrame.
2. Enrich: `send.py` joins orders with `product_master.csv` to get price and computes `sale_actual_price = quantity * price`.
3. Validate: Applies the rules listed above and aggregates `reason` per row.
4. Split: Produces:
   - Clean dataset (rows with no reasons)
   - Error dataset (rows with reasons; `rejection_reason` combined)
5. Organize: Uploads to S3 under:
   - `success_files/YYYYMMDD/clean_file.csv`
   - `rejected_files/YYYYMMDD/error_file.csv` (and per-file error extracts if using per-file mode)
6. Notify: Sends an SES email with:
   - Subject: `validation email YYYY-MM-DD`
   - Body: total files processed, how many passed/failed; or a no-files-found message if nothing to process

### Running the Pipeline
Prerequisites:
- Python 3.9+ recommended
- AWS credentials for S3 and SES with required permissions
- IAM policy allowing S3 read/write on the configured bucket and SES SendEmail in the configured region

Install dependencies (example):
```
pip install pandas loguru boto3
```

Run:
```
python send.py
```

This will:
- Read incoming files for today’s date
- Validate and split into clean/error outputs
- Upload results to S3
- Send the summary email

### Operational Notes
- Date handling uses today’s `YYYYMMDD` to select the S3 prefix.
- If no incoming files are found, the job sends a “No incoming files found” email.
- Push protection: Never commit secrets. Use `config.ini` locally and keep `config.sample.ini` in the repo.

### Extending/Customizing
- Add new validation rules by editing the `find_errors` logic in `send.py`.
- To support additional cities or constraints, adjust the rule set and, if needed, enrich data with more master tables.
- To process historical dates, parameterize the date used for prefixes and pass it via CLI or environment variable.

### Troubleshooting
- Push blocked due to secrets: remove credentials from commits, add to `.gitignore`, and use `config.sample.ini` for placeholders.
- SES errors: ensure your sender/receiver are verified if in SES sandbox and that your IAM user has permissions.
- S3 path issues: confirm bucket name and prefixes in `config.ini`.

### License
Proprietary — internal project for NamasteKart. Update the license as appropriate for your use case.
