provider "google" {
  project = var.project_id
  region  = var.region
}

# Service account for pipeline
resource "google_service_account" "finance_pipeline" {
  account_id   = "finance-pipeline-sa"
  display_name = "Finance Pipeline Service Account"
  description  = "Service account for personal finance data pipeline"
}

# Grant roles
resource "google_project_iam_member" "bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.finance_pipeline.email}"
}

# Create BigQuery dataset
resource "google_bigquery_dataset" "finance_dataset" {
  dataset_id                  = var.dataset_id
  friendly_name               = "Personal Finance Dataset"
  description                 = "Dataset for personal finance data"
  location                    = var.location
  default_table_expiration_ms = null

  # Set access control
  access {
    role          = "OWNER"
    user_by_email = google_service_account.finance_pipeline.email
  }

  access {
    role          = "OWNER"
    user_by_email = var.owner_email
  }
}

# Create main transactions table
resource "google_bigquery_table" "transactions_table" {
  dataset_id = google_bigquery_dataset.finance_dataset.dataset_id
  table_id   = "f_unified_transactions"
  
  deletion_protection = true

  time_partitioning {
    type                     = "DAY"
    field                    = "date"
    require_partition_filter = false
  }

  clustering = ["source", "category", "account_type"]

  description = "Unified table containing all financial transactions"
}

# Create view for recurring transactions
resource "google_bigquery_table" "recurring_transactions_view" {
  dataset_id = google_bigquery_dataset.finance_dataset.dataset_id
  table_id   = "v_recurring_transactions"
  
  view {
    query = <<-SQL
      SELECT
        *
      FROM
        `${var.project_id}.${var.dataset_id}.f_unified_transactions`
      WHERE
        is_recurring = TRUE
    SQL
    use_legacy_sql = false
  }

  description = "View of recurring transactions"
}

# Create view for monthly spending by category
resource "google_bigquery_table" "monthly_category_view" {
  dataset_id = google_bigquery_dataset.finance_dataset.dataset_id
  table_id   = "v_monthly_category_spending"
  
  view {
    query = <<-SQL
      SELECT
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(MONTH FROM date) AS month,
        category,
        subcategory,
        SUM(amount) AS total_amount,
        COUNT(*) AS transaction_count
      FROM
        `${var.project_id}.${var.dataset_id}.f_unified_transactions`
      WHERE
        amount < 0 -- Only include expenses (negative amounts)
        AND is_transfer = FALSE -- Exclude transfers
      GROUP BY
        year, month, category, subcategory
      ORDER BY
        year DESC, month DESC, total_amount ASC
    SQL
    use_legacy_sql = false
  }

  description = "Monthly spending by category and subcategory"
}
