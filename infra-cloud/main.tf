terraform {
  backend "gcs" {
    bucket = "apex-terraformbackend"
    prefix = "terraform/state"
  }
}

# Create bucket to store our data 

resource "google_storage_bucket" "apexdatabucket" {
  name                        = "apexamdaribucket"
  location                    = "US"
  uniform_bucket_level_access = true
}


resource "google_bigquery_dataset" "apexdataset" {
  dataset_id                  = "apex_db"
  friendly_name               = "Apex dataset"
  description                 = "this dataset contains the apex dataset"
  location                    = "US"
  default_table_expiration_ms = null
}

# Creating the prodcut table
resource "google_bigquery_table" "apexproducts" {
  dataset_id = google_bigquery_dataset.apexdataset.dataset_id
  table_id   = "products"

  time_partitioning {
    type = "DAY"
  }

  labels = {
    env       = "default"
    data_type = "product"
  }

  schema = <<EOF
[
  {
    "name": "product_id",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "product_name",
    "type": "STRING",
    "mode": "NULLABLE"
  }
]
EOF
}


#creating the region table

resource "google_bigquery_table" "apexregions" {
  dataset_id = google_bigquery_dataset.apexdataset.dataset_id
  table_id   = "regions"

  time_partitioning {
    type = "DAY"
  }

  labels = {
    env       = "default"
    data_type = "region"
  }

  schema = <<EOF
[
  {
    "name": "region_id",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "country",
    "type": "STRING",
    "mode": "NULLABLE"
  },

  {
    "name": "region_name",
    "type": "STRING",
    "mode": "NULLABLE"
  }
]
EOF
}

