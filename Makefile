init-infra:
	cd infra/terraform && terraform init
plan-infra:
	cd infra/terraform && terraform plan
apply-infra:
	cd infra/terraform && terraform apply
destroy-infra:
	cd infra/terraform && terraform destroy
up:
	cd airflow && docker-compose up airflow-init && docker-compose up --build -d
down:
	cd airflow && docker-compose down