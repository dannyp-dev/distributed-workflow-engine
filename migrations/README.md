# Migrations

Database migrations are applied in filename order by the Compose `migrate` service.

Migrations must be safe to run more than once because the service runs whenever the stack starts. Use a new numbered file for schema changes instead of editing a migration that has already been deployed.
