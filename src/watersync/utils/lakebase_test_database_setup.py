from __future__ import annotations

import itertools
import random
from datetime import datetime, timedelta

import psycopg2
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import Project, ProjectSpec


class LakebaseTestDatabaseSetup:
    def __init__(
        self,
        project_id: str = "slalom-jdbc-test",
        project_display_name: str = "Slalom JDBC Test DB",
        database_name: str = "databricks_postgres",
        workspace_client: WorkspaceClient | None = None,
    ):
        self.w = workspace_client or WorkspaceClient()
        self.project_id = project_id
        self.project_display_name = project_display_name
        self.database_name = database_name
        self.username = self.w.current_user.me().user_name

    def ensure_project(self):
        for project in itertools.islice(self.w.postgres.list_projects(page_size=50), 100):
            if project.name == f"projects/{self.project_id}":
                return project
        operation = self.w.postgres.create_project(
            project=Project(spec=ProjectSpec(display_name=self.project_display_name, pg_version=17)),
            project_id=self.project_id,
        )
        return operation.wait()

    def resolve_endpoint(self):
        branch = next(iter(self.w.postgres.list_branches(parent=f"projects/{self.project_id}")))
        endpoint = next(iter(self.w.postgres.list_endpoints(parent=branch.name)))
        return branch, endpoint

    def generate_token(self, endpoint_name: str) -> str:
        return self.w.postgres.generate_database_credential(endpoint=endpoint_name).token

    def connection(self):
        _, endpoint = self.resolve_endpoint()
        token = self.generate_token(endpoint.name)
        return psycopg2.connect(
            host=endpoint.status.hosts.host,
            port=5432,
            dbname=self.database_name,
            user=self.username,
            password=token,
            sslmode="require",
        )

    def create_standard_tables(self) -> None:
        with self.connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS dbo")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS dbo.customers (
                        customer_id SERIAL PRIMARY KEY,
                        first_name VARCHAR(100),
                        last_name VARCHAR(100),
                        email VARCHAR(255),
                        city VARCHAR(100),
                        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS dbo.products (
                        product_id SERIAL PRIMARY KEY,
                        sku VARCHAR(50),
                        product_name VARCHAR(255),
                        category VARCHAR(100),
                        price NUMERIC(12, 2),
                        last_changed TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS dbo.orders (
                        order_id INTEGER NOT NULL,
                        line_id INTEGER NOT NULL,
                        customer_id INTEGER,
                        product_id INTEGER,
                        quantity INTEGER,
                        order_status VARCHAR(50),
                        modified_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (order_id, line_id)
                    )
                """)

    def seed_standard_data(self, customer_count: int = 200, product_count: int = 100, order_count: int = 500) -> None:
        first_names = ["Alice", "Bob", "Carol", "David", "Eva", "Frank", "Grace", "Henry"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Davis", "Miller"]
        cities = ["New York", "London", "Berlin", "Paris", "Milan", "Toronto", "Sydney"]
        categories = ["Hardware", "Software", "Services", "Support"]
        statuses = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED"]

        with self.connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM dbo.customers")
                if cur.fetchone()[0] == 0:
                    for _ in range(customer_count):
                        first_name = random.choice(first_names)
                        last_name = random.choice(last_names)
                        cur.execute(
                            "INSERT INTO dbo.customers(first_name, last_name, email, city, updated_at) VALUES (%s, %s, %s, %s, %s)",
                            (first_name, last_name, f"{first_name.lower()}.{last_name.lower()}@example.com", random.choice(cities), datetime.now() - timedelta(days=random.randint(1, 60))),
                        )
                cur.execute("SELECT COUNT(*) FROM dbo.products")
                if cur.fetchone()[0] == 0:
                    for idx in range(product_count):
                        cur.execute(
                            "INSERT INTO dbo.products(sku, product_name, category, price, last_changed) VALUES (%s, %s, %s, %s, %s)",
                            (f"SKU-{idx:04d}", f"Product {idx:04d}", random.choice(categories), round(random.uniform(10, 500), 2), datetime.now() - timedelta(days=random.randint(1, 60))),
                        )
                cur.execute("SELECT COUNT(*) FROM dbo.orders")
                if cur.fetchone()[0] == 0:
                    for order_id in range(1, order_count + 1):
                        line_total = random.randint(1, 4)
                        for line_id in range(1, line_total + 1):
                            cur.execute(
                                "INSERT INTO dbo.orders(order_id, line_id, customer_id, product_id, quantity, order_status, modified_at) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                                (order_id, line_id, random.randint(1, customer_count), random.randint(1, product_count), random.randint(1, 5), random.choice(statuses), datetime.now() - timedelta(days=random.randint(1, 60))),
                            )

    def simulate_updates(self, customer_rows: int = 3, product_rows: int = 2, order_rows: int = 5) -> None:
        statuses = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "RETURNED"]
        cities = ["New York", "London", "Berlin", "Paris", "Milan", "Toronto", "Sydney"]
        with self.connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                for _ in range(customer_rows):
                    cur.execute(
                        "UPDATE dbo.customers SET city = %s, updated_at = %s WHERE customer_id = (SELECT customer_id FROM dbo.customers ORDER BY random() LIMIT 1)",
                        (random.choice(cities), datetime.now() - timedelta(seconds=random.randint(1, 30))),
                    )
                for _ in range(product_rows):
                    cur.execute(
                        "UPDATE dbo.products SET price = price + 1, last_changed = %s WHERE product_id = (SELECT product_id FROM dbo.products ORDER BY random() LIMIT 1)",
                        (datetime.now() - timedelta(seconds=random.randint(1, 30)),),
                    )
                for _ in range(order_rows):
                    cur.execute(
                        "UPDATE dbo.orders SET order_status = %s, modified_at = %s WHERE (order_id, line_id) = (SELECT order_id, line_id FROM dbo.orders ORDER BY random() LIMIT 1)",
                        (random.choice(statuses), datetime.now() - timedelta(seconds=random.randint(1, 30))),
                    )

    def jdbc_settings(self) -> dict[str, str]:
        _, endpoint = self.resolve_endpoint()
        host = endpoint.status.hosts.host
        return {
            "jdbc_url": f"jdbc:postgresql://{host}:5432/{self.database_name}?sslmode=require",
            "jdbc_user": self.username,
            "jdbc_password_hint": f"w.postgres.generate_database_credential(endpoint='{endpoint.name}').token",
            "jdbc_driver": "org.postgresql.Driver",
            "fetch_size": "10000",
            "num_partitions": "8",
        }
