import subprocess


def is_number(s):
    try:
        float(s) 
        return True
    except ValueError:
        return False


class pgTune:
    db_version="16"
    db_cpu=1
    db_memory="1GB"
    db_storage="ssd"
    db_type="web"
    db_maxconn=100
    db_tune={}

    def __init__(self, db_version, db_cpu, db_memory, db_storage, db_type, db_maxconn):
        self.db_version = db_version
        self.db_cpu = db_cpu
        self.db_memory = db_memory
        self.db_storage = db_storage
        self.db_type = db_type
        self.db_maxconn = db_maxconn

    def get_pg_tune(self):
        cmd = "pgtune.sh"
        self.db_tune={}
        result = subprocess.run(['bash', cmd , 
                                 '-u' , str(self.db_cpu),
                                '-m', self.db_memory,
                                '-v', self.db_version,
                                '-s', self.db_storage,
                                '-t', self.db_type,
                                '-c', str(self.db_maxconn)
                                ]
                                , stdout=subprocess.PIPE)
        lines = result.stdout.decode('utf-8').split("\n")
        for line in lines:
            if ("==" in line):
                values = line.split("==",1)
                parameter = values[0].strip()
                parameter_value = values[1].strip()
                self.db_tune[parameter]=parameter_value

        return self.db_tune

    def get_docker_cmd(self, db_config, db_version):
        """
        Generate a docker-compose YAML configuration for a PostgreSQL container
        with a persistent Docker volume automatically created based on the database name.

        Args:
            db_config (dict): Must contain:
                - db_name: database name
                - db_port: port to expose
                - db_user: PostgreSQL username
            db_version (str): PostgreSQL version, e.g. "17" (image will be postgres:17-alpine)

        Returns:
            str: docker-compose YAML content as a string
        """

        # Hardcoded password placeholder for security reasons (should be replaced manually)
        db_password = "xxxxx"

        # Service and volume names derived from db_name
        service_name = f"{db_config['db_name']}-db"
        volume_name = f"{db_config['db_name']}_data"

        # Header (compose version is optional but recommended for clarity)
        docker_cmd = "services:\n"

        # Build the main PostgreSQL service definition
        docker_cmd += (
            f"  {service_name}:\n"
            f"    restart: always\n"
            f"    image: postgres:{db_version}-alpine\n"
            f"    shm_size: {self.db_tune.get('shared_buffers', '128mb')}\n"
            f"    # Optional: Use tmpfs for shared memory when running in Swarm mode\n"
            f"    #volumes:\n"
            f"    #  - type: tmpfs\n"
            f"    #    target: /dev/shm\n"
            f"    #    tmpfs:\n"
            f"    #      size: {self.db_tune.get('shared_buffers', '128mb')}\n"
            f"    volumes:\n"
            f"      - {volume_name}:/var/lib/postgresql/data\n"
            f"    ports:\n"
            f"      - \"{db_config['db_port']}:5432\"\n"
            f"    deploy:\n"
            f"      resources:\n"
            f"        limits:\n"
            f"          cpus: \"{self.db_cpu}.0\"\n"
            f"          memory: {self.db_memory}\n"
            f"    environment:\n"
            f"      - POSTGRES_USER={db_config['db_user']}\n"
            f"      - POSTGRES_PASSWORD={db_password}\n"
            f"      - POSTGRES_DB={db_config.get('db_name', db_config['db_user'])}\n"
            f"      - POSTGRES_INITDB_ARGS=--auth-local=scram-sha-256 --auth-host=scram-sha-256\n"  
            f"    healthcheck:\n"
            f"      test: [\"CMD\", \"pg_isready\", \"-U\", \"{db_config['db_user']}\"]\n"
            f"      interval: 10s\n"
            f"      timeout: 5s\n"
            f"      retries: 5\n"
            f"    command: >\n"
            f"      postgres\n"
            f"        -c shared_preload_libraries='pg_stat_statements'\n"
            f"        -c autovacuum=on\n"
        )

        # Add database tuning parameters dynamically
        for param, value in self.db_tune.items():
            # Skip parameters already included manually
            if param in {"shared_preload_libraries", "autovacuum"}:
                continue

            # Numeric values are written as-is, strings are quoted
            if is_number(value):
                docker_cmd += f"        -c {param}={value}\n"
            else:
                docker_cmd += f"        -c {param}='{value}'\n"
            

        # Add root-level volume declaration for persistence
        docker_cmd += f"\nvolumes:\n  {volume_name}:\n"

        return docker_cmd
    
    def get_kube_cmd(self, db_config, db_version):
        """
        Generate a Kubernetes Deployment YAML descriptor for a PostgreSQL instance.
        - Normalizes memory quantities to Kubernetes units (Mi/Gi)
        - Adds resources.requests
        - Adds readiness/liveness probes using pg_isready
        - Maps /dev/shm via emptyDir(medium: Memory) with sizeLimit

        Args:
            db_config (dict): { db_name, db_user, db_port }
            db_version (str): e.g. "17" or "18"
        Returns:
            str: Kubernetes YAML manifest
        """

        def _to_k8s_quantity(val: str) -> str:
            """
            Convert common human units to K8s-compliant quantities.
            Examples:
            '512MB' -> '512Mi'
            '2GB'   -> '2Gi'
            '256mb' -> '256Mi'
            '1g'    -> '1Gi'
            Already-k8s units (Mi/Gi) are returned as-is.
            """
            if val is None:
                return val
            s = str(val).strip()

            # Already in Mi/Gi -> return as-is
            if s.lower().endswith(("mi", "gi")):
                return s

            # Normalize common suffixes to Gi/Mi
            lower = s.lower().replace(" ", "")
            # If pure number, assume Mi (conservative default)
            if lower.isdigit():
                return f"{lower}Mi"

            # map endings
            replacements = {
                "gib": "Gi", "gb": "Gi", "g": "Gi",
                "mib": "Mi", "mb": "Mi", "m": "Mi",
                "kib": "Ki", "kb": "Ki", "k": "Ki",
                "b": ""  # bytes not recommended for readability; leave empty
            }
            for suf, rep in replacements.items():
                if lower.endswith(suf):
                    num = lower[: -len(suf)]
                    # Guard: if the numeric part contains quotes or stray chars, keep original
                    try:
                        float(num)  # just a sanity check
                        # Drop decimals for k8s quantities to keep it simple; if decimals exist, keep them
                        return f"{num}{rep}"
                    except ValueError:
                        return s  # fallback: return original
            return s  # fallback: return original

        app_name = f"{db_config['db_name']}-db"

        # shared_mem based on shared_buffers; default '128mb' -> normalize to Mi
        shared_mem = _to_k8s_quantity(self.db_tune.get('shared_buffers', '128mb'))

        # Normalize container memory limit (self.db_memory might be '2GB', etc.)
        mem_limit = _to_k8s_quantity(getattr(self, "db_memory", "2Gi"))
        # Derive a reasonable request (50% of limit if possible); if not numeric, use a sane default
        try:
            # crude parser to split number + unit
            import re
            m = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([KMG]i)\s*$", mem_limit, re.IGNORECASE)
            if m:
                num = float(m.group(1)) / 2.0
                unit = m.group(2)
                mem_request = f"{num:.0f}{unit}"
            else:
                mem_request = "1Gi"
        except Exception:
            mem_request = "1Gi"

        # CPU: keep the provided limit; request half if it's an int, else a safe default
        cpu_limit = str(getattr(self, "db_cpu", "1"))
        try:
            cpu_float = float(cpu_limit)
            cpu_request = f"{cpu_float/2:.1f}".rstrip("0").rstrip(".")
            if cpu_request == "":
                cpu_request = "0.5"
        except Exception:
            cpu_request = "500m"  # fallback

        kube_yaml = (
    f"apiVersion: apps/v1\n"
    f"kind: Deployment\n"
    f"metadata:\n"
    f"  name: {app_name}\n"
    f"  labels:\n"
    f"    app: {app_name}\n"
    f"spec:\n"
    f"  replicas: 1\n"
    f"  selector:\n"
    f"    matchLabels:\n"
    f"      app: {app_name}\n"
    f"  template:\n"
    f"    metadata:\n"
    f"      labels:\n"
    f"        app: {app_name}\n"
    f"    spec:\n"
    f"      containers:\n"
    f"      - name: postgres\n"
    f"        image: postgres:{db_version}-alpine\n"
    f"        ports:\n"
    f"        - containerPort: 5432\n"
    f"        env:\n"
    f"        - name: POSTGRES_USER\n"
    f"          value: \"{db_config['db_user']}\"\n"
    f"        - name: POSTGRES_PASSWORD\n"
    f"          value: \"xxxxx\"\n"
    f"        - name: POSTGRES_DB\n"
    f"          value: \"{db_config.get('db_name', db_config['db_user'])}\"\n"
    f"        resources:\n"
    f"          requests:\n"
    f"            cpu: \"{cpu_request}\"\n"
    f"            memory: \"{mem_request}\"\n"
    f"          limits:\n"
    f"            cpu: \"{cpu_limit}\"\n"
    f"            memory: \"{mem_limit}\"\n"
    f"        command: [\"postgres\"]\n"
    f"        args:\n"
    f"          - \"-c\"\n"
    f"          - \"shared_preload_libraries=pg_stat_statements\"\n"
    f"          - \"-c\"\n"
    f"          - \"autovacuum=on\"\n"
        )

        # Add PostgreSQL tuning parameters dynamically
        for param, value in self.db_tune.items():
            # Keep quoting logic for non-numeric strings (e.g. '512MB')
            is_number_like = isinstance(value, (int, float)) or (
                isinstance(value, str) and value.replace('.', '', 1).isdigit()
            )
            if is_number_like:
                kube_yaml += f"          - \"-c\"\n          - \"{param}={value}\"\n"
            else:
                kube_yaml += f"          - \"-c\"\n          - \"{param}='{value}'\"\n"

        # Volume for /dev/shm (shared memory), with normalized sizeLimit
        kube_yaml += (
    f"        volumeMounts:\n"
    f"        - name: dshm\n"
    f"          mountPath: /dev/shm\n"
    f"        readinessProbe:\n"
    f"          exec:\n"
    f"            command: [\"pg_isready\", \"-U\", \"{db_config['db_user']}\", \"-h\", \"127.0.0.1\", \"-p\", \"5432\"]\n"
    f"          initialDelaySeconds: 10\n"
    f"          periodSeconds: 5\n"
    f"        livenessProbe:\n"
    f"          exec:\n"
    f"            command: [\"pg_isready\", \"-U\", \"{db_config['db_user']}\", \"-h\", \"127.0.0.1\", \"-p\", \"5432\"]\n"
    f"          initialDelaySeconds: 20\n"
    f"          periodSeconds: 10\n"
    f"      volumes:\n"
    f"      - name: dshm\n"
    f"        emptyDir:\n"
    f"          medium: Memory\n"
    f"          sizeLimit: \"{shared_mem}\"\n"
        )

        return kube_yaml

    def get_alter_system(self, running_values):
        sqlalter = ""
        for param in self.db_tune: 
            if param in running_values:  # Check if the key exists
                if self.db_tune[param] != running_values[param]:
                    if is_number(self.db_tune[param]):
                        sqlalter += f"ALTER SYSTEM SET {param}={self.db_tune[param]};\n"
                    else:
                        sqlalter += f"ALTER SYSTEM SET {param}='{self.db_tune[param]}';\n"
            else:
                print(f"Warning: Key '{param}' not found in running_values.")
        return sqlalter

