# Deploy on AWS: EC2 + S3 data lake

Runs the whole pipeline on **one EC2 server** with **Amazon S3** as the data lake. PostgreSQL,
Airflow and Metabase stay in Docker on the server. AWS credentials come from an **IAM role**
attached to the server, so there are **no AWS keys** in `.env` or in the code.

```
 Your PC ──SSH tunnel──► EC2 t3.xlarge (Docker Compose)          Amazon S3
   browser                ├─ Airflow (scheduler, UI)  ──────►  <prefix>-bronze
   localhost:3000/8080    ├─ Spark (inside Airflow tasks) ───►  <prefix>-silver
                          ├─ PostgreSQL warehouse              <prefix>-gold
                          └─ Metabase                    (IAM role, no keys)
```

**Cost** (Mumbai, `ap-south-1`, approximate, check the
[AWS Pricing Calculator](https://calculator.aws/)): t3.xlarge about **$0.18/hour while running**,
40 GB disk about **$4/month** (also while stopped), public IPv4 about $0.005/hour while running,
S3 a few cents. **Stop the server when you are not using it.** Running 24/7 costs about
$135/month.

Placeholders used below: `<prefix>` = your bucket prefix, e.g. `adithyan-sales` (lower case,
digits, `-`; must be globally unique). `<region>` = `ap-south-1`.

---

## Step 0: budget alert (do this first)

1. AWS console → **Billing and Cost Management** → **Budgets** → **Create budget**.
2. **Use a template** → **Monthly cost budget**. Amount: e.g. **$30**. Enter your email.
3. Create. You get emails at 85% and 100%. Optionally add a second budget at $10.

Also check **Billing → Credits** for the credit expiry date and eligible services.

---

## Step 1: S3 buckets

Console → **S3** → **Create bucket**, three times, in region **Asia Pacific (Mumbai) ap-south-1**:

* `<prefix>-bronze`
* `<prefix>-silver`
* `<prefix>-gold`

Keep the defaults: **Block all public access = ON**, encryption **SSE-S3**, versioning off.

(The pipeline can also create them itself, but then the IAM role needs `s3:CreateBucket`.
Creating them by hand keeps the role smaller.)

---

## Step 2: IAM role for the server (least privilege)

**2a. Policy.** Console → **IAM** → **Policies** → **Create policy** → **JSON**. Paste and replace
`<prefix>` (6 places):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "LakeBuckets",
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation", "s3:ListBucketMultipartUploads"],
      "Resource": [
        "arn:aws:s3:::<prefix>-bronze",
        "arn:aws:s3:::<prefix>-silver",
        "arn:aws:s3:::<prefix>-gold"
      ]
    },
    {
      "Sid": "LakeObjects",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"],
      "Resource": [
        "arn:aws:s3:::<prefix>-bronze/*",
        "arn:aws:s3:::<prefix>-silver/*",
        "arn:aws:s3:::<prefix>-gold/*"
      ]
    }
  ]
}
```

Name it `sales-pipeline-lake-access`.

**2b. Role.** IAM → **Roles** → **Create role** → Trusted entity **AWS service** → Use case
**EC2** → Next → attach `sales-pipeline-lake-access` → name the role `sales-pipeline-ec2`.

The server can now read and write **only these three buckets**, nothing else in your account.

---

## Step 3: launch the EC2 server

Console → **EC2** (region **ap-south-1**) → **Launch instance**:

| Setting | Value |
|---|---|
| Name | `sales-pipeline` |
| AMI | **Ubuntu Server 24.04 LTS**, 64-bit **(x86)** |
| Instance type | **t3.xlarge** (4 vCPU, 16 GiB). Smaller types are untested and likely run out of memory |
| Key pair | **Create new key pair** → RSA, `.pem` → save it, e.g. `C:\Users\<you>\.ssh\sales-pipeline.pem` |
| Network → Security group | **Create**: allow **SSH (22)** from **My IP** only. **Nothing else.** |
| Storage | **40 GiB gp3** |
| Advanced details → IAM instance profile | **`sales-pipeline-ec2`** |
| Advanced details → Shutdown behavior | **Stop** (NOT Terminate) |
| Advanced details → Metadata version | **V2 only (token required)** |
| Advanced details → **Metadata response hop limit** | **2** ← important |

> **Why hop limit 2?** The pipeline runs inside Docker containers. The IAM role's credentials
> come from the instance metadata service, and each network hop (host → container) counts. With
> the default of 1, code inside containers gets **no credentials** and S3 access fails with
> "Unable to locate credentials" / `NoAwsCredentialsException`.
>
> Already launched with 1? EC2 → select instance → **Actions → Instance settings → Modify
> instance metadata options** → hop limit **2**.

Click **Launch instance**. Note its **Public IPv4 address** (it changes after each stop/start).

---

## Step 4: connect from Windows

PowerShell on your PC. Restrict the key file first (SSH refuses keys others can read):

```powershell
icacls "$env:USERPROFILE\.ssh\sales-pipeline.pem" /inheritance:r /grant:r "$($env:USERNAME):R"
ssh -i "$env:USERPROFILE\.ssh\sales-pipeline.pem" ubuntu@<public-ip>
```

Type `yes` the first time.

---

## Step 5: install Docker on the server

On the server:

```bash
sudo apt-get update && sudo apt-get -y upgrade
curl -fsSL https://get.docker.com | sudo sh        # Docker Engine + Compose plugin
sudo usermod -aG docker ubuntu
exit                                               # log out so the group change applies
```

Reconnect (Step 4), then check:

```bash
docker --version && docker compose version          # Compose must be 2.24 or newer
```

Quick IAM check (should print an access key id starting with `ASIA`; no AWS CLI needed):

```bash
TOKEN=$(curl -s -X PUT http://169.254.169.254/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 60")
curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/iam/security-credentials/sales-pipeline-ec2 | head -5
```

---

## Step 6: get the code and configure

```bash
git clone https://github.com/ADITHYANKG/sales-medallion-lakehouse.git
cd sales-medallion-lakehouse
python3 scripts/setup_env.py --aws --region ap-south-1 --bucket-prefix <prefix>
```

(Private repository? Use a GitHub personal access token as the password, or copy the code with
`scp`. Never copy your local `.env` to the server.)

`setup_env.py --aws` writes a `.env` with:

* fresh random passwords for everything,
* `LAKE_STORAGE=aws`, `S3_REGION`, and the three bucket names,
* no `S3_ENDPOINT_URL` and **no AWS keys** (the IAM role is used),
* `COMPOSE_FILE=compose.yaml:compose.aws.yaml`, so every `docker compose` command below uses
  S3 and does not start MinIO.

Read the generated passwords with `grep PASSWORD .env` (you need the Airflow and Metabase ones).

---

## Step 7: build and start

```bash
docker compose up -d --build          # first build ~10-15 minutes
docker compose ps                     # wait until services are healthy (1-2 minutes)
docker compose logs init              # should end with migrations_applied / buckets_created: []
```

`buckets_created: []` is correct: the buckets already exist. An `AccessDenied` or credentials
error here means Step 2 or the hop limit (Step 3) is wrong.

---

## Step 8: load data and dashboards

The server starts empty. Load the same demo data as on your PC (deterministic generator,
2026-01-01 to 2026-03-10, about 25 minutes), then build the dashboards:

```bash
docker compose run --rm app python -m src.cli lake-run --date 2026-01-01 --days 69
docker compose run --rm app python -m src.cli provision-metabase
docker compose run --rm app python scripts/verify_pipeline.py
```

Turn on the daily schedule (DAGs start paused on a new server):

```bash
docker compose exec airflow-scheduler airflow dags unpause sales_data_pipeline
```

Check S3: console → S3 → `<prefix>-bronze` → `sales/ingestion_date=…/` folders should be there.

---

## Step 9: open the UIs (SSH tunnel)

Nothing is exposed to the internet. On your PC, open a tunnel and keep the window open:

```powershell
ssh -i "$env:USERPROFILE\.ssh\sales-pipeline.pem" -N `
    -L 3000:localhost:3000 -L 8080:localhost:8080 ubuntu@<public-ip>
```

Then browse to:

* **Metabase:** http://localhost:3000 (`METABASE_ADMIN_EMAIL` / `METABASE_ADMIN_PASSWORD` from the
  **server's** `.env`). `provision-metabase` printed the dashboard URLs.
* **Airflow:** http://localhost:8080 (`admin` / `AIRFLOW_ADMIN_PASSWORD`).
* **Lake files:** the S3 console (there is no MinIO on AWS).

Stop the local Docker stack on your PC first, or use other local ports (e.g.
`-L 13000:localhost:3000`), because your PC already uses 3000 and 8080.

---

## Step 10: save money (stop when not in use)

**Stop** (not terminate): EC2 → select instance → **Instance state → Stop instance**. Data on the
disk and in S3 is kept. **Start** again the same way; the public IP changes.

After starting, connect and run (safe even if already running):

```bash
cd sales-medallion-lakehouse && docker compose up -d
```

**Automatic stop every night** (in case you forget). On the server:

```bash
sudo crontab -e
# add this line: stop at 18:30 UTC = 00:00 India time
30 18 * * * /sbin/shutdown -h now
```

This only **stops** the instance because Shutdown behavior = Stop (Step 3).

While the server is stopped, the 00:00 UTC Airflow run does not happen. After a start, Airflow
runs the latest missed day once; backfill older days if needed (see `docs/HOW_IT_WORKS.md`).

---

## Updating the code later

```bash
cd sales-medallion-lakehouse
git pull
docker compose up -d --build
```

---

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `Unable to locate credentials`, `NoAwsCredentialsException`, IMDS timeout | Hop limit is 1 (Step 3), or the IAM role is not attached to the instance |
| `AccessDenied` on S3 | Bucket names in `.env` differ from the policy, or a typo in `<prefix>` |
| `PermanentRedirect` / `AuthorizationHeaderMalformed` | `S3_REGION` in `.env` differs from the buckets' region |
| Containers restarting, out of memory | Instance smaller than t3.xlarge; check `free -h` and `docker stats` |
| SSH times out | Your home IP changed: update the security group rule "My IP" |
| `no space left on device` | Disk below 40 GB, or old images: `docker system prune` |
| `!override` / `depends_on` errors | Compose older than 2.24: reinstall Docker with Step 5 |

---

## Tearing everything down

To stop all charges:

1. EC2 → instance → **Terminate** (deletes the server and its disk).
2. S3 → each bucket → **Empty**, then **Delete**.
3. IAM → delete role `sales-pipeline-ec2` and policy `sales-pipeline-lake-access`.
4. EC2 → Key pairs and Security groups → delete the ones created (free, just tidy).

---

## What this deployment does and does not do

Done: S3 lake with IAM role (no keys), least-privilege bucket policy, no public ports, budget
alert, encrypted S3 (SSE-S3), random secrets, nightly auto-stop.

Not done (see [PRODUCTION_READINESS.md](PRODUCTION_READINESS.md)): HTTPS / public dashboard
link, managed database (RDS), backups, CI/CD, central logs and alerts, high availability.

**Testing status:** the AWS mode is covered by unit tests (settings, boto3 and Spark S3A
configuration, bucket creation, `.env` generation) and the Compose override was validated with
`docker compose config`. Local MinIO mode still passes the full test suite. A real deployment to
AWS has not been run yet.
