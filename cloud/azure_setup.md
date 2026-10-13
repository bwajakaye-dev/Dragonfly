# Running Dragonfly against Kafka on an Azure VM

This guide puts a **single Kafka broker** on a small Azure virtual machine.
Then it runs the transmitters and the switchboard on your laptop, connected to
that broker **over the internet**. Only the broker list changes; the code is
identical to the local setup.

```
 your laptop                                   Azure VM (Ubuntu + Docker)
┌───────────────────────────┐   internet     ┌──────────────────────────┐
│ transmitter.py  x N  ─────┼──── :9092 ────▶│  Kafka broker (KRaft)    │
│ switchboard.py   ◀────────┼──── :9092 ─────│  docker-compose.cloud.yml│
└───────────────────────────┘ (only your IP) └──────────────────────────┘
```

> **One broker is not fault tolerant.** This setup shows the system working
> across a real network. The broker-loss drill needs the 3-broker local
> cluster (`docker-compose.yml`).

**Time needed:** about 20 minutes. **Cost:** a few cents per hour while the VM
runs; see [Controlling cost](#7-controlling-cost-do-this) before you start.

---

## 0. Prerequisites (on your laptop)

| Tool | Install (Windows) | Check |
|---|---|---|
| Azure subscription | Azure for Students works | sign in at <https://portal.azure.com> |
| Azure CLI | `winget install -e --id Microsoft.AzureCLI` | `az version` |
| OpenSSH client | built into Windows 10/11 | `ssh -V` |
| This repo + venv | see the main README | `python transmitter.py --help` |

Run all of the laptop commands below in **PowerShell**, from the repo root.

```powershell
az login                                   # opens a browser to sign in
az account show --query name -o tsv        # confirm the right subscription
```

## 1. Choose names and find your public IP

```powershell
$RG    = "dragonfly-rg"
$VM    = "dragonfly-kafka"
$LOC   = "eastus"                          # any region your subscription allows
$MyIp  = (Invoke-RestMethod https://api.ipify.org)
"my public IP: $MyIp"
```

`$MyIp` is the only address that will be allowed to reach the VM.

## 2. Create the VM

```powershell
az group create --name $RG --location $LOC

az vm create `
  --resource-group $RG --name $VM `
  --image Ubuntu2204 --size Standard_B2s `
  --admin-username azureuser --generate-ssh-keys `
  --public-ip-sku Standard `
  --nsg-rule NONE

$Ip = az vm show -d -g $RG -n $VM --query publicIps -o tsv
"VM public IP: $Ip"
```

* **Standard_B2s** has 2 vCPUs and 4 GB RAM. `Standard_B1ms` (1 vCPU, 2 GB) also
  works with the capped heap and costs less. If a size is not offered in your
  region or subscription, pick another region or size.
* `--nsg-rule NONE` creates the VM's firewall, the **network security group**
  (NSG) named `dragonfly-kafkaNSG`, with **no inbound rules**. Nothing can
  reach the VM until step 3.
* A Standard-SKU public IP is **static**: it stays the same when you stop and
  start the VM, so the broker address in your commands keeps working.

## 3. Open SSH and Kafka to your IP only

```powershell
az network nsg rule create -g $RG --nsg-name "${VM}NSG" `
  --name allow-ssh-from-me --priority 1000 --direction Inbound --access Allow `
  --protocol Tcp --source-address-prefixes "$MyIp/32" --destination-port-ranges 22

az network nsg rule create -g $RG --nsg-name "${VM}NSG" `
  --name allow-kafka-from-me --priority 1010 --direction Inbound --access Allow `
  --protocol Tcp --source-address-prefixes "$MyIp/32" --destination-port-ranges 9092
```

Kafka here uses **PLAINTEXT with no authentication**. The NSG rule
restricted to `/32` (exactly one address) is the only thing keeping strangers
out. Never open 9092 to `*` or `Internet`.

**When your IP changes** (for example at home vs. on campus for the demo),
update both rules before connecting:

```powershell
$MyIp = (Invoke-RestMethod https://api.ipify.org)
foreach ($rule in "allow-ssh-from-me", "allow-kafka-from-me") {
  az network nsg rule update -g $RG --nsg-name "${VM}NSG" -n $rule --source-address-prefixes "$MyIp/32"
}
```

## 4. Install Docker on the VM and start Kafka

Copy the compose file and the example environment file to the VM:

```powershell
ssh azureuser@$Ip "mkdir -p ~/dragonfly"
scp cloud/docker-compose.cloud.yml cloud/.env.example azureuser@${Ip}:~/dragonfly/
ssh azureuser@$Ip
```

Now you are **on the VM** (bash):

```bash
# Docker Engine + Compose plugin (official convenience script)
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER
exit                                   # log out so the group change applies
```

```powershell
ssh azureuser@$Ip                      # back in
```

```bash
cd ~/dragonfly
cp .env.example .env
# Advertise the VM's PUBLIC address (see "Why advertised.listeners matters")
sed -i "s/REPLACE_WITH_VM_PUBLIC_IP/$(curl -s https://api.ipify.org)/" .env
cat .env                               # KAFKA_PUBLIC_HOST=<the VM's public IP>

docker compose -f docker-compose.cloud.yml up -d
docker compose -f docker-compose.cloud.yml logs --tail 20 kafka   # wait for "Kafka Server started"
free -h                                # the JVM heap is capped at 512 MB
exit
```

### Why `advertised.listeners` matters

A Kafka client connects in two steps:

1. It contacts the **bootstrap** address you gave it (`--brokers 20.x.x.x:9092`).
2. The broker replies with metadata that says "talk to me at
   *advertised listener*". Every later request goes to **that** address.

Inside Docker the broker only knows its container name and private address. If
it advertised `localhost:9092` or `10.0.0.4:9092`, your laptop would succeed at
step 1 and then fail at step 2, often with a confusing timeout. That's why
`KAFKA_ADVERTISED_LISTENERS` is built from `KAFKA_PUBLIC_HOST` in `.env`.
The compose file refuses to start if that variable is missing.

## 5. Create the topics (from your laptop)

```powershell
Test-NetConnection $Ip -Port 9092      # TcpTestSucceeded : True
python scripts/create_topics.py --brokers "${Ip}:9092" --replication-factor 1 --min-isr 1
```

With one broker the replication factor must be 1. The script refuses 3 and
says why.

## 6. Run Dragonfly over the internet

Point every process at the VM. The environment variable saves retyping it in
each terminal:

```powershell
$env:DRAGONFLY_BROKERS = "${Ip}:9092"

# terminal 1
python switchboard.py

# terminal 2 (each transmitter window inherits DRAGONFLY_BROKERS)
.\scripts\run_demo_local.ps1 -MaxWidth 640
```

**Mind your upload bandwidth.** Every frame goes up to the VM and comes back
down to the switchboard. The transmitter's stats line shows `KB/frame`, and
the bandwidth per camera is about `KB/frame x fps x 8 / 1000` Mbit/s. Measured
on the sample clips at 640 px wide and quality 80, a landscape clip averages
about 25 KB/frame (about 6 Mbit/s at 30 fps). The portrait clips are taller at
the same width, about 50–56 KB/frame (about 12–13 Mbit/s). On a slow home
connection, reduce it:

```powershell
python transmitter.py --camera-id cam1 --file sample_videos/cam1.mp4 `
  --max-width 640 --fps 15 --jpeg-quality 70
```

What you should see:

* The multiview footer shows `Kafka: 1 broker(s)`.
* Latency is higher than locally, because the frames make two internet trips.
  The latency figure compares the transmitter's and the switchboard's clocks,
  so it is accurate here because both run on the same laptop.
* The as-run log works the same way:
  `python scripts/asrun_log.py --brokers "${Ip}:9092"`

## 7. Controlling cost (do this!)

**Stop the VM whenever you are not using it.** "Deallocate" releases the
compute, so you stop paying for CPU and RAM. The disk and the static IP still
cost a small amount.

```powershell
az vm deallocate -g $RG -n $VM         # stop billing for compute
az vm start      -g $RG -n $VM         # before the next session; same IP
az vm get-instance-view -g $RG -n $VM --query "instanceView.statuses[1].displayStatus" -o tsv
```

Shutting down from inside Linux (`sudo poweroff`) does **not** deallocate.
The VM shows as "Stopped" but is still billed. Always use `az vm deallocate`
or the portal's **Stop** button.

Docker and Kafka start again by themselves after `az vm start`
(`restart: unless-stopped`), and topics persist on the Docker volume.

### Set a budget alert

The portal makes this easiest:

1. Open <https://portal.azure.com>, search for **Cost Management**, and open it.
2. Go to **Budgets**, then **+ Add**.
3. Set **Scope** to your subscription, **Reset period** to *Monthly*, and
   **Amount** to, for example, `10` (USD).
4. Add alert conditions of type *Actual* at **50 %**, **80 %** and **100 %**,
   and enter your email address.
5. Click **Create**.

A budget only **alerts**; it does not stop anything. Deallocating is what
saves money.

### Remove everything when the course is over

```powershell
az group delete --name $RG --yes --no-wait   # deletes the VM, disk, IP, NSG
```

## 8. Troubleshooting

| Symptom | Likely cause and fix |
|---|---|
| `Test-NetConnection` fails | Your IP changed: update the NSG rules (step 3). The VM may be deallocated: run `az vm start`. Or Kafka is not running: `ssh` in and run `docker compose -f docker-compose.cloud.yml ps`. |
| `create_topics.py` hangs, then "cannot reach Kafka" | Same as above. |
| Bootstrap works, then timeouts | `KAFKA_PUBLIC_HOST` in `~/dragonfly/.env` is wrong. Fix it, then `docker compose -f docker-compose.cloud.yml up -d`. |
| Kafka container restarts repeatedly | Out of memory: run `docker logs dragonfly-kafka`. Use B2s, or keep the 512 MB heap cap. |
| Low fps and rising `failed` in transmitter stats | Not enough upload bandwidth: lower `--max-width`, `--fps` or `--jpeg-quality`. |
| `replication factor 3 needs 3 brokers` | Pass `--replication-factor 1 --min-isr 1` for the single cloud broker. |
