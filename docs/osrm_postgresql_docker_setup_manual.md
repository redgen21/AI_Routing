# OSRM / PostgreSQL / Docker / systemd 통합 설치 운영 매뉴얼

> 2026-07-18 경로 변경: 현재 기준 애플리케이션 루트는
> `/home/csda/AI_Routing/production`과 `/home/csda/AI_Routing/development`, OSRM 루트는
> `/home/osrm`이다. 실행 스크립트는 각 애플리케이션 루트에 둔다.
> 최신 배포 범위와 명령은 `services/README.md`를 우선한다.

이 문서는 `AI_Routing` 서버를 새로 구성하거나 재설치할 때 필요한 설치, 데이터 경로 이전, OSRM Docker 빌드/실행, 로그 제한, `systemd` 자동 시작, 재부팅 검증, 지도 데이터 갱신 절차를 하나로 정리한 통합 문서입니다.

기존 `server_auto_start_manual_20260422.md`의 자동 시작 내용은 이 문서의 `systemd` 및 운영 섹션으로 통합했습니다.

## 기준 경로와 포트

- AI Routing 프로젝트: `/home/csda/AI_Routing/production`
- OSRM 데이터/스크립트: `/home/osrm`
- PostgreSQL 데이터 위치: `/data/postgresql/14/main`
- PostgreSQL 데이터 디스크 마운트: `/data`
- Common VRP API: `8065`
- Smart Routing API: `8055`
- Streamlit Common VRP Client: `8501`
- OSRM Korea: `5000`
- OSRM LA/Socal: `5001`
- OSRM Atlanta/Georgia: `5002`

## 1. 기본 패키지 설치

Ubuntu 22.04 기준입니다.

```bash
sudo apt update
sudo apt install -y \
  git curl rsync unzip htop net-tools \
  python3 python3-venv python3-pip \
  postgresql postgresql-contrib
```

Docker는 현재 서버처럼 snap Docker를 사용할 수 있습니다.

```bash
sudo snap install docker
sudo systemctl enable snap.docker.dockerd
sudo systemctl start snap.docker.dockerd
sudo docker ps
```

일반 Docker CE를 쓰는 서버라면 Docker 공식 설치 절차를 따라 설치하고 `docker` 서비스가 떠 있는지 확인합니다.

```bash
sudo systemctl status docker
sudo docker ps
```

## 2. PostgreSQL 설치와 /data 데이터 디스크 설정

PostgreSQL 데이터는 OS 디스크(`/`)나 Azure resource disk(`/mnt`)가 아니라 별도 Managed Data Disk를 `/data`에 마운트해 저장합니다.

권장 기준:

- Azure Managed Data Disk: Premium SSD LRS
- Host caching: `None`
- 마운트 위치: `/data`
- PostgreSQL data directory: `/data/postgresql/14/main`

주의: Azure 기본 `/mnt`는 resource disk로 잡힐 수 있으며 VM size 변경, 재배포, 재시작 과정에서 사라지거나 초기화될 수 있습니다. DB 영구 저장소로 사용하지 않습니다.

### 2.1 디스크와 마운트 상태 확인

```bash
df -h
lsblk -f
findmnt /data
cat /etc/fstab
```

정상 예:

```text
/dev/sdb1  126G  ...  /data
```

`/data`가 보이지 않으면 Azure Portal에서 VM의 Data disks에 Managed Disk가 연결되어 있는지 먼저 확인합니다.

### 2.2 새 data disk를 /data에 마운트

새 디스크가 `/dev/sdb`로 보이는 예입니다. 실제 장치명은 `lsblk -f`로 확인합니다.

```bash
sudo parted /dev/sdb --script mklabel gpt mkpart primary ext4 0% 100%
sudo partprobe /dev/sdb
lsblk -f
```

파티션이 `/dev/sdb1`로 생기면 포맷하고 마운트합니다.

```bash
sudo mkfs.ext4 /dev/sdb1
sudo mkdir -p /data
sudo mount /dev/sdb1 /data
df -h
```

UUID를 확인합니다.

```bash
sudo blkid /dev/sdb1
```

`/etc/fstab` 맨 아래에 실제 UUID를 등록합니다.

```fstab
UUID=<DATA_DISK_UUID> /data ext4 defaults,nofail 0 2
```

예:

```fstab
UUID=511a7f62-2631-484d-a727-125555f258ac /data ext4 defaults,nofail 0 2
```

자동 마운트 검증:

```bash
sudo umount /data
sudo mount -a
findmnt /data
df -h
```

### 2.3 PostgreSQL cluster 상태 확인

PostgreSQL cluster 상태를 확인합니다.

```bash
pg_lsclusters
sudo systemctl status postgresql@14-main
```

처음 설치 직후 cluster가 내려가 있으면 시작합니다.

```bash
sudo pg_ctlcluster 14 main start
pg_lsclusters
```

### 2.4 PostgreSQL data directory를 /data로 설정

기본 PostgreSQL 데이터 경로는 보통 `/var/lib/postgresql/14/main`이고 root 파티션을 사용합니다. root가 작으면 DB checkpoint, init file, temp file 생성 실패로 PostgreSQL이 중단될 수 있습니다.

새로 DB를 구성하거나 기존 data directory가 사라진 경우에는 cluster를 `/data/postgresql/14/main`에 새로 만듭니다.

```bash
sudo systemctl stop postgresql@14-main || true
sudo pg_dropcluster 14 main --stop
sudo pg_createcluster 14 main --datadir=/data/postgresql/14/main
sudo systemctl start postgresql@14-main
pg_lsclusters
```

기존 cluster의 data directory가 없어 `Warning: corrupted cluster: data directory does not exist`가 나와도, 새 cluster를 만들 계획이면 다음 단계로 진행하면 됩니다.

정상 예:

```text
14  main  5432  online  postgres  /data/postgresql/14/main ...
```

기존 `/var/lib/postgresql/14/main`에 살아 있는 DB를 `/data`로 옮기는 경우에는 아래 절차를 사용합니다.

기존 데이터 경로를 확인합니다.

```bash
pg_lsclusters
```

예상 기존 상태:

```text
14  main  5432  online  postgres  /var/lib/postgresql/14/main ...
```

이전 절차:

```bash
sudo systemctl stop common-vrp.service || true
sudo systemctl stop postgresql@14-main

sudo mkdir -p /data/postgresql/14
sudo chown -R postgres:postgres /data/postgresql
sudo chmod 700 /data/postgresql

sudo rsync -aHAX --numeric-ids /var/lib/postgresql/14/main /data/postgresql/14/
sudo mv /var/lib/postgresql/14/main /var/lib/postgresql/14/main.bak
```

`postgresql.conf`의 data directory를 변경합니다.

```bash
sudo sed -i "s|^data_directory = .*|data_directory = '/data/postgresql/14/main'|" /etc/postgresql/14/main/postgresql.conf
```

권한을 확인합니다.

```bash
sudo chown -R postgres:postgres /data/postgresql/14/main
sudo chmod 700 /data/postgresql/14/main
```

시작 및 확인:

```bash
sudo systemctl start postgresql@14-main
pg_lsclusters
```

정상 예:

```text
14  main  5432  online  postgres  /data/postgresql/14/main ...
```

정상 확인 후 root 공간을 회수합니다.

```bash
sudo rm -rf /var/lib/postgresql/14/main.bak
df -h
```

### 2.5 DB와 계정 생성

DB와 계정을 생성합니다.

```bash
sudo -u postgres psql
```

`psql` 안에서 실행합니다.

```sql
create user vrp_agent with password '<DB_PASSWORD>';
create database vrp_db owner vrp_agent;
grant all privileges on database vrp_db to vrp_agent;
\q
```

운영 환경의 프로젝트 설정 파일은 `/home/csda/AI_Routing/production/config_common_vrp.json`입니다.

```json
{
  "database": {
    "host": "localhost",
    "port": 5432,
    "dbname": "vrp_db",
    "user": "vrp_agent",
    "password": "<DB_PASSWORD>"
  }
}
```

### 2.6 PostgreSQL 자동 실행 설정

부팅 시 PostgreSQL cluster를 자동 시작하도록 설정합니다.

```bash
sudo systemctl enable postgresql@14-main
sudo systemctl status postgresql@14-main
```

장애 후 자동 재시작을 원하면 override를 추가합니다.

```bash
sudo systemctl edit postgresql@14-main
```

내용:

```ini
[Service]
Restart=on-failure
RestartSec=5
```

적용:

```bash
sudo systemctl daemon-reload
sudo systemctl restart postgresql@14-main
```

### 2.7 root가 이미 100%라 sed가 실패할 때

`No space left on device`로 설정 파일 수정이 실패하면 먼저 root 공간을 확보합니다.

```bash
sudo journalctl --vacuum-size=200M
sudo apt clean
sudo find /var/log -type f -name "*.gz" -delete
sudo find /var/log -type f -name "*.1" -delete
df -h /
```

Docker 컨테이너 로그가 큰 경우:

```bash
sudo docker inspect osrm-korea --format '{{.LogPath}}'
sudo truncate -s 0 <LOG_PATH>
df -h /
```

공간이 조금이라도 생긴 뒤 다시 `sed` 명령을 실행합니다.

### 2.8 VM size 변경 또는 재부팅 후 확인

Azure에서 CPU/VM size를 변경해도 Managed Data Disk가 VM에 계속 attach되어 있고 `/etc/fstab`에 UUID로 등록되어 있으면 `/data`는 자동 마운트됩니다.

변경 후 확인:

```bash
findmnt /data
pg_lsclusters
sudo systemctl status postgresql@14-main --no-pager
sudo systemctl status common-vrp.service --no-pager
```

정상 기준:

- `/data`가 `/dev/sdX1` 같은 data disk로 마운트됨
- PostgreSQL `14 main 5432 online`
- PostgreSQL data directory가 `/data/postgresql/14/main`
- Common VRP API가 `active (running)`

## 3. Docker 로그 제한 설정

Docker 기본 로그 제한을 설정합니다. snap Docker 기준:

```bash
sudo mkdir -p /var/snap/docker/current/config
sudo nano /var/snap/docker/current/config/daemon.json
```

내용:

```json
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "100m",
    "max-file": "3"
  }
}
```

적용:

```bash
sudo systemctl restart snap.docker.dockerd
```

일반 Docker CE 기준 설정 파일은 보통 `/etc/docker/daemon.json`입니다.

```bash
sudo nano /etc/docker/daemon.json
sudo systemctl restart docker
```

주의: daemon 설정은 새로 생성되는 컨테이너에 확실하게 적용됩니다. 기존 컨테이너는 재생성해야 `HostConfig.LogConfig`가 바뀝니다.

확인:

```bash
sudo docker inspect osrm-korea --format '{{json .HostConfig.LogConfig}}'
```

정상 예:

```json
{"Type":"json-file","Config":{"max-file":"3","max-size":"100m"}}
```

## 4. OSRM 데이터 빌드

OSRM 데이터와 스크립트는 `/home/osrm`에 둡니다.

필수 파일 예:

- `/home/osrm/profiles/custom_car.lua`
- `/home/osrm/south-korea/south-korea-latest.osm.pbf`
- `/home/osrm/socal/socal-latest.osm.pbf`
- `/home/osrm/georgia/georgia-latest.osm.pbf`

스크립트 실행 권한:

```bash
chmod +x /home/osrm/install_osrm_korea.sh
chmod +x /home/osrm/install_osrm_usa.sh
chmod +x /home/osrm/run_osrm_korea.sh
chmod +x /home/osrm/run_osrm_usa.sh
chmod +x /home/osrm/run_osrm_regions.sh
chmod +x /home/osrm/update_osrm_korea.sh
chmod +x /home/osrm/update_osrm_usa.sh
chmod +x /home/osrm/nightly_update_osrm_korea.sh
chmod +x /home/osrm/nightly_update_osrm_usa.sh
```

Korea 빌드:

```bash
cd /home/osrm
./install_osrm_korea.sh
```

USA 빌드:

```bash
cd /home/osrm
./install_osrm_usa.sh
```

현재 install/run 스크립트에는 Docker 로그 제한 옵션이 포함되어 있어야 합니다.

```bash
--log-driver json-file
--log-opt "max-size=${LOG_MAX_SIZE}"
--log-opt "max-file=${LOG_MAX_FILE}"
```

기본값:

```bash
LOG_MAX_SIZE=100m
LOG_MAX_FILE=3
```

필요 시 실행 시점에 조정할 수 있습니다.

```bash
OSRM_DOCKER_LOG_MAX_SIZE=50m OSRM_DOCKER_LOG_MAX_FILE=5 ./run_osrm_korea.sh
```

## 5. OSRM 컨테이너 실행

Korea:

```bash
cd /home/osrm
./run_osrm_korea.sh
```

USA:

```bash
cd /home/osrm
./run_osrm_usa.sh
```

`run_osrm_regions.sh`가 있는 환경에서는 지역별 실행 래퍼로 사용할 수 있습니다.

```bash
cd /home/osrm
./run_osrm_regions.sh
```

수동으로 재생성할 경우 Korea 예:

```bash
sudo docker stop osrm-korea || true
sudo docker rm osrm-korea || true

sudo docker run -d \
  --name osrm-korea \
  --restart unless-stopped \
  --log-driver json-file \
  --log-opt max-size=100m \
  --log-opt max-file=3 \
  -p 5000:5000 \
  -v /home/osrm:/data \
  ghcr.io/project-osrm/osrm-backend \
  osrm-routed --algorithm mld /data/south-korea/south-korea-latest.osrm
```

동작 확인:

```bash
curl "http://localhost:5000/route/v1/driving/127.0276,37.4979;127.0300,37.5000?overview=false"
curl "http://localhost:5001/route/v1/driving/-118.2437,34.0522;-118.2500,34.0600?overview=false"
curl "http://localhost:5002/route/v1/driving/-84.3880,33.7490;-84.3900,33.7500?overview=false"
```

정상 응답에는 `"code":"Ok"`가 포함됩니다.

## 6. AI Routing Python 환경

프로젝트 위치:

```bash
cd /home/csda/AI_Routing/production
```

venv 생성:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
```

프로젝트 requirements 파일이 있으면 설치합니다.

```bash
pip install -r requirements.txt
```

requirements 파일이 없으면 프로젝트에서 사용하는 패키지를 기존 서버 기준으로 맞춰 설치해야 합니다.

주요 실행 파일:

- `python sr_common_vrp_api_server.py --config config_common_vrp.json --port 8065`
- `sr_common_vrp_client_server.py`
- `sr_vrp_api_server.py`

사전 확인:

```bash
ls -l /home/csda/AI_Routing/production/.venv/bin/python
ls -l /home/csda/AI_Routing/production/sr_common_vrp_api_server.py
ls -l /home/csda/AI_Routing/production/sr_common_vrp_client_server.py
ls -l /home/csda/AI_Routing/production/sr_vrp_api_server.py
```

## 7. Common VRP DB 초기화

Common VRP API는 시작 시 `init_schema()`를 호출하므로 테이블은 자동 생성됩니다.

개발 DB의 master/context seed가 필요하면 API HTTP endpoint가 아니라 검증된
bootstrap CLI를 사용합니다. 이 명령은 `development`, `vrp_db_dev`, `8066`의
일치 여부를 먼저 검사합니다.

```bash
cd /home/csda/AI_Routing/development
./bootstrap_common_vrp_dev.sh
```

동일한 명령을 직접 실행하려면:

```bash
cd /home/csda/AI_Routing/development
.venv/bin/python sr_common_vrp_api_server.py \
  --config config_common_vrp.dev.json --host 0.0.0.0 --port 8066 \
  --expected-environment development --bootstrap-only
```

Production seed는 일반 startup이나 HTTP 요청으로 수행하지 않습니다. 승인된
변경 계획과 DB backup 후 production config 및 port를 명시하고
`--confirm-production-bootstrap`까지 제공해야 합니다. 자세한 절차와 rollback은
`services/README.md`를 따릅니다.

## 8. systemd 자동 시작 설정

이 프로젝트의 권장 자동 시작 방식은 `systemd`입니다.

관리 대상:

- `common-vrp.service`: Common VRP API
- `common-vrp-client.service`: Streamlit Common VRP Client
- `smart-routing.service`: Smart Routing API
- `osrm-korea.service`: OSRM Korea 컨테이너 시작
- `osrm-usa.service`: OSRM USA 컨테이너 시작

권장 원칙:

- 부팅 시 시작은 `systemd`로 관리합니다.
- Python API 프로세스는 `Restart=always`를 사용합니다.
- OSRM 컨테이너는 Docker `--restart unless-stopped`를 사용합니다.
- 예전 watchdog `crontab @reboot` 방식과 `systemd` 방식을 동시에 사용하지 않습니다.

### 예전 watchdog 방식 비활성화

예전에 `crontab @reboot` watchdog을 사용했다면 먼저 제거합니다.

```bash
crontab -e
```

다음과 같은 줄을 삭제합니다.

```cron
@reboot /usr/bin/nohup /home/csda/AI_Routing/production/watch_common_vrp_api.sh >/home/csda/AI_Routing/production/log/watch_common.out 2>&1 &
@reboot /usr/bin/nohup /home/csda/AI_Routing/production/watch_smart_routing_api.sh >/home/csda/AI_Routing/production/log/watch_smart.out 2>&1 &
@reboot /usr/bin/nohup /home/osrm/watch_osrm_korea.sh >/home/osrm/log/watch_osrm_korea.out 2>&1 &
@reboot /usr/bin/nohup /home/osrm/watch_osrm_usa.sh >/home/osrm/log/watch_osrm_usa.out 2>&1 &
```

제거 확인:

```bash
crontab -l
```

이미 떠 있는 watchdog 프로세스가 있으면 중지합니다.

```bash
pkill -f watch_common_vrp_api.sh || true
pkill -f watch_smart_routing_api.sh || true
pkill -f watch_osrm_korea.sh || true
pkill -f watch_osrm_usa.sh || true
```

### 서비스 파일 설치

서비스 파일은 repository의 `/home/csda/AI_Routing/production/systemd` 아래에 있습니다.

```bash
sudo cp /home/csda/AI_Routing/production/systemd/common-vrp.service /etc/systemd/system/
sudo cp /home/csda/AI_Routing/production/systemd/common-vrp-client.service /etc/systemd/system/
sudo cp /home/csda/AI_Routing/production/systemd/smart-routing.service /etc/systemd/system/
```

OSRM unit과 실행 스크립트는 애플리케이션 배포본에 포함되지 않는다. `/home/osrm`
운영 패키지에서 별도로 설치한다.

적용:

```bash
sudo systemctl daemon-reload
```

서비스 파일을 수정해 다시 복사한 뒤에도 `daemon-reload`를 다시 실행해야 합니다.

자동 시작 enable:

```bash
sudo systemctl enable postgresql@14-main
sudo systemctl enable common-vrp.service
sudo systemctl enable common-vrp-client.service
sudo systemctl enable smart-routing.service
sudo systemctl enable osrm-korea.service
sudo systemctl enable osrm-usa.service
```

시작:

```bash
sudo systemctl start postgresql@14-main
sudo systemctl start osrm-korea.service
sudo systemctl start osrm-usa.service
sudo systemctl start common-vrp.service
sudo systemctl start common-vrp-client.service
sudo systemctl start smart-routing.service
```

### 서비스 상태 의미

Python API/Client:

- `common-vrp.service`
- `common-vrp-client.service`
- `smart-routing.service`
- 정상 상태는 `active (running)`입니다.

OSRM services:

- `osrm-korea.service`
- `osrm-usa.service`
- 정상 상태는 `active (exited)`일 수 있습니다.

OSRM 서비스는 startup script를 한 번 실행해 Docker 컨테이너를 만든 뒤 종료되는 `oneshot` 방식입니다. 컨테이너 자체는 Docker `--restart unless-stopped`가 계속 살립니다.

## 9. 상태 확인

PostgreSQL:

```bash
pg_lsclusters
sudo systemctl status postgresql@14-main
```

systemd:

```bash
sudo systemctl status common-vrp.service
sudo systemctl status common-vrp-client.service
sudo systemctl status smart-routing.service
sudo systemctl status osrm-korea.service
sudo systemctl status osrm-usa.service
```

Common VRP API:

```bash
curl -fsS http://127.0.0.1:8065/api/v1/common/contexts
```

정상 응답 예:

```json
{"subsidiaries": ["LGEAI"], "cities": ["Atlanta, GA"]}
```

Smart Routing API process:

```bash
pgrep -af "sr_vrp_api_server.py"
```

예상:

```text
... sr_vrp_api_server.py --host 0.0.0.0 --port 8055
```

Docker / OSRM:

```bash
sudo docker ps --format 'table {{.Names}}\t{{.Ports}}\t{{.Status}}'
sudo docker inspect osrm-korea --format '{{json .HostConfig.LogConfig}}'
sudo docker inspect osrm-socal --format '{{json .HostConfig.LogConfig}}'
sudo docker inspect osrm-georgia --format '{{json .HostConfig.LogConfig}}'
```

OSRM health check:

```bash
curl -fsS http://127.0.0.1:5000/nearest/v1/driving/126.9780,37.5665 >/dev/null && echo korea_ok
curl -fsS http://127.0.0.1:5001/nearest/v1/driving/-118.2437,34.0522 >/dev/null && echo socal_ok
curl -fsS http://127.0.0.1:5002/nearest/v1/driving/-84.3880,33.7490 >/dev/null && echo georgia_ok
```

디스크:

```bash
df -h
sudo du -xh /var /home /tmp 2>/dev/null | sort -h | tail -n 30
```

## 10. 로그 확인

systemd 최근 로그:

```bash
journalctl -u common-vrp.service -n 50 --no-pager
journalctl -u common-vrp-client.service -n 50 --no-pager
journalctl -u smart-routing.service -n 50 --no-pager
journalctl -u osrm-korea.service -n 50 --no-pager
journalctl -u osrm-usa.service -n 50 --no-pager
```

실시간 로그:

```bash
journalctl -u common-vrp.service -f
journalctl -u common-vrp-client.service -f
journalctl -u smart-routing.service -f
journalctl -u osrm-korea.service -f
journalctl -u osrm-usa.service -f
```

OSRM 부팅 시작이 실패하면 Docker가 준비되기 전에 실행됐는지 먼저 확인합니다.

```bash
journalctl -u osrm-korea.service -b --no-pager
journalctl -u osrm-usa.service -b --no-pager
docker ps
```

## 11. 재부팅 테스트

설정 완료 후 재부팅합니다.

```bash
sudo reboot
```

재접속 후 확인합니다.

```bash
pg_lsclusters
sudo systemctl status common-vrp.service --no-pager
sudo systemctl status common-vrp-client.service --no-pager
sudo systemctl status smart-routing.service --no-pager
sudo systemctl status osrm-korea.service --no-pager
sudo systemctl status osrm-usa.service --no-pager

curl -fsS http://127.0.0.1:8065/api/v1/common/contexts
pgrep -af "sr_vrp_api_server.py"
docker ps --format "table {{.Names}}\t{{.Ports}}\t{{.Status}}" | grep -E "osrm-korea|osrm-socal|osrm-georgia"
```

모든 확인이 통과하면 boot-time auto-start가 정상 동작하는 것입니다.

## 12. 수동 시작 / 재시작 / 업데이트

systemd가 활성화된 정상 운영 환경에서는 다음 명령으로 재시작한다.

```bash
sudo systemctl restart common-vrp common-vrp-client smart-routing
sudo systemctl restart common-vrp-dev common-vrp-client-dev smart-routing-dev
```

아래 shell wrapper는 systemd를 사용하지 않는 수동 검증·긴급 복구용이다.
동일 서비스를 systemd가 관리 중일 때 실행하면 자동 재시작과 포트 경쟁이
발생할 수 있으므로 먼저 해당 unit을 중지한 경우에만 사용한다.

```bash
/home/csda/AI_Routing/production/restart_common_vrp_api.sh
/home/csda/AI_Routing/production/restart_common_vrp_client_server.sh
/home/csda/AI_Routing/production/restart_smart_routing_api.sh
```

OSRM 수동 시작 스크립트:

```bash
/home/osrm/run_osrm_korea.sh
/home/osrm/run_osrm_usa.sh
/home/osrm/run_osrm_regions.sh
```

OSRM 수동 업데이트 스크립트:

```bash
/home/osrm/update_osrm_korea.sh
/home/osrm/update_osrm_usa.sh
```

의미:

- 최신 PBF 다운로드
- 현재 custom profile로 `.osrm` artifact 재빌드
- `systemd` 서비스 재시작은 자동으로 하지 않음

업데이트 후 관련 OSRM 서비스를 재시작합니다.

```bash
sudo systemctl restart osrm-korea.service
sudo systemctl restart osrm-usa.service
```

nightly update wrapper:

```bash
/home/osrm/nightly_update_osrm_korea.sh
/home/osrm/nightly_update_osrm_usa.sh
```

의미:

- 최신 PBF 다운로드
- `.osrm` artifact 재빌드
- 매칭되는 OSRM 컨테이너 즉시 재시작

systemd 재시작:

```bash
sudo systemctl restart common-vrp.service
sudo systemctl restart common-vrp-client.service
sudo systemctl restart smart-routing.service
sudo systemctl restart osrm-korea.service
sudo systemctl restart osrm-usa.service
```

## 13. OSRM USA 도시 확장

USA OSRM은 `CITY_ENTRIES` 목록 기반입니다.

관련 파일:

- `/home/osrm/install_osrm_usa.sh`
- `/home/osrm/run_osrm_usa.sh`
- `/home/osrm/watch_osrm_usa.sh`
- `/home/osrm/update_osrm_usa.sh`

install script 형식:

```bash
"socal|LA"
"georgia|Atlanta"
```

run/watch script 형식:

```bash
"socal|LA|5001|-118.2437,34.0522"
"georgia|Atlanta|5002|-84.3880,33.7490"
```

필드:

- `dir_name`
- `display_name`
- `host_port`
- `healthcheck_lonlat`

새 도시 추가 예:

```bash
"dallas|Dallas"
```

run/watch에는 포트와 health check 좌표를 포함합니다.

```bash
"dallas|Dallas|5003|-96.7970,32.7767"
```

그 다음 다음 경로와 PBF 파일을 준비합니다.

- `/home/osrm/dallas/`
- `/home/osrm/dallas/dallas-latest.osm.pbf`

## 14. 매일 자정 지도 업데이트 스케줄링

자동 지도 갱신은 시간대별 스케줄 지정이 쉬운 cron을 사용합니다. 이 cron은 부팅 자동 시작용 `systemd`와 목적이 다르므로 함께 사용할 수 있습니다.

스크립트 준비:

```bash
chmod +x /home/osrm/nightly_update_osrm_korea.sh
chmod +x /home/osrm/nightly_update_osrm_usa.sh
mkdir -p /home/osrm/log
```

cron 열기:

```bash
crontab -e
```

권장 등록:

```cron
CRON_TZ=Asia/Seoul
0 0 * * * /usr/bin/flock -n /tmp/nightly_update_osrm_korea.lock /home/osrm/nightly_update_osrm_korea.sh >>/home/osrm/log/nightly_update_korea.out 2>&1
0 20 * * * /usr/bin/flock -n /tmp/nightly_update_osrm_usa.lock /home/osrm/nightly_update_osrm_usa.sh >>/home/osrm/log/nightly_update_usa.out 2>&1
```

메모:

- Korea는 `Asia/Seoul` 자정에 실행됩니다.
- USA는 기본적으로 `America/New_York` 자정에 실행됩니다.
- USA 스크립트가 LA와 Atlanta를 함께 갱신하므로 하나의 기준 시간대를 선택해야 합니다.

등록 확인:

```bash
crontab -l
```

첫 실행 후 확인:

```bash
tail -n 100 /home/osrm/log/nightly_update_korea.out
tail -n 100 /home/osrm/log/nightly_update_usa.out
docker ps --format "table {{.Names}}\t{{.Ports}}\t{{.Status}}" | grep -E "osrm-korea|osrm-socal|osrm-georgia"
```

## 15. 장애 대응

### Common VRP API에서 PostgreSQL connection refused

증상:

```text
connection to server at "localhost" (127.0.0.1), port 5432 failed: Connection refused
```

확인:

```bash
pg_lsclusters
sudo tail -n 100 /var/log/postgresql/postgresql-14-main.log
df -h
```

`Status down`이고 로그에 `No space left on device`가 있으면 root full이 원인입니다.

처리:

```bash
sudo journalctl --vacuum-size=200M
sudo docker inspect osrm-korea --format '{{.LogPath}}'
sudo truncate -s 0 <LOG_PATH>
sudo pg_ctlcluster 14 main start
sudo systemctl restart common-vrp.service
```

### Docker 로그가 다시 커질 때

컨테이너 로그 경로:

```bash
sudo docker inspect osrm-korea --format '{{.LogPath}}'
```

현재 컨테이너 로그 제한 확인:

```bash
sudo docker inspect osrm-korea --format '{{json .HostConfig.LogConfig}}'
```

`Config`가 비어 있으면 컨테이너를 재생성해야 합니다.

### OSRM 데이터 업데이트 후 반영

`.osrm` 파일을 교체하거나 재빌드한 뒤에는 `osrm-routed`가 자동 reload하지 않습니다.

```bash
sudo docker restart osrm-korea
sudo docker restart osrm-socal
sudo docker restart osrm-georgia
```

또는 run script로 재생성합니다.

```bash
cd /home/osrm
./run_osrm_korea.sh
./run_osrm_usa.sh
```

## 16. 운영 체크리스트

재부팅 후 확인:

```bash
pg_lsclusters
sudo docker ps --format 'table {{.Names}}\t{{.Ports}}\t{{.Status}}'
sudo systemctl status common-vrp.service --no-pager
sudo systemctl status common-vrp-client.service --no-pager
sudo systemctl status smart-routing.service --no-pager
curl http://127.0.0.1:8065/api/v1/common/contexts
curl -fsS http://127.0.0.1:5000/nearest/v1/driving/126.9780,37.5665 >/dev/null && echo korea_ok
curl -fsS http://127.0.0.1:5001/nearest/v1/driving/-118.2437,34.0522 >/dev/null && echo socal_ok
curl -fsS http://127.0.0.1:5002/nearest/v1/driving/-84.3880,33.7490 >/dev/null && echo georgia_ok
df -h
```

정상 기준:

- PostgreSQL `14 main 5432 online`
- PostgreSQL data directory가 `/data/postgresql/14/main`
- root 사용량이 90% 미만
- `common-vrp.service`, `common-vrp-client.service`, `smart-routing.service`가 `active (running)`
- `osrm-korea.service`, `osrm-usa.service`는 `active (exited)`여도 정상
- `osrm-korea`, `osrm-socal`, `osrm-georgia` 컨테이너 Up
- OSRM 컨테이너 LogConfig에 `max-size`, `max-file` 존재
- Common VRP API가 `/api/v1/common/contexts`에 응답
- OSRM health check가 `korea_ok`, `socal_ok`, `georgia_ok`를 출력
