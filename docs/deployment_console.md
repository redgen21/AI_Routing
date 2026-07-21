# 배포 콘솔 사용법

`sr_deployment_console.py`는 개발/운영 배포 artifact, 서버 데이터, DB 관리 작업과
배포 후 서비스 시작을 한 화면에서 처리하는 로컬 Streamlit 도구다. 서버에서 실행하는
애플리케이션이 아니라, 운영자 PC에서 SFTP/SSH로 서버를 관리한다.

## 실행

```powershell
python -m pip install -r services/deploy/requirements.txt
streamlit run sr_deployment_console.py
```

## Runtime artifact build workflow

The **Build artifact** section builds from the source checkout that is currently
open on the operator workstation. Select the target environment in the sidebar,
enter a new version, review the Git revision and dirty-state preview, and then
build the artifact. An existing version directory or ZIP is never overwritten.

- For `development`, a clean checkout is preferred. A dirty checkout can be used
  only after enabling the explicit dirty-source approval. The resulting artifact
  is for development verification and is non-promotable.
- For `production`, use a separate clean checkout build. Production never offers
  or accepts the dirty-source bypass.
- Never copy or rename a development artifact to promote it to production. Build
  the production artifact separately from the reviewed clean checkout so its
  environment, manifest, source revision, and checksum are production-specific.
- **Build runtime artifact** creates and validates local staging and ZIP outputs
  only. It does not upload, deploy, restart a service, or contact the server.
  Review the newly listed artifact and use the separate upload action only after
  the normal deployment authorization and remote-diff checks are complete.

프로젝트의 `.streamlit/config.toml`은 `server.fileWatcherType = "none"`과
`server.runOnSave = false`를 적용한다. 따라서 Windows UNC/NAS 경로에서 임시 배포
디렉터리가 생성·삭제될 때 watchdog이 `os.path.realpath` 오류로 종료되는 문제를 피하면서
기존 실행 명령을 그대로 사용할 수 있다. 코드 변경 자동 감지는 사용하지 않으므로 콘솔
소스를 수정한 뒤에는 Streamlit 프로세스를 수동으로 다시 실행한다.

배포 콘솔의 좌우 파일 표는 최신 `width="stretch"` API를 사용하므로 Streamlit 1.55
이상이 필요하다. 위 requirements 설치 명령이 호환 버전을 보장한다.

접속 정보는 Git에서 제외되는 `config/server_ftp.local.json`에만 저장한다. 배포 정책은
`config/server_deploy.local.json`에서 관리한다. 템플릿의 기본값은 다음과 같이 모든
원격 변경을 막는다.

```json
{
  "credentials_file": "config/server_ftp.local.json",
  "remote_root": "/home/csda/AI_Routing",
  "allow_upload": false,
  "allow_service_control": false
}
```

Artifact를 선택하면 manifest 전체에 대해 원격 diff를 자동 조회한다. 왼쪽 로컬 경로는
프로젝트 루트(`북미 라우팅`) 이후의 상대경로로 줄여 표시하고, 오른쪽에는 대응하는 서버
전체 경로를 표시한다. checksum은 SHA-256 파일 내용 지문이며 화면에는 앞 12자만 표시한다.
`unchanged`는 로컬과 서버 파일의 전체 SHA-256이 같다는 뜻이다. 이 비교는 read-only이며 대상 서버와 artifact checksum 기준으로
캐시된다. `Files to upload`에는 `create`와 `update` 파일만 나타나며 `unchanged` 파일은
숨긴다. 변경 파일이 없으면 `No changed files to upload`를 표시하고 업로드 버튼을 제공하지
않는다. 실제 업로드가 필요할 때만 `config/server_deploy.local.json`의
`allow_upload`를 `true`로 설정하고, 변경 파일을 선택한 뒤 `Upload selected files`를
눌러 요청 내용을 만든다. 이어서 표시되는 `Confirm upload`를 눌러야 실제 업로드가
실행되며 `Cancel`로 취소할 수 있다. 환경, artifact, archive checksum 또는 선택 파일이
바뀌면 대기 중인 확인은 자동으로 무효화된다. 성공 후 원격 diff는 자동으로 다시
조회된다. Incremental 업로드도 같은 remote lock 안에서 미선택 manifest 파일 checksum을
먼저 검증하고, 선택 파일 반영 후 서버의 전체 manifest checksum을 다시 검증해야 성공한다.
배포 이력의 `selected_full_manifest`는 이번 요청에서 모든 파일을 선택했는지를 뜻한다.
`remote_manifest_verified`와 `complete_manifest`는 작업 후 서버 전체 파일 집합이 manifest와
일치했음을 뜻하며, `service_eligible`은 runtime 전체 원격 manifest 검증이 끝난 경우에만 참이다.
Confirm 직후 확인 버튼은 사라지고 진행 상태가 표시되며, 완료 후에는 현재 환경, artifact,
archive checksum, 대상 서버에 결속된 release ID와 파일 수만 안전한 완료 알림으로 남는다.
실패해도 기존 Confirm은 재사용되지 않으며 첫 번째 Upload 단계부터 다시 요청해야 한다.
DB 작업은 별도의 환경별
local DB 설정과 migration manifest 또는 고정된
import 명령만 사용한다.

## 배포 후 시작

코드 runtime의 실제 업로드가 성공하면 사이드바 `Menu`의 `Monitoring` 화면 아래쪽에서
운영·개발 환경별 시작 unit을 선택할 수 있다. 각 환경의 3개 허용 unit이 기본 선택되며
필요하면 사용자가 일부를 해제한다. `Start` 또는 `Restart` 버튼은 별도 확인 문구 입력 없이
한 번 클릭하면 실행되고, 실행 중에는 버튼을 제거해 중복 요청을 막고 진행 상태를 표시한다.
`start`는 정지된 서비스를 시작하고, 새 코드를 이미 실행 중인 프로세스에 반영할 때는
`restart`를 사용한다. 동작 순서는 Common VRP API, Smart Routing API, Streamlit
client이며 환경별 허용 unit은 다음과 같다.

| 환경 | 허용 unit |
| --- | --- |
| 개발 | `common-vrp-dev.service`, `smart-routing-dev.service`, `common-vrp-client-dev.service` |
| 운영 | `common-vrp.service`, `smart-routing.service`, `common-vrp-client.service` |

UI는 클릭한 action, 환경, 최신 release ID와 선택 unit으로 backend 확인 문구를 내부 생성한다.
백엔드는 현재 서버와 일치하는 전체 runtime 업로드 이력, 원격 파일 checksum,
환경별 unit 허용 목록, 내부 확인 문구, `allow_service_control: true`를 모두 확인한 뒤
다음 작업만 수행한다.

```text
sudo -n systemctl start|restart <allowlisted-unit>
```

이후 각 포트의 health endpoint와 `systemctl is-active`를 확인한다. 하나라도 실패하면
성공으로 표시하지 않는다. 콘솔은 stop, disable, 원격 파일 삭제 기능을 제공하지 않는다.

## 서버 모니터링

사이드바 `Menu`에서 `Monitoring`을 선택하면 환경 선택 없이 운영·개발·공용 OSRM을 한 번에
조회한다. 상태 표는 `observe_platform` 결과를 사용한 하나만 표시하고, 그 아래에 운영·개발
서비스 제어를 함께 배치한다. 이 화면에는 별도의 환경별 상태 표나 배포/DB 탭을 표시하지
않으며, `Deployment` 화면에도 Services/Monitor 탭을 중복 제공하지 않는다.
기본은 화면을 열거나 `Refresh now`를 누를 때 조회하며, 필요할 때만 10초 자동 새로고침을
켠다.

| 범위 | 프로세스/endpoint |
| --- | --- |
| 운영 | Common API `8065`, Smart API `8055`, client `8501` |
| 개발 | Common API `8066`, Smart API `8056`, client `8503` |
| 공용 OSRM | Korea `5000`, Los Angeles `5001`, Atlanta `5002` |

각 행은 systemd `active`, `enabled`, 실제 HTTP health 응답을 함께 표시한다. OSRM의
`oneshot` unit은 `active (exited)`도 정상이며 실제 라우팅 endpoint 응답을 별도로
확인한다. 비정상 행에는 읽을 수 있는 범위의 최근 journal만 표시하고 자격정보 형식은
마스킹한다. 모니터링 명령은 read-only이고 `sudo`, start, restart를 실행하지 않는다.

## 서버 최초 1회 준비

콘솔은 임의의 systemd unit을 설치하지 않는다. `systemd/`의 여섯 unit을 서버의
`/etc/systemd/system/`에 관리자가 먼저 설치하고 enable 해야 한다. 또한 SSH 사용자
`csda`가 위 허용 unit에 한해 비대화식 `systemctl start`, `restart`를 실행할 수 있도록
최소 범위의 sudoers 정책이 필요하다. 이 준비가 없으면 콘솔은
서비스 제어를 실패로 처리한다.

저장소의 `systemd/ai-routing-deploy-console.sudoers`가 이 최소 권한 템플릿이다.
서버 관리자가 내용을 검토한 뒤 `/etc/sudoers.d/ai-routing-deploy-console`에 mode `0440`으로
설치하고 `visudo -cf`로 검증한다. OSRM unit 권한은 이 템플릿에 포함되지 않는다.

운영 권장 순서는 다음과 같다.

1. development artifact 업로드 및 `restart`
2. health/status와 기능 검증
3. clean source에서 production artifact 생성
4. production 업로드 및 `restart`
5. health/status와 배포 이력 확인

실패 시 `History / rollback`은 해당 release가 최신이고, 모든 대상이 기존 파일을
덮어써서 checksum이 확인된 백업을 가진 경우에만 전체 복원한다. 새로 생성된 파일이
하나라도 포함된 release나 원격 파일이 이후 변경된 경우에는 혼합 버전을 막기 위해
rollback을 차단한다. 단, 진행 중인 업로드 자체가 실패한 경우에는 그 작업이 방금 만든
파일만 보상 삭제하고 이미 덮어쓴 파일은 즉시 백업으로 되돌린다.
